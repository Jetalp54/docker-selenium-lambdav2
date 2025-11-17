import os
import json
import time
import logging
import traceback
import io
import sys

import boto3
import paramiko
import pyotp

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
)

# Initialize logger first
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Monkey-patch SeleniumManager to prevent it from running
# This is a workaround for Selenium 4.x trying to use SeleniumManager even with explicit paths
try:
    from selenium.webdriver.common.selenium_manager import SeleniumManager
    
    def patched_binary_paths(self, *args, **kwargs):
        # If executable_path is provided in service, don't use SeleniumManager
        # Return empty dict to force Selenium to use provided paths
        logger.info("[LAMBDA] SeleniumManager.binary_paths called - attempting to bypass")
        return {'driver_path': '', 'browser_path': ''}
    
    # Only patch if we're in Lambda environment
    if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
        SeleniumManager.binary_paths = patched_binary_paths
        logger.info("[LAMBDA] SeleniumManager patched to prevent execution")
except Exception as e:
    logger.warning(f"[LAMBDA] Could not patch SeleniumManager: {e}")

# =====================================================================
# Helpers: Selenium driver in Lambda
# =====================================================================


def get_chrome_driver():
    """
    Create a headless Chrome driver inside the umihico/aws-lambda-selenium-python image.
    The base image already has Chrome and ChromeDriver pre-installed.
    We MUST bypass SeleniumManager completely to avoid "No space left on device" errors.
    
    Strategy:
    1. Find Chrome and ChromeDriver binaries explicitly
    2. Set environment variables to disable SeleniumManager
    3. Use Service with explicit executable_path
    4. Set binary_location in options
    5. Clean /tmp to ensure space is available
    """
    import os
    import subprocess
    import shutil
    
    # Clean /tmp to ensure we have space (Lambda /tmp is limited to 512MB)
    try:
        # Only clean selenium cache, not everything in /tmp
        cache_dir = '/tmp/.cache/selenium'
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir, ignore_errors=True)
        os.makedirs(cache_dir, exist_ok=True)
        logger.info("[LAMBDA] Cleaned /tmp/.cache/selenium directory")
    except Exception as e:
        logger.warning(f"[LAMBDA] Could not clean /tmp cache: {e}")
    
    # Set environment variables to use /tmp for Selenium cache (Lambda read-only filesystem)
    # This is critical - Lambda filesystem is read-only except /tmp
    os.environ['HOME'] = '/tmp'
    os.environ['XDG_CACHE_HOME'] = '/tmp/.cache'
    os.environ['SELENIUM_MANAGER_CACHE'] = '/tmp/.cache/selenium'
    
    # Completely disable SeleniumManager - use multiple methods
    os.environ['SE_SELENIUM_MANAGER'] = 'false'
    os.environ['SELENIUM_MANAGER'] = 'false'
    os.environ['SELENIUM_DISABLE_DRIVER_MANAGER'] = '1'
    
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1280,800")
    chrome_options.add_argument("--lang=en-US")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--single-process")  # Important for Lambda
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # DEBUG: List what's actually in /opt to see what the base image contains
    logger.info("[LAMBDA] Checking /opt directory contents...")
    try:
        if os.path.exists('/opt'):
            opt_contents = os.listdir('/opt')
            logger.info(f"[LAMBDA] Contents of /opt: {opt_contents}")
            # Check subdirectories
            for item in opt_contents:
                item_path = os.path.join('/opt', item)
                if os.path.isdir(item_path):
                    try:
                        sub_contents = os.listdir(item_path)
                        logger.info(f"[LAMBDA] Contents of /opt/{item}: {sub_contents[:10]}")
                    except:
                        pass
    except Exception as e:
        logger.warning(f"[LAMBDA] Could not list /opt: {e}")
    
    # Find Chrome binary - umihico base image may have it in various locations
    # Try multiple methods: direct path check, which command, find command
    chrome_binary_paths = [
        "/opt/chrome/headless-chromium",
        "/opt/chrome/chromium",
        "/opt/chrome/chrome",
        "/opt/chrome/google-chrome",
        "/opt/chrome/google-chrome-stable",
        "/opt/headless-chromium",
        "/opt/chromium",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/local/bin/google-chrome",
        "/usr/local/bin/chromium",
    ]
    
    chrome_binary = None
    
    # First, try direct path checks
    for path in chrome_binary_paths:
        if os.path.exists(path):
            chrome_binary = path
            logger.info(f"[LAMBDA] Found Chrome binary at: {chrome_binary}")
            break
    
    # If not found, try using 'which' command for various names
    if not chrome_binary:
        for cmd in ['google-chrome', 'google-chrome-stable', 'chromium', 'chromium-browser', 'chrome']:
            try:
                result = subprocess.run(['which', cmd], capture_output=True, text=True, timeout=2)
                if result.returncode == 0 and result.stdout.strip():
                    candidate = result.stdout.strip()
                    if os.path.exists(candidate):
                        chrome_binary = candidate
                        logger.info(f"[LAMBDA] Found Chrome binary via which {cmd}: {chrome_binary}")
                        break
            except:
                continue
    
    # Last resort: try 'find' command in common directories
    if not chrome_binary:
        try:
            logger.info("[LAMBDA] Attempting to find Chrome using find command...")
            # Use separate find commands for better compatibility
            for pattern in ['chrome', 'chromium', 'google-chrome*']:
                try:
                    # Use shell=False with proper find syntax
                    result = subprocess.run(
                        ['find', '/usr', '/opt', '/var', '-type', 'f', '-name', pattern],
                        capture_output=True, text=True, timeout=5, stderr=subprocess.DEVNULL
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        for line in result.stdout.strip().split('\n'):
                            line = line.strip()
                            if line and os.path.exists(line) and os.access(line, os.X_OK):
                                chrome_binary = line
                                logger.info(f"[LAMBDA] Found Chrome binary via find: {chrome_binary}")
                                break
                    if chrome_binary:
                        break
                except:
                    continue
        except Exception as e:
            logger.warning(f"[LAMBDA] Find command failed: {e}")
    
    if chrome_binary:
        chrome_options.binary_location = chrome_binary
    else:
        logger.warning("[LAMBDA] Chrome binary not found in standard locations")
    
    # Find ChromeDriver - try multiple methods
    chromedriver_paths = [
        "/usr/bin/chromedriver",
        "/usr/local/bin/chromedriver",
        "/opt/chromedriver/chromedriver",
        "/opt/chromedriver",
        "/var/task/chromedriver",  # Sometimes in Lambda task root
    ]
    
    chromedriver_path = None
    
    # First, try direct path checks
    for path in chromedriver_paths:
        if os.path.exists(path):
            chromedriver_path = path
            logger.info(f"[LAMBDA] Found ChromeDriver at: {chromedriver_path}")
            break
    
    # If not found, try using 'which' command
    if not chromedriver_path:
        for cmd in ['chromedriver', 'chromedriver-linux64', 'chromedriver-linux']:
            try:
                result = subprocess.run(['which', cmd], capture_output=True, text=True, timeout=2)
                if result.returncode == 0 and result.stdout.strip():
                    candidate = result.stdout.strip()
                    if os.path.exists(candidate):
                        chromedriver_path = candidate
                        logger.info(f"[LAMBDA] Found ChromeDriver via which {cmd}: {chromedriver_path}")
                        break
            except:
                continue
    
    # Last resort: try 'find' command
    if not chromedriver_path:
        try:
            logger.info("[LAMBDA] Attempting to find ChromeDriver using find command...")
            result = subprocess.run(
                ['find', '/usr', '/opt', '/var', '-type', 'f', '-name', 'chromedriver*'],
                capture_output=True, text=True, timeout=5, stderr=subprocess.DEVNULL
            )
            if result.returncode == 0 and result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    line = line.strip()
                    if line and os.path.exists(line) and os.access(line, os.X_OK):
                        chromedriver_path = line
                        logger.info(f"[LAMBDA] Found ChromeDriver via find: {chromedriver_path}")
                        break
        except Exception as e:
            logger.warning(f"[LAMBDA] Find command for ChromeDriver failed: {e}")

    if not chromedriver_path:
        logger.error("[LAMBDA] ChromeDriver not found! Attempting to list common directories for debugging...")
        # Debug: list what's in common directories
        debug_dirs = ['/usr/bin', '/usr/local/bin', '/opt', '/opt/chrome', '/opt/chromedriver', '/var/task', '/var/lang']
        for debug_dir in debug_dirs:
            try:
                if os.path.exists(debug_dir):
                    files = os.listdir(debug_dir)
                    logger.info(f"[LAMBDA] Contents of {debug_dir}: {files[:20]}")  # First 20 items
                    # Also check for any chrome/chromedriver related files
                    chrome_files = [f for f in files if 'chrome' in f.lower() or 'chromium' in f.lower()]
                    if chrome_files:
                        logger.info(f"[LAMBDA] Chrome-related files in {debug_dir}: {chrome_files}")
            except Exception as e:
                logger.warning(f"[LAMBDA] Could not list {debug_dir}: {e}")
        
        # Try one more thing: check if chromedriver is in PATH but not found by which
        try:
            logger.info("[LAMBDA] Checking PATH environment variable...")
            path_dirs = os.environ.get('PATH', '').split(':')
            for path_dir in path_dirs:
                if path_dir and os.path.exists(path_dir):
                    try:
                        files = os.listdir(path_dir)
                        if 'chromedriver' in files:
                            candidate = os.path.join(path_dir, 'chromedriver')
                            if os.access(candidate, os.X_OK):
                                chromedriver_path = candidate
                                logger.info(f"[LAMBDA] Found ChromeDriver in PATH directory: {chromedriver_path}")
                                break
                    except:
                        continue
        except:
            pass
        
        if not chromedriver_path:
            raise Exception("ChromeDriver not found in base image. Check Docker image build.")

    # CRITICAL: Chrome binary MUST be found, otherwise SeleniumManager will try to find it
    if not chrome_binary:
        # Final debug: list all executable files in /opt
        logger.error("[LAMBDA] Chrome binary not found! Listing all files in /opt for debugging...")
        try:
            for root, dirs, files in os.walk('/opt'):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.access(file_path, os.X_OK):
                        logger.info(f"[LAMBDA] Executable found: {file_path}")
                        if 'chrome' in file.lower() or 'chromium' in file.lower():
                            logger.info(f"[LAMBDA] CHROME-RELATED EXECUTABLE: {file_path}")
        except Exception as e:
            logger.warning(f"[LAMBDA] Could not walk /opt: {e}")
        
        raise Exception("Chrome binary not found! Cannot proceed without Chrome binary path. Checked paths: " + str(chrome_binary_paths))
    
    try:
        # Create Service with explicit ChromeDriver path
        # This should prevent SeleniumManager from trying to download/find the driver
        service = Service(executable_path=chromedriver_path)
        
        # Set browser executable path in options - CRITICAL to prevent SeleniumManager
        chrome_options.binary_location = chrome_binary
        
        # Set environment variables one more time right before driver creation
        os.environ['SE_SELENIUM_MANAGER'] = 'false'
        os.environ['SELENIUM_MANAGER'] = 'false'
        os.environ['SELENIUM_DISABLE_DRIVER_MANAGER'] = '1'
        
        logger.info(f"[LAMBDA] Initializing Chrome driver with ChromeDriver: {chromedriver_path}, Chrome: {chrome_binary}")
        logger.info(f"[LAMBDA] Environment: SE_SELENIUM_MANAGER={os.environ.get('SE_SELENIUM_MANAGER')}")
        
        # Create driver with explicit paths - this should bypass SeleniumManager
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Inject anti-detection script
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            '''
        })
        
        logger.info("[LAMBDA] Chrome driver initialized successfully")
        return driver
    except Exception as e:
        logger.error(f"[LAMBDA] Failed to initialize Chrome driver: {e}")
        logger.error(traceback.format_exc())
        
        # Last resort: try with just chromedriver path and minimal options
        try:
            logger.info("[LAMBDA] Retrying with minimal options and explicit paths...")
            minimal_options = Options()
            minimal_options.add_argument("--headless=new")
            minimal_options.add_argument("--no-sandbox")
            minimal_options.add_argument("--disable-dev-shm-usage")
            
            if chrome_binary:
                minimal_options.binary_location = chrome_binary
            
            from selenium.webdriver.chrome.service import Service
            service = Service(executable_path=chromedriver_path)
            driver = webdriver.Chrome(service=service, options=minimal_options)
            logger.info("[LAMBDA] Chrome driver initialized with minimal options")
            return driver
        except Exception as e2:
            logger.error(f"[LAMBDA] Final retry also failed: {e2}")
            logger.error(traceback.format_exc())
            raise Exception(f"Chrome driver initialization failed: {e2}. Chrome: {chrome_binary}, ChromeDriver: {chromedriver_path}")


def wait_for_xpath(driver, xpath, timeout=20):
    """Wait for element by XPath to be present."""
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, xpath))
    )


def wait_for_clickable_xpath(driver, xpath, timeout=20):
    """Wait for element by XPath to be clickable."""
    return WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, xpath))
    )


def click_xpath(driver, xpath, timeout=20, use_js=True):
    """Click element by XPath, with JavaScript fallback."""
    try:
        el = wait_for_clickable_xpath(driver, xpath, timeout)
        if use_js:
            driver.execute_script("arguments[0].scrollIntoView(true);", el)
            driver.execute_script("arguments[0].click();", el)
        else:
            el.click()
        return el
    except Exception as e:
        logger.warning(f"Failed to click {xpath}: {e}")
        raise


def element_exists(driver, xpath, timeout=5):
    """Check if element exists without raising exception."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        return True
    except TimeoutException:
        return False


def find_element_with_fallback(driver, xpath_list, timeout=10, description="element"):
    """Try multiple XPath variations to find an element."""
    per_xpath_timeout = max(1, timeout // len(xpath_list)) if len(xpath_list) > 1 else timeout
    
    for i, xpath in enumerate(xpath_list):
        try:
            element = WebDriverWait(driver, per_xpath_timeout).until(
                EC.presence_of_element_located((By.XPATH, xpath))
            )
            logger.info(f"{description} found using XPath variation {i+1}")
            return element
        except TimeoutException:
            continue
    
    logger.error(f"Failed to locate {description} with any XPath variation")
    return None


# =====================================================================
# Helpers: SFTP for secret key (server storage)
# =====================================================================


def get_sftp_params():
    """
    SFTP parameters from environment variables:
      SECRET_SFTP_HOST
      SECRET_SFTP_PORT         (optional, default 22)
      SECRET_SFTP_USER
      SECRET_SFTP_PASSWORD     (OR SECRET_SFTP_KEY for private key content)
      SECRET_SFTP_REMOTE_DIR   (directory to store secrets)
    """
    host = os.environ.get("SECRET_SFTP_HOST")
    user = os.environ.get("SECRET_SFTP_USER")
    remote_dir = os.environ.get("SECRET_SFTP_REMOTE_DIR", "/tmp/gw_secrets")
    port = int(os.environ.get("SECRET_SFTP_PORT", "22"))
    password = os.environ.get("SECRET_SFTP_PASSWORD")
    key_content = os.environ.get("SECRET_SFTP_KEY")

    if not host or not user:
        return None

    return {
        "host": host,
        "user": user,
        "port": port,
        "password": password,
        "key_content": key_content,
        "remote_dir": remote_dir,
    }


def sftp_connect_from_env():
    """Establish SFTP connection using environment variables."""
    params = get_sftp_params()
    if not params:
        logger.warning("SFTP parameters not fully configured (SECRET_SFTP_HOST / SECRET_SFTP_USER).")
        return None, None

    host = params["host"]
    user = params["user"]
    port = params["port"]
    password = params["password"]
    key_content = params["key_content"]

    try:
        transport = paramiko.Transport((host, port))
        if key_content:
            # Use key for authentication
            key_file = io.StringIO(key_content)
            pkey = paramiko.RSAKey.from_private_key(key_file)
            transport.connect(username=user, pkey=pkey)
        else:
            # Use password
            transport.connect(username=user, password=password)

        sftp = paramiko.SFTPClient.from_transport(transport)
        logger.info(f"[SFTP] Connected to {host}:{port} as {user}")
        return sftp, params["remote_dir"]
    except Exception as e:
        logger.error(f"[SFTP] Failed to connect: {e}")
        return None, None


def ensure_remote_dir(sftp, remote_dir):
    """Ensure the remote directory exists (create it if necessary)."""
    parts = remote_dir.strip("/").split("/")
    path = ""
    for part in parts:
        path = path + "/" + part if path else "/" + part
        try:
            sftp.listdir(path)
        except IOError:
            sftp.mkdir(path)
            logger.info(f"[SFTP] Created directory: {path}")


def save_secret_key_to_server(email, secret_key):
    """
    Save the secret key for the account on the remote server via SFTP.
    Creates one file per email: <alias>_totp_secret.txt
    """
    if not secret_key:
        logger.warning("[SFTP] No secret key to save.")
        return False

    sftp, remote_dir = sftp_connect_from_env()
    if not sftp:
        logger.warning("[SFTP] Skipping server secret save due to missing SFTP connection.")
        return False

    try:
        ensure_remote_dir(sftp, remote_dir)
        alias = email.split("@")[0]
        remote_path = f"{remote_dir}/{alias}_totp_secret.txt"
        
        with sftp.open(remote_path, "w") as f:
            f.write(secret_key.strip() + "\n")
        
        logger.info(f"[SFTP] Secret key saved to server at {remote_path} for {email}")
        sftp.close()
        return True
    except Exception as e:
        logger.error(f"[SFTP] Failed to save secret key to server for {email}: {e}")
        try:
            sftp.close()
        except:
            pass
        return False


# =====================================================================
# Helpers: S3 global app_passwords.txt
# =====================================================================


def append_app_password_to_s3(email, app_password):
    """
    Maintain *one* text file on S3 that holds all app passwords.
    Format: email:password\n
    New entries overwrite existing entries for that email.
    """
    bucket = os.environ.get("APP_PASSWORDS_S3_BUCKET")
    key = os.environ.get("APP_PASSWORDS_S3_KEY", "app_passwords.txt")

    if not bucket:
        logger.warning("[S3] APP_PASSWORDS_S3_BUCKET is not set. Skipping S3 app password storage.")
        return False, None, None

    s3 = boto3.client("s3")

    existing_body = ""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        existing_body = obj["Body"].read().decode("utf-8")
        logger.info(f"[S3] Read existing app_passwords.txt from s3://{bucket}/{key}")
    except s3.exceptions.NoSuchKey:
        existing_body = ""
        logger.info(f"[S3] Creating new app_passwords.txt at s3://{bucket}/{key}")
    except Exception as e:
        logger.warning(f"[S3] Could not read existing S3 app_passwords.txt: {e}")

    # Parse existing lines
    entries = {}
    if existing_body:
        for line in existing_body.splitlines():
            if ":" in line:
                parts = line.split(":", 1)
                if len(parts) == 2:
                    e, p = parts[0].strip(), parts[1].strip()
                    if e and p:
                        entries[e] = p

    # Update this email (remove dashes from app password for consistency)
    clean_password = app_password.replace("-", "").replace(" ", "").strip()
    entries[email] = clean_password

    new_body = "".join(f"{e}:{p}\n" for e, p in sorted(entries.items()))

    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=new_body.encode("utf-8"),
            ContentType="text/plain",
        )
        logger.info(f"[S3] App password updated in global S3 file for {email}: s3://{bucket}/{key}")
        return True, bucket, key
    except Exception as e:
        logger.error(f"[S3] Failed to write app_passwords.txt to S3: {e}")
        return False, bucket, key


# =====================================================================
# Step 1: Login + optional existing 2FA handling
# =====================================================================


def login_google(driver, email, password, known_totp_secret=None):
    """
    Login to Google. If a 2FA code is requested and we know a TOTP secret,
    we will try to solve it; otherwise we fail with an explicit error.
    """
    logger.info(f"[STEP] Login started for {email}")
    driver.get("https://accounts.google.com/signin/v2/identifier?hl=en&flowName=GlifWebSignIn")
    time.sleep(2)

    try:
        # Enter email
        email_input = wait_for_xpath(driver, "//input[@id='identifierId']", timeout=20)
        email_input.clear()
        email_input.send_keys(email)
        logger.info("[STEP] Email entered")
        
        # Click Next button
        email_next_xpaths = [
            "//*[@id='identifierNext']",
            "//button[@id='identifierNext']",
            "//span[contains(text(), 'Next')]/ancestor::button",
        ]
        email_next = find_element_with_fallback(driver, email_next_xpaths, timeout=10, description="email next button")
        if email_next:
            click_xpath(driver, "//*[@id='identifierNext']", timeout=5)
        else:
            # Try Enter key
            email_input.send_keys(Keys.RETURN)
        logger.info("[STEP] Email submitted")

        # Wait for password field
        time.sleep(2)

        # Enter password
        password_input_xpaths = [
            "//input[@name='Passwd']",
            "//input[@type='password']",
            "//input[@aria-label*='password' or @aria-label*='Password']",
        ]
        password_input = find_element_with_fallback(driver, password_input_xpaths, timeout=20, description="password input")
        if not password_input:
            return False, "LOGIN_PASSWORD_FIELD_NOT_FOUND", "Password field not found after email submission"
        
        password_input.clear()
        password_input.send_keys(password)
        logger.info("[STEP] Password entered")
        
        # Click Next button
        pw_next_xpaths = [
            "//*[@id='passwordNext']",
            "//button[@id='passwordNext']",
            "//span[contains(text(), 'Next')]/ancestor::button",
        ]
        pw_next = find_element_with_fallback(driver, pw_next_xpaths, timeout=10, description="password next button")
        if pw_next:
            click_xpath(driver, "//*[@id='passwordNext']", timeout=5)
        else:
            password_input.send_keys(Keys.RETURN)
        logger.info("[STEP] Password submitted")

        # Wait for potential 2FA challenge or account home
        time.sleep(3)

        current_url = driver.current_url
        logger.info(f"[STEP] URL after login attempt: {current_url}")

        # Check for account verification/ID verification required
        if "speedbump/idvreenable" in current_url or "idvreenable" in current_url:
            logger.error("[STEP] ID verification required - manual intervention needed")
            return False, "ID_VERIFICATION_REQUIRED", "Manual ID verification required"

        # If we're on MyAccount or Gmail, we are probably logged in
        if "myaccount.google.com" in current_url or "mail.google.com" in current_url or "accounts.google.com/b/0" in current_url:
            logger.info("[STEP] Login success without visible 2FA step")
            return True, None, None

        # Check for 2FA challenge (TOTP)
        if "challenge" in current_url or "signin/challenge" in current_url or element_exists(driver, "//input[@type='tel' or @autocomplete='one-time-code']", timeout=5):
            logger.info("[STEP] 2FA challenge detected")
            if not known_totp_secret:
                logger.error("[STEP] 2FA is required but no TOTP secret is available")
                return False, "2FA_REQUIRED", "2FA required but secret is unknown"

            # Generate TOTP code
            try:
                clean_secret = known_totp_secret.replace(" ", "").upper()
                totp = pyotp.TOTP(clean_secret)
                otp_code = totp.now()
                logger.info(f"[STEP] Generated TOTP code for challenge: {otp_code}")
            except Exception as e:
                logger.error(f"[STEP] Failed to generate TOTP code: {e}")
                return False, "TOTP_GENERATION_FAILED", str(e)

            # Try to fill OTP input
            try:
                otp_input_xpaths = [
                    "//input[@type='tel']",
                    "//input[@autocomplete='one-time-code']",
                    "//input[@type='text' and contains(@aria-label, 'code')]",
                ]
                otp_input = find_element_with_fallback(driver, otp_input_xpaths, timeout=20, description="OTP input")
                if not otp_input:
                    return False, "OTP_INPUT_NOT_FOUND", "OTP input field not found"
                
                # Use JavaScript to set value (more reliable in headless)
                driver.execute_script("arguments[0].value = '';", otp_input)
                driver.execute_script("arguments[0].value = arguments[1];", otp_input, otp_code)
                logger.info(f"[STEP] OTP code entered: {otp_code}")
                
                # Submit OTP
                submit_btn_xpaths = [
                    "//button[contains(@type,'submit')]",
                    "//button[@role='button' and contains(., 'Next')]",
                    "//span[contains(text(), 'Next')]/ancestor::button",
                ]
                if element_exists(driver, submit_btn_xpaths[0], timeout=5):
                    click_xpath(driver, submit_btn_xpaths[0], timeout=5)
                else:
                    otp_input.send_keys(Keys.RETURN)
                
                time.sleep(3)
            except Exception as e:
                logger.error(f"[STEP] Failed to submit 2FA code: {e}")
                return False, "2FA_SUBMIT_FAILED", str(e)

            # Check if we reached account home
            time.sleep(2)
            current_url = driver.current_url
            if "myaccount.google.com" in current_url or "mail.google.com" in current_url or "accounts.google.com/b/0" in current_url:
                logger.info("[STEP] Login success after 2FA")
                return True, None, None
            else:
                logger.error(f"[STEP] 2FA submitted but not redirected to account. URL={current_url}")
                return False, "2FA_UNEXPECTED_FLOW", f"After 2FA code, got URL={current_url}"

        # No explicit 2FA page detected, but also not clearly logged in
        logger.error(f"[STEP] Unknown login state, URL={current_url}. Manual check needed.")
        return False, "LOGIN_UNKNOWN_STATE", f"URL={current_url}"

    except TimeoutException as e:
        logger.error(f"[STEP] Timeout during login: {e}")
        return False, "LOGIN_TIMEOUT", str(e)
    except Exception as e:
        logger.error(f"[STEP] Unexpected error during login: {e}")
        logger.error(traceback.format_exc())
        return False, "LOGIN_EXCEPTION", str(e)


# =====================================================================
# Step 2: Navigate to security / authenticator / 2-step pages
# =====================================================================


def navigate_to_security(driver):
    """Navigate to Google Account Security page."""
    try:
        logger.info("[STEP] Navigating to Security page")
        driver.get("https://myaccount.google.com/security?hl=en")
        time.sleep(3)
        
        # Verify we're on the security page
        if "security" not in driver.current_url.lower():
            logger.warning(f"[STEP] May not be on security page. URL: {driver.current_url}")
        
        logger.info("[STEP] Security page loaded")
        return True, None, None
    except Exception as e:
        logger.error(f"[STEP] Failed to navigate to security page: {e}")
        return False, "NAV_SECURITY_FAILED", str(e)


# =====================================================================
# Step 3: Setup Authenticator app + capture secret
# =====================================================================


def is_authenticator_set_up(driver):
    """Check if Authenticator is already set up."""
    try:
        current_url = driver.current_url
        if "two-step-verification/authenticator" not in current_url:
            driver.get("https://myaccount.google.com/two-step-verification/authenticator?hl=en")
            time.sleep(3)
        
        # Check for "Set up" button - if it exists, authenticator is NOT set up
        setup_button_xpaths = [
            "//button[contains(., 'Set up')]",
            "//span[contains(text(), 'Set up')]/ancestor::button",
            "//button[contains(@aria-label, 'Set up')]",
        ]
        
        if find_element_with_fallback(driver, setup_button_xpaths, timeout=5, description="setup button"):
            logger.info("[STEP] Authenticator is NOT set up - setup required")
            return False
        
        # Check for indicators that it's already set up
        already_setup_indicators = [
            "//*[contains(text(), 'Authenticator app') and (contains(text(), 'On') or contains(text(), 'Active'))]",
            "//button[contains(., 'Change') or contains(., 'Remove')]",
        ]
        
        for indicator in already_setup_indicators:
            if element_exists(driver, indicator, timeout=3):
                logger.info("[STEP] Authenticator is already set up")
                return True
        
        # If we can't determine, assume it needs setup
        logger.info("[STEP] Could not determine authenticator status, attempting setup")
        return False
    except Exception as e:
        logger.warning(f"[STEP] Error checking authenticator status: {e}, assuming setup needed")
        return False


def setup_authenticator_app(driver, email):
    """
    Go to the Authenticator app setup page, start the flow,
    extract the secret key shown, confirm using TOTP,
    and return the secret.
    """
    try:
        logger.info("[STEP] Setting up Authenticator app")
        
        # Navigate to authenticator page
        driver.get("https://myaccount.google.com/two-step-verification/authenticator?hl=en")
        time.sleep(3)
        
        # Check if already set up
        if is_authenticator_set_up(driver):
            logger.info("[STEP] Authenticator already set up, extracting existing secret")
            # Try to get existing secret (this may not always work)
            # For now, we'll proceed with setup attempt
            pass
        
        # Click "Set up" button
        setup_button_xpaths = [
            "//button[contains(., 'Set up')]",
            "//span[contains(text(), 'Set up')]/ancestor::button",
            "//button[contains(@aria-label, 'Set up')]",
            "//button//*[contains(text(),'Get started')]/ancestor::button",
        ]
        
        setup_clicked = False
        for xpath in setup_button_xpaths:
            if element_exists(driver, xpath, timeout=5):
                click_xpath(driver, xpath, timeout=5)
                setup_clicked = True
                logger.info("[STEP] Clicked setup button")
                time.sleep(3)
                break
        
        if not setup_clicked:
            logger.warning("[STEP] Could not find setup button, may already be set up")
        
        # Handle "Can't scan it?" link to show secret key
        cant_scan_xpaths = [
            "//a[contains(text(), 'Can't scan it?')]",
            "//a[contains(text(), \"Can't scan\")]",
            "//button[contains(text(), 'Can't scan')]",
        ]
        
        for xpath in cant_scan_xpaths:
            if element_exists(driver, xpath, timeout=5):
                click_xpath(driver, xpath, timeout=5)
                logger.info("[STEP] Clicked 'Can't scan it?' link")
                time.sleep(2)
                break
        
        # Extract secret key - try multiple XPath variations
        secret_key = None
        secret_xpaths = [
            # Common patterns for secret key display
            "//strong[contains(text(), '-')]",
            "//code[contains(text(), '-')]",
            "//span[contains(text(), '-') and string-length(text()) >= 16]",
            "/html/body/div[9]/div/div[2]/span/div/div/ol/li[2]/div/strong",
            "/html/body/div[10]/div/div[2]/span/div/div/ol/li[2]/div/strong",
            "/html/body/div[11]/div/div[2]/span/div/div/ol/li[2]/div/strong",
            "/html/body/div[12]/div/div[2]/span/div/div/ol/li[2]/div/strong",
            "/html/body/div[13]/div/div[2]/span/div/div/ol/li[2]/div/strong",
            "//div[contains(@class, 'secret')]//strong",
            "//ol//li[2]//strong",
        ]
        
        for xpath in secret_xpaths:
            try:
                element = wait_for_xpath(driver, xpath, timeout=5)
                text = element.text.strip()
                # Secret keys are typically base32 with spaces or dashes
                text = text.replace(" ", "").replace("-", "")
                if 16 <= len(text) <= 32:  # Base32 secret keys are typically 16-32 chars
                    secret_key = text
                    logger.info(f"[STEP] Secret key extracted: {secret_key[:8]}****")
                    break
            except TimeoutException:
                continue
        
        if not secret_key:
            logger.error("[STEP] Could not extract secret key from page")
            return False, None, "AUTH_SECRET_NOT_FOUND", "No secret key detected on page"
        
        # Save secret key to server
        save_secret_key_to_server(email, secret_key)
        
        # Now confirm with TOTP code
        try:
            clean_secret = secret_key.replace(" ", "").upper()
            totp = pyotp.TOTP(clean_secret)
            code = totp.now()
            logger.info(f"[STEP] Generated TOTP code for confirmation: {code}")
        except Exception as e:
            logger.error(f"[STEP] Failed to generate TOTP code: {e}")
            return False, None, "AUTH_TOTP_GENERATION_FAILED", str(e)
        
        # Enter TOTP code
        otp_input_xpaths = [
            "//input[@type='tel']",
            "//input[@autocomplete='one-time-code']",
            "//input[@type='text' and contains(@aria-label, 'code')]",
        ]
        
        otp_input = find_element_with_fallback(driver, otp_input_xpaths, timeout=20, description="OTP input for verification")
        if not otp_input:
            return False, None, "AUTH_OTP_INPUT_NOT_FOUND", "OTP input field not found for verification"
        
        # Use JavaScript to set value
        driver.execute_script("arguments[0].value = '';", otp_input)
        driver.execute_script("arguments[0].value = arguments[1];", otp_input, code)
        logger.info(f"[STEP] TOTP code entered for verification")
        
        # Click Verify/Next button
        verify_button_xpaths = [
            "//button[contains(., 'Next')]",
            "//button[contains(., 'Verify')]",
            "//span[contains(text(), 'Next')]/ancestor::button",
            "//span[contains(text(), 'Verify')]/ancestor::button",
        ]
        
        verified = False
        for xpath in verify_button_xpaths:
            if element_exists(driver, xpath, timeout=5):
                click_xpath(driver, xpath, timeout=5)
                verified = True
                logger.info("[STEP] Clicked verify button")
                time.sleep(3)
                break
        
        if not verified:
            # Try Enter key
            otp_input.send_keys(Keys.RETURN)
            time.sleep(3)
        
        logger.info("[STEP] Authenticator app setup flow completed")
        return True, secret_key, None, None

    except Exception as e:
        logger.error(f"[STEP] Unexpected error in authenticator setup: {e}")
        logger.error(traceback.format_exc())
        return False, None, "AUTH_EXCEPTION", str(e)


# =====================================================================
# Step 4: Ensure 2-step verification is enabled
# =====================================================================


def ensure_two_step_enabled(driver, email):
    """
    Open the 2-step verification page and enable it if needed.
    """
    try:
        logger.info("[STEP] Checking 2-Step Verification status")
        driver.get("https://myaccount.google.com/signinoptions/twosv?hl=en")
        time.sleep(3)
        
        # Check if already enabled
        on_indicators = [
            "//*[contains(text(),'2-Step Verification') and contains(text(),'On')]",
            "//*[contains(text(),'Two-step verification') and contains(text(),'On')]",
            "//button[contains(., 'Turn off')]",
        ]
        
        for indicator in on_indicators:
            if element_exists(driver, indicator, timeout=5):
                logger.info("[STEP] 2-Step Verification already enabled")
                return True, None, None
        
        # Try to enable it
        logger.info("[STEP] 2-Step is not clearly 'On', attempting to enable...")
        
        enable_button_xpaths = [
            "//button[contains(., 'Get started')]",
            "//button[contains(., 'Turn on')]",
            "//button[contains(., 'Continue')]",
            "//span[contains(text(), 'Get started')]/ancestor::button",
        ]
        
        for xpath in enable_button_xpaths:
            if element_exists(driver, xpath, timeout=5):
                click_xpath(driver, xpath, timeout=5)
                logger.info("[STEP] Clicked enable button")
                time.sleep(5)
                break
        
        # Verify it's now enabled
        time.sleep(3)
        for indicator in on_indicators:
            if element_exists(driver, indicator, timeout=10):
                logger.info("[STEP] 2-Step Verification is now enabled")
                return True, None, None
        
        logger.warning("[STEP] Could not confirm 2-Step is enabled, but proceeding")
        return True, None, None  # Proceed anyway
        
    except Exception as e:
        logger.error(f"[STEP] Exception while enabling 2-Step: {e}")
        return False, "TWO_STEP_EXCEPTION", str(e)


# =====================================================================
# Step 5: Generate App password
# =====================================================================


def generate_app_password(driver, email):
    """
    Navigate to app passwords page and generate a new app password.
    Return the password string.
    """
    try:
        logger.info("[STEP] Opening App Passwords page")
        driver.get("https://myaccount.google.com/apppasswords?hl=en")
        time.sleep(5)
        
        # Check if we need to select app type first
        app_name_xpaths = [
            "//input[@aria-label='App name']",
            "//input[contains(@placeholder, 'app') or contains(@placeholder, 'name')]",
            "//input[@type='text']",
        ]
        
        app_name_field = find_element_with_fallback(driver, app_name_xpaths, timeout=10, description="app name field")
        if app_name_field:
            app_name = f"SMTP-{int(time.time())}"
            app_name_field.clear()
            app_name_field.send_keys(app_name)
            logger.info(f"[STEP] Entered app name: {app_name}")
            time.sleep(1)
        
        # Click Generate button
        generate_button_xpaths = [
            "//button[contains(., 'Generate')]",
            "//button[contains(@aria-label, 'Generate')]",
            "//span[contains(text(), 'Generate')]/ancestor::button",
            "//button[contains(., 'Next')]",
        ]
        
        generate_clicked = False
        for xpath in generate_button_xpaths:
            if element_exists(driver, xpath, timeout=5):
                click_xpath(driver, xpath, timeout=5)
                generate_clicked = True
                logger.info("[STEP] Clicked generate button")
                time.sleep(5)
                break
        
        if not generate_clicked:
            logger.warning("[STEP] Could not find generate button, checking if password already displayed")
        
        # Extract app password - try multiple patterns
        password = None
        password_xpaths = [
            # Common patterns for app password display
            "//code[contains(text(), '-')]",
            "//span[contains(text(), '-') and string-length(text()) >= 16]",
            "//div[contains(text(), '-') and string-length(text()) >= 16]",
            "//strong[contains(text(), '-')]",
            "//*[contains(@class, 'password')]//code",
            "//*[contains(@class, 'password')]//span",
        ]
        
        for xpath in password_xpaths:
            try:
                element = wait_for_xpath(driver, xpath, timeout=10)
                text = element.text.strip()
                # App passwords are 16 chars with dashes: xxxx-xxxx-xxxx-xxxx
                text = text.replace(" ", "")
                if "-" in text and len(text.replace("-", "")) == 16:
                    password = text
                    logger.info(f"[STEP] App password extracted: {password[:8]}****")
                    break
            except TimeoutException:
                continue
        
        # If not found with dashes, try to find 16-char string and format it
        if not password:
            try:
                # Look for any element with 16+ characters
                all_elements = driver.find_elements(By.XPATH, "//*[string-length(text()) >= 16]")
                for el in all_elements:
                    text = el.text.strip().replace(" ", "").replace("-", "")
                    if len(text) == 16 and text.isalnum():
                        # Format as xxxx-xxxx-xxxx-xxxx
                        password = f"{text[:4]}-{text[4:8]}-{text[8:12]}-{text[12:16]}"
                        logger.info(f"[STEP] App password extracted and formatted: {password[:8]}****")
                        break
            except Exception as e:
                logger.warning(f"[STEP] Could not extract password from text elements: {e}")
        
        if not password:
            logger.error("[STEP] Could not extract app password from page")
            return False, None, "APP_PASSWORD_NOT_FOUND", "No app password text detected"
        
        logger.info(f"[STEP] App password obtained for {email}")
        return True, password, None, None

    except Exception as e:
        logger.error(f"[STEP] Exception generating app password: {e}")
        logger.error(traceback.format_exc())
        return False, None, "APP_PASSWORD_EXCEPTION", str(e)


# =====================================================================
# Orchestration: one account per Lambda
# =====================================================================


def process_account(driver, email, password, known_totp_secret=None):
    """
    Full flow for one account:
      1) Login (with optional known TOTP secret)
      2) Navigate to security
      3) Setup Authenticator app (capture + save secret key)
      4) Ensure 2-Step Verification is enabled
      5) Generate App password and append to S3 file
    
    Returns:
      success (bool),
      step_completed (str),
      error_type (str or None),
      error_message (str or None),
      secret_key (str or None),
      app_password (str or None),
      s3_bucket (str or None),
      s3_key (str or None),
      timings (dict)
    """
    start_time = time.time()
    timings = {}
    step = "login"
    s3_bucket = None
    s3_key = None
    secret_key = None
    app_password = None

    # 1. LOGIN
    step_start = time.time()
    ok, err_type, err_msg = login_google(driver, email, password, known_totp_secret)
    timings["login"] = time.time() - step_start
    if not ok:
        return False, step, err_type, err_msg, secret_key, app_password, s3_bucket, s3_key, timings

    # 2. NAVIGATE SECURITY
    step = "navigation"
    step_start = time.time()
    ok, err_type, err_msg = navigate_to_security(driver)
    timings["navigation"] = time.time() - step_start
    if not ok:
        return False, step, err_type, err_msg, secret_key, app_password, s3_bucket, s3_key, timings

    # 3. AUTHENTICATOR APP
    step = "authenticator_setup"
    step_start = time.time()
    ok, secret_key, err_type, err_msg = setup_authenticator_app(driver, email)
    timings["authenticator_setup"] = time.time() - step_start
    if not ok:
        return False, step, err_type, err_msg, secret_key, app_password, s3_bucket, s3_key, timings

    # 4. 2-STEP
    step = "2step_verification"
    step_start = time.time()
    ok, err_type, err_msg = ensure_two_step_enabled(driver, email)
    timings["2step_verification"] = time.time() - step_start
    if not ok:
        return False, step, err_type, err_msg, secret_key, app_password, s3_bucket, s3_key, timings

    # 5. APP PASSWORD
    step = "app_password"
    step_start = time.time()
    ok, app_password, err_type, err_msg = generate_app_password(driver, email)
    timings["app_password"] = time.time() - step_start
    if not ok:
        return False, step, err_type, err_msg, secret_key, app_password, s3_bucket, s3_key, timings

    # Save to S3 global file
    step_start = time.time()
    success_s3, s3_bucket, s3_key = append_app_password_to_s3(email, app_password)
    timings["s3_save"] = time.time() - step_start
    if not success_s3:
        return False, step, "S3_SAVE_FAILED", "App password generated but S3 save failed", secret_key, app_password, s3_bucket, s3_key, timings

    timings["total"] = time.time() - start_time
    return True, "completed", None, None, secret_key, app_password, s3_bucket, s3_key, timings


# =====================================================================
# Lambda handler: ONE ACCOUNT PER INVOCATION
# =====================================================================


def handler(event, context):
    """
    Lambda entrypoint.

    Expected event:
      {
        "email": "user@example.com",
        "password": "clear-text-password",
        "known_totp_secret": "BASE32SECRET"  // optional
      }

    You can also provide these via environment variables:
      GW_EMAIL, GW_PASSWORD, KNOWN_TOTP_SECRET

    Returns JSON with:
      status          = "ok" | "failed"
      email
      step_completed  = "login" | "navigation" | "authenticator_setup" | "2step_verification" | "app_password" | "completed"
      error_step      = step where error occurred (if failed)
      error_message   = error description (if failed)
      app_password    = generated app password (if successful)
      secret_key      = extracted TOTP secret (if successful, masked)
      app_passwords_s3_bucket
      app_passwords_s3_key
      timings         = dict of step timings
    """
    
    # Ensure logging is properly configured
    logger.setLevel(logging.INFO)
    logger.info("=" * 60)
    logger.info("[LAMBDA] Handler invoked")
    logger.info(f"[LAMBDA] Event type: {type(event)}")
    logger.info(f"[LAMBDA] Event content: {json.dumps(event) if isinstance(event, dict) else str(event)}")
    logger.info(f"[LAMBDA] Context: {context}")
    logger.info("=" * 60)

    email = event.get("email") or os.environ.get("GW_EMAIL")
    password = event.get("password") or os.environ.get("GW_PASSWORD")
    known_totp_secret = event.get("known_totp_secret") or os.environ.get("KNOWN_TOTP_SECRET")

    if not email or not password:
        msg = "email and password must be provided via event or env (GW_EMAIL / GW_PASSWORD)."
        logger.error(msg)
        return {
            "status": "failed",
            "email": email or "unknown",
            "step_completed": "init",
            "error_step": "init",
            "error_message": msg,
            "app_password": None,
            "secret_key": None,
            "app_passwords_s3_bucket": None,
            "app_passwords_s3_key": None,
            "timings": {},
        }

    driver = None
    step_completed = "init"
    error_type = None
    error_message = None
    s3_bucket = None
    s3_key = None
    secret_key = None
    app_password = None
    timings = {}

    try:
        driver = get_chrome_driver()
        logger.info(f"[LAMBDA] Chrome driver started for {email}")

        success, step_completed, error_type, error_message, secret_key, app_password, s3_bucket, s3_key, timings = (
            process_account(driver, email, password, known_totp_secret)
        )

        status = "ok" if success else "failed"
        
        # Mask secret key in response (show first 8 chars only)
        masked_secret = None
        if secret_key:
            masked_secret = secret_key[:8] + "****" if len(secret_key) > 8 else "****"

        return {
            "status": status,
            "email": email,
            "step_completed": step_completed,
            "error_step": step_completed if not success else None,
            "error_message": error_message,
            "app_password": app_password,
            "secret_key": masked_secret,  # Masked for security
            "app_passwords_s3_bucket": s3_bucket,
            "app_passwords_s3_key": s3_key,
            "timings": timings,
        }

    except Exception as e:
        logger.error(f"[LAMBDA] Unhandled exception: {e}")
        logger.error(traceback.format_exc())
        return {
            "status": "failed",
            "email": email,
            "step_completed": step_completed,
            "error_step": step_completed,
            "error_message": str(e),
            "app_password": None,
            "secret_key": None,
            "app_passwords_s3_bucket": s3_bucket,
            "app_passwords_s3_key": s3_key,
            "timings": timings,
        }
    finally:
        if driver:
            try:
                driver.quit()
                logger.info("[LAMBDA] Chrome driver closed")
            except Exception as e:
                logger.warning(f"[LAMBDA] Error closing driver: {e}")
