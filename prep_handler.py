"""
G-Workspace Prep Lambda Function
- Installs gcloud
- Authenticates via Selenium (Hybrid)
- Creates Project, Service Account, Keys
- Enables APIs
- Uploads JSON key to S3
"""

import os
import json
import time
import random
import logging
import subprocess
import traceback
import boto3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

# Modern User-Agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
]

# Common Window Sizes for rotation
WINDOW_SIZES = [
    "1920,1080",
    "1366,768",
    "1440,900",
    "1536,864",
    "1280,800",
    "1280,720"
]

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET_NAME = os.environ.get("PREP_S3_BUCKET_NAME", "edu-gw-service-accounts")

def handler(event, context):
    """
    Lambda Handler for G-Workspace Prep
    """
    logger.info("=" * 60)
    logger.info("[PREP] Handler invoked")
    logger.info(f"[PREP] Event: {event}")
    
    users = event.get("users", [])
    if not users:
        return {"status": "failed", "error": "No users provided"}

    return process_workspace_prep(users)

def get_proxy_from_env():
    """
    Get proxy configuration from environment variable (Simplified for Prep).
    Returns: dict with proxy config or None if not set
    """
    proxy_url = os.environ.get('PROXY_URL', '').strip()
    if not proxy_url:
        return None
    return {'http': proxy_url, 'https': proxy_url}

def get_chrome_driver():
    """
    Initialize Selenium Chrome driver for AWS Lambda environment.
    Uses standard Selenium with CDP-based anti-detection (Lambda-compatible).
    """
    # Force environment variables to prevent SeleniumManager from trying to write to read-only FS
    os.environ['HOME'] = '/tmp'
    os.environ['XDG_CACHE_HOME'] = '/tmp/.cache'
    os.environ['SELENIUM_MANAGER_CACHE'] = '/tmp/.cache/selenium'
    os.environ['SE_SELENIUM_MANAGER'] = 'false'
    os.environ['SELENIUM_MANAGER'] = 'false'
    os.environ['SELENIUM_DISABLE_DRIVER_MANAGER'] = '1'
    
    # Ensure /tmp directories exist
    os.makedirs('/tmp/.cache/selenium', exist_ok=True)
    
    # Locate Chrome binary and ChromeDriver
    logger.info("[LAMBDA] Checking /opt directory contents...")
    chrome_binary = None
    chromedriver_path = None
    
    # Common paths for Chrome binary
    chrome_paths = [
        '/opt/chrome/chrome',
        '/opt/chrome/headless-chromium',
        '/usr/bin/chromium',
        '/usr/bin/google-chrome',
    ]
    
    for path in chrome_paths:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            chrome_binary = path
            logger.info(f"[LAMBDA] Found Chrome binary at: {chrome_binary}")
            break
            
    if not chrome_binary:
        # Fallback to which
        try:
            result = subprocess.run(['which', 'google-chrome'], capture_output=True, text=True)
            if result.returncode == 0:
                chrome_binary = result.stdout.strip()
        except:
            pass

    if not chrome_binary:
        logger.error("[LAMBDA] Chrome binary not found!")
        raise Exception("Chrome binary not found in Lambda environment")
    
    # Common paths for ChromeDriver
    chromedriver_paths = [
        '/opt/chromedriver',
        '/usr/bin/chromedriver',
        '/usr/local/bin/chromedriver',
    ]
    
    for path in chromedriver_paths:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            chromedriver_path = path
            logger.info(f"[LAMBDA] Found ChromeDriver at: {chromedriver_path}")
            break
            
    if not chromedriver_path:
        logger.error("[LAMBDA] ChromeDriver not found!")
        raise Exception("ChromeDriver not found in Lambda environment")

    # Use Selenium Chrome options with anti-detection
    chrome_options = Options()
    
    # Get proxy configuration if enabled
    proxy_config = get_proxy_from_env()
    if proxy_config:
        chrome_options.add_argument(f"--proxy-server={proxy_config['http']}")
    
    # Randomize User-Agent
    user_agent = random.choice(USER_AGENTS)
    chrome_options.add_argument(f"--user-agent={user_agent}")
    logger.info(f"[ANTI-DETECT] Using User-Agent: {user_agent}")

    # Randomize Window Size
    window_size = random.choice(WINDOW_SIZES)
    chrome_options.add_argument(f"--window-size={window_size}")
    
    # Core stability options for Lambda
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--lang=en-US")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--disable-background-networking")
    chrome_options.add_argument("--disable-default-apps")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-sync")
    chrome_options.add_argument("--metrics-recording-only")
    chrome_options.add_argument("--mute-audio")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--safebrowsing-disable-auto-update")
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument("--disable-software-rasterizer")
    
    # Anti-detection options
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_experimental_option("prefs", {
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_settings.popups": 0,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
    })

    try:
        # Create Service with explicit ChromeDriver path
        service = Service(executable_path=chromedriver_path)
        
        # Set browser executable path in options
        chrome_options.binary_location = chrome_binary
        
        # Set environment variables to disable SeleniumManager
        os.environ['SE_SELENIUM_MANAGER'] = 'false'
        os.environ['SELENIUM_MANAGER'] = 'false'
        os.environ['SELENIUM_DISABLE_DRIVER_MANAGER'] = '1'
        
        logger.info(f"[LAMBDA] Initializing Chrome driver with ChromeDriver: {chromedriver_path}, Chrome: {chrome_binary}")
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)
        
        # Inject anti-detection script
        try:
            anti_detection_script = '''
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
            '''
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': anti_detection_script
            })
        except Exception as e:
            logger.warning(f"[LAMBDA] Could not inject anti-detection script: {e}")
        
        return driver
    except Exception as e:
        logger.error(f"[LAMBDA] Failed to initialize Chrome driver: {e}")
        logger.error(traceback.format_exc())
        raise

def install_gcloud():
    """Install gcloud CLI to /tmp if not present"""
    gcloud_path = "/tmp/google-cloud-sdk/bin/gcloud"
    if os.path.exists(gcloud_path):
        return gcloud_path

    logger.info("[GCLOUD] Installing gcloud CLI...")
    try:
        subprocess.check_call(["curl", "-o", "/tmp/gcloud.tar.gz", "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz"])
        subprocess.check_call(["tar", "-xf", "/tmp/gcloud.tar.gz", "-C", "/tmp"])
        subprocess.check_call(["/tmp/google-cloud-sdk/install.sh", "--quiet"])
        return gcloud_path
    except Exception as e:
        logger.error(f"[GCLOUD] Installation failed: {e}")
        raise

def login_google(driver, email, password):
    """Login to Google"""
    try:
        logger.info(f"[LOGIN] Navigating to Google Login for {email}...")
        driver.get("https://accounts.google.com/signin/v2/identifier?flowName=GlifWebSignIn&flowEntry=ServiceLogin")
        time.sleep(2)
        
        # Email
        email_field = driver.find_element(By.ID, "identifierId")
        email_field.send_keys(email)
        driver.find_element(By.ID, "identifierNext").click()
        time.sleep(2)
        
        # Password
        password_field = driver.find_element(By.NAME, "password") # Might need explicit wait
        password_field.send_keys(password)
        driver.find_element(By.ID, "passwordNext").click()
        time.sleep(3)
        
        # Check for success (url change or element)
        if "myaccount.google.com" in driver.current_url or "accounts.google.com" in driver.current_url:
            return True, None, None
            
        return True, None, None # Optimistic
    except Exception as e:
        return False, None, str(e)

def process_workspace_prep(users):
    """Process G-Workspace preparation for a list of users"""
    results = []
    
    # Install gcloud once
    try:
        install_gcloud()
        os.environ["PATH"] += f":/tmp/google-cloud-sdk/bin"
    except Exception as e:
        return {"status": "failed", "error": f"Gcloud install failed: {e}"}

    s3 = boto3.client("s3")

    for user in users:
        email = user.get("email")
        password = user.get("password")
        
        driver = None
        try:
            # Initialize Driver
            driver = get_chrome_driver()
            
            # Login
            success, _, err = login_google(driver, email, password)
            if not success:
                results.append({"email": email, "success": False, "error": f"Login failed: {err}"})
                driver.quit()
                continue

            # Run Gcloud Setup
            setup_success, msg = run_gcloud_setup(driver, email)
            if not setup_success:
                results.append({"email": email, "success": False, "error": msg})
                driver.quit()
                continue

            # Run Project & Service Account Creation
            project_id = f"gbot-prep-{int(time.time())}-{random.randint(1000,9999)}"
            service_account_name = "gbot-sa"
            
            logger.info(f"[GCLOUD] Starting resource creation for {email} (Project: {project_id})")
            
            creation_success, creation_result = run_gcloud_resources(project_id, service_account_name)
            
            if creation_success:
                # Upload to S3
                key_content = creation_result
                s3_key = f"service-accounts/{email}/{project_id}-key.json"
                
                try:
                    s3.put_object(
                        Bucket=S3_BUCKET_NAME,
                        Key=s3_key,
                        Body=key_content,
                        ContentType="application/json"
                    )
                    s3_url = f"s3://{S3_BUCKET_NAME}/{s3_key}"
                    
                    results.append({
                        "email": email, 
                        "success": True, 
                        "message": "G-Workspace Prep Completed Successfully",
                        "project_id": project_id,
                        "s3_url": s3_url
                    })
                except Exception as s3_err:
                    results.append({"email": email, "success": False, "error": f"S3 Upload Failed: {s3_err}"})

            else:
                results.append({"email": email, "success": False, "error": creation_result})
            
            driver.quit()
            
        except Exception as e:
            results.append({"email": email, "success": False, "error": str(e)})
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    return {"status": "completed", "results": results}

def run_gcloud_resources(project_id, service_account_name):
    """
    Run the sequence of gcloud commands to create project, SA, and enable APIs.
    Returns (success, message_or_json_content).
    """
    try:
        # 1. Create Project
        logger.info(f"[GCLOUD] Creating project {project_id}...")
        subprocess.check_call([
            "gcloud", "projects", "create", project_id, 
            "--name", "Gbot Prep Project",
            "--quiet"
        ])
        
        # 2. Create Service Account
        logger.info(f"[GCLOUD] Creating service account {service_account_name}...")
        subprocess.check_call([
            "gcloud", "iam", "service-accounts", "create", service_account_name,
            "--project", project_id,
            "--display-name", "Gbot Service Account",
            "--quiet"
        ])
        
        # 3. Disable IAM Policy (Optional/Best Effort)
        try:
            logger.info(f"[GCLOUD] Attempting to disable iam.disableServiceAccountKeyCreation...")
            subprocess.call([
                "gcloud", "resource-manager", "org-policies", "disable-enforce",
                "iam.disableServiceAccountKeyCreation",
                "--project", project_id,
                "--quiet"
            ])
        except Exception as e:
            logger.warning(f"[GCLOUD] Could not disable org policy (might not be needed): {e}")

        # 4. Create Keys
        sa_email = f"{service_account_name}@{project_id}.iam.gserviceaccount.com"
        key_path = f"/tmp/{project_id}-key.json"
        
        logger.info(f"[GCLOUD] Creating key for {sa_email}...")
        subprocess.check_call([
            "gcloud", "iam", "service-accounts", "keys", "create", key_path,
            "--iam-account", sa_email,
            "--project", project_id,
            "--quiet"
        ])
        
        # Read the key file
        with open(key_path, 'r') as f:
            key_content = f.read()
            
        # 5. Enable APIs
        apis = ["admin.googleapis.com", "siteverification.googleapis.com"]
        for api in apis:
            logger.info(f"[GCLOUD] Enabling API {api}...")
            subprocess.check_call([
                "gcloud", "services", "enable", api,
                "--project", project_id,
                "--quiet"
            ])
            
        logger.info(f"[GCLOUD] All resources created successfully for {project_id}")
        return True, key_content

    except subprocess.CalledProcessError as e:
        logger.error(f"[GCLOUD] Command failed: {e}")
        return False, f"Gcloud command failed: {e}"
    except Exception as e:
        logger.error(f"[GCLOUD] Resource creation failed: {e}")
        return False, f"Resource creation failed: {str(e)}"

def run_gcloud_setup(driver, email):
    """Run gcloud auth and setup steps"""
    try:
        process = subprocess.Popen(
            ["gcloud", "auth", "login", "--no-launch-browser"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        auth_url = None
        start_time = time.time()
        while time.time() - start_time < 30:
            line = process.stdout.readline()
            if not line: break
            logger.info(f"[GCLOUD] {line.strip()}")
            if "https://accounts.google.com" in line:
                import re
                urls = re.findall(r'https://accounts.google.com\S+', line)
                if urls:
                    auth_url = urls[0]
                    break
        
        if not auth_url:
            process.kill()
            return False, "Could not find auth URL in gcloud output"

        logger.info(f"[GCLOUD] Navigating to auth URL: {auth_url}")
        driver.get(auth_url)
        time.sleep(3)

        try:
            allow_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Allow')]")
            if allow_buttons:
                allow_buttons[0].click()
        except Exception as e:
            logger.warning(f"[GCLOUD] Error clicking Allow: {e}")

        time.sleep(3)

        code = None
        try:
            code_element = driver.find_element(By.TAG_NAME, "textarea")
            if code_element:
                code = code_element.get_attribute("value")
        except:
            pass

        if not code:
            process.kill()
            return False, "Could not retrieve verification code from page"

        logger.info(f"[GCLOUD] Sending verification code...")
        process.stdin.write(code + "\n")
        process.stdin.flush()
        
        process.wait(timeout=60)
        
        if process.returncode == 0:
            return True, "Gcloud authenticated successfully"
        else:
            return False, f"Gcloud exited with code {process.returncode}"

    except Exception as e:
        return False, f"Gcloud setup error: {str(e)}"
