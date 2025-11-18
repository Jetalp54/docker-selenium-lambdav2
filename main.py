#!/usr/bin/env python3
"""
AWS Lambda handler for Google Workspace 2-Step Verification and App Password setup.

Complete workflow:
1. Login to Google account
2. Handle post-login pages (Speedbump, verification, etc.)
3. Navigate to myaccount.google.com
4. Setup Authenticator App (extract TOTP secret)
5. Save secret to SFTP server (46.101.170.250:/home/Api_Appas/)
6. Enable 2-Step Verification
7. Generate App Password (random name)
8. Save App Password to S3 (app_passwords.txt)
"""

import os
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

# Disable SeleniumManager at module load time
os.environ['SE_SELENIUM_MANAGER'] = 'false'
os.environ['SELENIUM_MANAGER'] = 'false'
os.environ['SELENIUM_DISABLE_DRIVER_MANAGER'] = '1'

# Patch SeleniumManager to prevent it from running
try:
    from selenium.webdriver.common import selenium_manager
    original_binary_paths = selenium_manager.SeleniumManager.binary_paths
    def patched_binary_paths(self, *args, **kwargs):
        raise Exception("SeleniumManager is disabled in Lambda")
    selenium_manager.SeleniumManager.binary_paths = patched_binary_paths
    logger.info("[LAMBDA] SeleniumManager patched to prevent execution")
except Exception as e:
    logger.warning(f"[LAMBDA] Could not patch SeleniumManager: {e}")


def get_chrome_driver():
    """
    Initialize Chrome/Chromium driver optimized for Lambda environment.
    Uses pre-installed binaries from umihico base image.
    """
    # Clean up any Selenium cache in /tmp
    try:
        import shutil
        cache_dir = "/tmp/.cache/selenium"
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)
            logger.info("[LAMBDA] Cleaned /tmp/.cache/selenium directory")
    except Exception as e:
        logger.warning(f"[LAMBDA] Could not clean /tmp cache: {e}")
    
    # Point all cache/temp to /tmp (Lambda's writable directory)
    os.environ['HOME'] = '/tmp'
    os.environ['XDG_CACHE_HOME'] = '/tmp/.cache'
    os.environ['SELENIUM_MANAGER_CACHE'] = '/tmp/.cache/selenium'
    
    # Completely disable SeleniumManager - use multiple methods
    os.environ['SE_SELENIUM_MANAGER'] = 'false'
    os.environ['SELENIUM_MANAGER'] = 'false'
    os.environ['SELENIUM_DISABLE_DRIVER_MANAGER'] = '1'
    
    # Use Selenium Chrome options with anti-detection
    chrome_options = Options()
    
    # Core stability options for Lambda
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1280,800")
    chrome_options.add_argument("--lang=en-US")
    
    # Additional stability options for Lambda environment
    chrome_options.add_argument("--single-process")  # Critical for Lambda
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
    
    # Anti-detection options (Lambda-compatible)
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_experimental_option("prefs", {
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_settings.popups": 0,
    })
    
    # DEBUG: List what's actually in /opt to see what the base image contains
    logger.info("[LAMBDA] Checking /opt directory contents...")
    try:
        if os.path.exists('/opt'):
            opt_contents = os.listdir('/opt')
            logger.info(f"[LAMBDA] Contents of /opt: {opt_contents}")
            
            if os.path.exists('/opt/chrome'):
                chrome_contents = os.listdir('/opt/chrome')
                logger.info(f"[LAMBDA] Contents of /opt/chrome: {chrome_contents}")
    except Exception as e:
        logger.warning(f"[LAMBDA] Could not list /opt: {e}")
    
    # Find Chrome binary - check common Lambda paths
    chrome_binary_paths = [
        '/opt/chrome/chrome',
        '/opt/chrome/headless-chromium',
        '/opt/google/chrome/chrome',
        '/usr/bin/chromium-browser',
        '/usr/bin/chromium',
        '/usr/bin/google-chrome',
    ]
    
    chrome_binary = None
    for path in chrome_binary_paths:
        if os.path.exists(path) and os.access(path, os.X_OK):
            chrome_binary = path
            logger.info(f"[LAMBDA] Found Chrome binary at: {chrome_binary}")
            break
    
    # Fallback: try 'which' command
    if not chrome_binary:
        logger.warning("[LAMBDA] Chrome not found in common paths, trying 'which'...")
        try:
            import subprocess
            result = subprocess.run(['which', 'chrome'], capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and result.stdout.strip():
                chrome_binary = result.stdout.strip()
                logger.info(f"[LAMBDA] Found Chrome via 'which': {chrome_binary}")
        except Exception as e:
            logger.warning(f"[LAMBDA] 'which chrome' failed: {e}")
    
    # Last resort: search /opt recursively
    if not chrome_binary:
        logger.warning("[LAMBDA] Searching /opt recursively for Chrome...")
        try:
            for root, dirs, files in os.walk('/opt'):
                for file in files:
                    if file in ['chrome', 'chromium', 'google-chrome', 'headless-chromium']:
                        full_path = os.path.join(root, file)
                        if os.access(full_path, os.X_OK):
                            chrome_binary = full_path
                            logger.info(f"[LAMBDA] Found Chrome via recursive search: {chrome_binary}")
                            break
                if chrome_binary:
                    break
        except Exception as e:
            logger.warning(f"[LAMBDA] Could not walk /opt: {e}")
    
    if not chrome_binary:
        # List contents for debugging
        logger.error("[LAMBDA] Chrome binary not found! Listing /opt contents for debugging:")
        try:
            for root, dirs, files in os.walk('/opt'):
                logger.error(f"[LAMBDA] {root}: dirs={dirs}, files={files[:10]}")
        except Exception as e:
            logger.warning(f"[LAMBDA] Could not walk /opt: {e}")
        
        raise Exception("Chrome binary not found! Cannot proceed without Chrome binary path. Checked paths: " + str(chrome_binary_paths))
    
    # Find ChromeDriver - check common Lambda paths
    chromedriver_paths = [
        '/opt/chromedriver',
        '/usr/bin/chromedriver',
        '/usr/local/bin/chromedriver',
    ]
    
    chromedriver_path = None
    for path in chromedriver_paths:
        if os.path.exists(path) and os.access(path, os.X_OK):
            chromedriver_path = path
            logger.info(f"[LAMBDA] Found ChromeDriver at: {chromedriver_path}")
            break
    
    # Fallback: try 'which' command
    if not chromedriver_path:
        logger.warning("[LAMBDA] ChromeDriver not found in common paths, trying 'which'...")
        try:
            import subprocess
            result = subprocess.run(['which', 'chromedriver'], capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and result.stdout.strip():
                chromedriver_path = result.stdout.strip()
                logger.info(f"[LAMBDA] Found ChromeDriver via 'which': {chromedriver_path}")
        except Exception as e:
            logger.warning(f"[LAMBDA] 'which chromedriver' failed: {e}")
    
    if not chromedriver_path:
        logger.warning("[LAMBDA] ChromeDriver path not found, using PATH")
        chromedriver_path = "chromedriver"
    
    try:
        # Create Service with explicit ChromeDriver path
        service = Service(executable_path=chromedriver_path)
        
        # Set browser executable path in options - CRITICAL to prevent SeleniumManager
        chrome_options.binary_location = chrome_binary
        
        # Set environment variables to disable SeleniumManager
        os.environ['SE_SELENIUM_MANAGER'] = 'false'
        os.environ['SELENIUM_MANAGER'] = 'false'
        os.environ['SELENIUM_DISABLE_DRIVER_MANAGER'] = '1'
        
        logger.info(f"[LAMBDA] Initializing Chrome driver with ChromeDriver: {chromedriver_path}, Chrome: {chrome_binary}")
        logger.info(f"[LAMBDA] Environment: SE_SELENIUM_MANAGER={os.environ.get('SE_SELENIUM_MANAGER')}")
        
        # Create driver with explicit paths - this bypasses SeleniumManager
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Set page load timeout BEFORE any operations
        driver.set_page_load_timeout(60)
        
        # Wait for Chrome to fully initialize
        time.sleep(2)
        
        # Inject anti-detection scripts AFTER driver is stable
        # Do this BEFORE any navigation to ensure it's applied to all pages
        try:
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                    window.chrome = {runtime: {}};
                '''
            })
            logger.info("[LAMBDA] Anti-detection script injected successfully")
        except Exception as e:
            logger.warning(f"[LAMBDA] Could not inject anti-detection script (non-critical): {e}")
            # Continue anyway - this is not critical, but log it
        
        logger.info("[LAMBDA] Chrome driver created successfully")
        return driver
    except Exception as e:
        logger.error(f"[LAMBDA] Failed to initialize Chrome driver: {e}")
        logger.error(traceback.format_exc())
        
        # Last resort: try with absolute minimal options
        try:
            logger.info("[LAMBDA] Retrying with absolute minimal options...")
            minimal_options = Options()
            # Only the absolute essentials - nothing more
            minimal_options.add_argument("--headless=new")
            minimal_options.add_argument("--no-sandbox")
            minimal_options.add_argument("--disable-dev-shm-usage")
            minimal_options.add_argument("--disable-gpu")
            minimal_options.add_argument("--single-process")  # Critical for Lambda stability
            
            if chrome_binary:
                minimal_options.binary_location = chrome_binary
            
            # Use Service with explicit paths
            service = Service(executable_path=chromedriver_path)
            driver = webdriver.Chrome(service=service, options=minimal_options)
            
            # Wait but DO NOT verify - verification causes crashes
            time.sleep(3)
            
            logger.info("[LAMBDA] Chrome driver created with minimal options")
            return driver
        except Exception as e2:
            logger.error(f"[LAMBDA] Final retry also failed: {e2}")
            logger.error(traceback.format_exc())
            raise Exception(f"Chrome driver initialization failed: {e2}. Chrome: {chrome_binary}, ChromeDriver: {chromedriver_path}")


# =====================================================================
# Helper functions for Selenium
# =====================================================================


def wait_for_xpath(driver, xpath, timeout=20):
    """Wait for an element by XPath and return it."""
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, xpath))
    )


def wait_for_clickable_xpath(driver, xpath, timeout=20):
    """Wait for an element to be clickable and return it."""
    return WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, xpath))
    )


def click_xpath(driver, xpath, timeout=20):
    """Wait for element to be clickable and click it."""
    element = wait_for_clickable_xpath(driver, xpath, timeout)
    element.click()
    return element


def element_exists(driver, xpath, timeout=5):
    """Check if element exists (returns True/False, doesn't raise)."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        return True
    except TimeoutException:
        return False


def find_element_with_fallback(driver, xpaths, timeout=20, description="element"):
    """
    Try multiple XPath expressions to find an element.
    Returns the first found element or None.
    """
    for xpath in xpaths:
        try:
            elem = wait_for_xpath(driver, xpath, timeout=timeout)
            if elem:
                logger.info(f"[STEP] Found {description} using xpath: {xpath}")
                return elem
        except TimeoutException:
            continue
    logger.warning(f"[STEP] Could not find {description} with any of the provided xpaths")
    return None


# =====================================================================
# SFTP upload for TOTP secret
# =====================================================================


def upload_secret_to_sftp(email, secret_key):
    """
    Upload the secret key to an SFTP server.
    Environment vars needed:
      SECRET_SFTP_HOST
      SECRET_SFTP_PORT         (optional, default 22)
      SECRET_SFTP_USER
      SECRET_SFTP_PASSWORD     (OR SECRET_SFTP_KEY for private key content)
      SECRET_SFTP_REMOTE_DIR   (directory to store secrets)
    """
    host = os.environ.get("SECRET_SFTP_HOST", "46.101.170.250")
    user = os.environ.get("SECRET_SFTP_USER")
    remote_dir = os.environ.get("SECRET_SFTP_REMOTE_DIR", "/home/Api_Appas/")
    port = int(os.environ.get("SECRET_SFTP_PORT", "22"))
    password = os.environ.get("SECRET_SFTP_PASSWORD")
    key_content = os.environ.get("SECRET_SFTP_KEY")

    if not host or not user:
        logger.warning("SFTP parameters not fully configured (SECRET_SFTP_HOST / SECRET_SFTP_USER).")
        return None, None

    try:
        # Create filename based on email
        safe_email = email.replace("@", "_at_").replace(".", "_")
        remote_filename = f"{safe_email}_secret.txt"
        remote_path = os.path.join(remote_dir, remote_filename).replace("\\", "/")

        transport = paramiko.Transport((host, port))
        
        # Authenticate with password or key
        if password:
            transport.connect(username=user, password=password)
        elif key_content:
            key_file = io.StringIO(key_content)
            pkey = paramiko.RSAKey.from_private_key(key_file)
            transport.connect(username=user, pkey=pkey)
        else:
            logger.error("No SFTP password or key provided.")
            return None, None

        sftp = paramiko.SFTPClient.from_transport(transport)
        
        # Ensure remote directory exists
        try:
            sftp.stat(remote_dir)
        except IOError:
            logger.info(f"[SFTP] Creating remote directory: {remote_dir}")
            sftp.mkdir(remote_dir)

        # Write secret to file
        with sftp.open(remote_path, 'w') as f:
            f.write(secret_key)
        
        logger.info(f"[SFTP] Secret uploaded to {host}:{remote_path}")
        sftp.close()
        transport.close()
        
        return host, remote_path

    except Exception as e:
        logger.error(f"[SFTP] Failed to upload secret: {e}")
        logger.error(traceback.format_exc())
        return None, None


# =====================================================================
# S3 upload for App Passwords
# =====================================================================


def append_app_password_to_s3(email, app_password):
    """
    Append the app password to a global S3 file (app_passwords.txt).
    Environment vars:
      APP_PASSWORDS_S3_BUCKET  (required)
      APP_PASSWORDS_S3_KEY     (optional, default app_passwords.txt)
    """
    bucket = os.environ.get("APP_PASSWORDS_S3_BUCKET")
    key = os.environ.get("APP_PASSWORDS_S3_KEY", "app_passwords.txt")

    if not bucket:
        logger.error("[S3] APP_PASSWORDS_S3_BUCKET not configured.")
        return False, bucket, key

    try:
        s3 = boto3.client("s3")
        
        # Try to fetch existing file content
        existing_content = ""
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            existing_content = obj['Body'].read().decode('utf-8')
        except s3.exceptions.NoSuchKey:
            logger.info(f"[S3] {key} does not exist yet, will create it.")
        except Exception as e:
            logger.warning(f"[S3] Could not read existing file: {e}")

        # Append new entry
        new_line = f"{email}|{app_password}\n"
        updated_content = existing_content + new_line

        # Write back
        s3.put_object(Bucket=bucket, Key=key, Body=updated_content.encode('utf-8'))
        logger.info(f"[S3] App password appended to s3://{bucket}/{key}")
        return True, bucket, key

    except Exception as e:
        logger.error(f"[S3] Failed to write app_passwords.txt to S3: {e}")
        return False, bucket, key


# =====================================================================
# Step 1: Login + optional existing 2FA handling
# =====================================================================


def handle_post_login_pages(driver, max_attempts=20):
    """
    Handle all intermediate pages after login (Speedbump, verification prompts, etc.)
    before reaching myaccount.google.com
    Returns (success: bool, error_code: str|None, error_message: str|None)
    """
    logger.info("[STEP] Handling post-login pages (Speedbump, verification, etc.)")
    
    for attempt in range(max_attempts):
        time.sleep(3)  # Wait between checks
        
        try:
            current_url = driver.current_url
            logger.info(f"[STEP] Post-login check {attempt + 1}/{max_attempts}: URL = {current_url}")
            
            # Check if we've reached myaccount
            if "myaccount.google.com" in current_url:
                logger.info("[STEP] Successfully reached myaccount.google.com")
                return True, None, None
            
            # Handle Speedbump page (Don't now / Continue)
            if "speedbump" in current_url or element_exists(driver, "//button[contains(., 'Continue') or contains(., 'Next')]", timeout=2):
                logger.info("[STEP] Speedbump or confirmation page detected")
                
                # Try multiple button selectors for Continue/Next
                continue_button_xpaths = [
                    "//button[contains(., 'Continue')]",
                    "//button[contains(., 'Next')]",
                    "//span[contains(text(), 'Continue')]/ancestor::button",
                    "//span[contains(text(), 'Next')]/ancestor::button",
                    "//div[@role='button' and contains(., 'Continue')]",
                    "//div[@role='button' and contains(., 'Next')]",
                ]
                
                clicked = False
                for xpath in continue_button_xpaths:
                    try:
                        if element_exists(driver, xpath, timeout=2):
                            click_xpath(driver, xpath, timeout=5)
                            logger.info(f"[STEP] Clicked Continue/Next button using: {xpath}")
                            clicked = True
                            time.sleep(2)
                            break
                    except Exception as e:
                        logger.debug(f"[STEP] Could not click button with xpath {xpath}: {e}")
                        continue
                
                if not clicked:
                    logger.warning("[STEP] Could not find Continue/Next button, checking for 'Don't now' button")
                    # Try "Don't now" or "Not now" or "Skip"
                    skip_button_xpaths = [
                        "//button[contains(., \"Don't now\")]",
                        "//button[contains(., 'Not now')]",
                        "//button[contains(., 'Skip')]",
                        "//span[contains(text(), \"Don't now\")]/ancestor::button",
                        "//span[contains(text(), 'Not now')]/ancestor::button",
                        "//span[contains(text(), 'Skip')]/ancestor::button",
                    ]
                    
                    for xpath in skip_button_xpaths:
                        try:
                            if element_exists(driver, xpath, timeout=2):
                                click_xpath(driver, xpath, timeout=5)
                                logger.info(f"[STEP] Clicked Skip/Don't now button using: {xpath}")
                                time.sleep(2)
                                break
                        except Exception as e:
                            logger.debug(f"[STEP] Could not click skip button with xpath {xpath}: {e}")
                            continue
                
                continue  # Go to next iteration to check new page
            
            # Handle "Verify it's you" or recovery info pages
            if "verify" in current_url.lower() or element_exists(driver, "//h1[contains(., 'Verify')]", timeout=2):
                logger.info("[STEP] Verification page detected")
                
                # Try to click Continue/Next/Skip
                verify_button_xpaths = [
                    "//button[contains(., 'Continue')]",
                    "//button[contains(., 'Next')]",
                    "//button[contains(., 'Skip')]",
                    "//span[contains(text(), 'Continue')]/ancestor::button",
                    "//span[contains(text(), 'Next')]/ancestor::button",
                    "//span[contains(text(), 'Skip')]/ancestor::button",
                ]
                
                for xpath in verify_button_xpaths:
                    try:
                        if element_exists(driver, xpath, timeout=2):
                            click_xpath(driver, xpath, timeout=5)
                            logger.info(f"[STEP] Clicked verification button using: {xpath}")
                            time.sleep(2)
                            break
                    except Exception as e:
                        logger.debug(f"[STEP] Could not click verify button with xpath {xpath}: {e}")
                        continue
                
                continue  # Go to next iteration
            
            # Handle "Add recovery email/phone" prompts
            if "recovery" in current_url.lower() or element_exists(driver, "//h1[contains(., 'recovery') or contains(., 'Recovery')]", timeout=2):
                logger.info("[STEP] Recovery info page detected")
                
                # Try to skip/not now
                recovery_skip_xpaths = [
                    "//button[contains(., 'Skip')]",
                    "//button[contains(., 'Not now')]",
                    "//button[contains(., \"Don't now\")]",
                    "//button[contains(., 'Done')]",
                    "//span[contains(text(), 'Skip')]/ancestor::button",
                    "//span[contains(text(), 'Not now')]/ancestor::button",
                    "//span[contains(text(), 'Done')]/ancestor::button",
                ]
                
                for xpath in recovery_skip_xpaths:
                    try:
                        if element_exists(driver, xpath, timeout=2):
                            click_xpath(driver, xpath, timeout=5)
                            logger.info(f"[STEP] Clicked skip recovery button using: {xpath}")
                            time.sleep(2)
                            break
                    except Exception as e:
                        logger.debug(f"[STEP] Could not click recovery skip button with xpath {xpath}: {e}")
                        continue
                
                continue  # Go to next iteration
            
            # Handle "Review account info" or similar
            if element_exists(driver, "//h1[contains(., 'Review')]", timeout=2):
                logger.info("[STEP] Review page detected")
                
                # Try to continue/next
                review_button_xpaths = [
                    "//button[contains(., 'Continue')]",
                    "//button[contains(., 'Next')]",
                    "//button[contains(., 'I agree')]",
                    "//button[contains(., 'Agree')]",
                    "//span[contains(text(), 'Continue')]/ancestor::button",
                    "//span[contains(text(), 'Next')]/ancestor::button",
                ]
                
                for xpath in review_button_xpaths:
                    try:
                        if element_exists(driver, xpath, timeout=2):
                            click_xpath(driver, xpath, timeout=5)
                            logger.info(f"[STEP] Clicked review button using: {xpath}")
                            time.sleep(2)
                            break
                    except Exception as e:
                        logger.debug(f"[STEP] Could not click review button with xpath {xpath}: {e}")
                        continue
                
                continue  # Go to next iteration
            
            # If we don't recognize the page but haven't reached myaccount, try generic Continue/Next
            if "google.com" in current_url and "myaccount" not in current_url:
                logger.info("[STEP] Unrecognized intermediate Google page, trying generic Continue/Next")
                
                generic_button_xpaths = [
                    "//button[contains(., 'Continue')]",
                    "//button[contains(., 'Next')]",
                    "//button[contains(., 'Done')]",
                    "//button[contains(., 'I agree')]",
                    "//span[contains(text(), 'Continue')]/ancestor::button",
                    "//span[contains(text(), 'Next')]/ancestor::button",
                    "//span[contains(text(), 'Done')]/ancestor::button",
                ]
                
                found_button = False
                for xpath in generic_button_xpaths:
                    try:
                        if element_exists(driver, xpath, timeout=2):
                            click_xpath(driver, xpath, timeout=5)
                            logger.info(f"[STEP] Clicked generic button using: {xpath}")
                            time.sleep(2)
                            found_button = True
                            break
                    except Exception as e:
                        logger.debug(f"[STEP] Could not click generic button with xpath {xpath}: {e}")
                        continue
                
                if not found_button:
                    # No button found, just wait and check next iteration
                    logger.info("[STEP] No recognizable button found, waiting...")
            
        except Exception as e:
            logger.warning(f"[STEP] Error during post-login page handling: {e}")
            # Continue anyway, might resolve in next iteration
    
    # If we've exhausted all attempts and still not on myaccount
    try:
        final_url = driver.current_url
        logger.warning(f"[STEP] Did not reach myaccount.google.com after {max_attempts} attempts. Final URL: {final_url}")
        
        # Check if we're at least on a Google domain
        if "google.com" in final_url:
            logger.info("[STEP] Still on Google domain, attempting to navigate directly to myaccount")
            driver.get("https://myaccount.google.com/")
            time.sleep(5)
            
            if "myaccount.google.com" in driver.current_url:
                logger.info("[STEP] Successfully navigated to myaccount.google.com directly")
                return True, None, None
        
        return False, "POST_LOGIN_TIMEOUT", f"Could not reach myaccount.google.com. Final URL: {final_url}"
    except Exception as e:
        return False, "POST_LOGIN_ERROR", str(e)


def login_google(driver, email, password, known_totp_secret=None):
    """
    Login to Google. If a 2FA code is requested and we know a TOTP secret,
    we will try to solve it; otherwise we fail with an explicit error.
    """
    logger.info(f"[STEP] Login started for {email}")
    
    # Don't check driver health before navigation - it can cause crashes in Lambda
    # Just proceed directly to navigation
    
    # Navigate with timeout and error handling
    try:
        logger.info("[STEP] Navigating to Google login page...")
        driver.get("https://accounts.google.com/signin/v2/identifier?hl=en&flowName=GlifWebSignIn")
        logger.info("[STEP] Navigation to Google login page completed")
        time.sleep(3)  # Increased wait for page to fully load in Lambda
        logger.info("[STEP] Page stabilized, proceeding with login")
    except Exception as nav_error:
        logger.error(f"[STEP] Navigation failed: {nav_error}")
        logger.error(traceback.format_exc())
        return False, "navigation_failed", str(nav_error)

    try:
        # Enter email
        email_input = wait_for_xpath(driver, "//input[@id='identifierId']", timeout=30)
        email_input.clear()
        time.sleep(0.5)
        email_input.send_keys(email)
        logger.info("[STEP] Email entered")
        time.sleep(1)
        
        # Click Next button
        email_next_xpaths = [
            "//*[@id='identifierNext']",
            "//button[@id='identifierNext']",
            "//span[contains(text(), 'Next')]/ancestor::button",
        ]
        email_next = find_element_with_fallback(driver, email_next_xpaths, timeout=20, description="email next button")
        if email_next:
            click_xpath(driver, "//*[@id='identifierNext']", timeout=10)
        else:
            # Try Enter key
            email_input.send_keys(Keys.RETURN)
        logger.info("[STEP] Email submitted")

        # Wait for password field
        time.sleep(3)  # Increased wait for password page to load

        # Enter password
        password_input_xpaths = [
            "//input[@name='Passwd']",
            "//input[@type='password']",
            "//input[@aria-label*='password' or @aria-label*='Password']",
        ]
        password_input = find_element_with_fallback(driver, password_input_xpaths, timeout=30, description="password input")
        if not password_input:
            return False, "LOGIN_PASSWORD_FIELD_NOT_FOUND", "Password field not found after email submission"
        
        password_input.clear()
        time.sleep(0.5)
        password_input.send_keys(password)
        logger.info("[STEP] Password entered")
        time.sleep(1)
        
        # Click Next button
        pw_next_xpaths = [
            "//*[@id='passwordNext']",
            "//button[@id='passwordNext']",
            "//span[contains(text(), 'Next')]/ancestor::button",
        ]
        pw_next = find_element_with_fallback(driver, pw_next_xpaths, timeout=20, description="password next button")
        if pw_next:
            click_xpath(driver, "//*[@id='passwordNext']", timeout=10)
        else:
            password_input.send_keys(Keys.RETURN)
        logger.info("[STEP] Password submitted")

        # Wait for potential challenge pages or account home
        max_wait_attempts = 15
        wait_interval = 3
        current_url = None
        
        for attempt in range(max_wait_attempts):
            time.sleep(wait_interval)
            try:
                current_url = driver.current_url
                logger.info(f"[STEP] Check {attempt + 1}/{max_wait_attempts}: URL = {current_url}")
            except Exception as e:
                logger.error(f"[STEP] Failed to get current URL: {e}")
                return False, "driver_crashed", f"Driver crashed while checking URL: {e}"
            
            # Check for account verification/ID verification required
            if "speedbump/idvreenable" in current_url or "idvreenable" in current_url:
                logger.error("[STEP] ID verification required - manual intervention needed")
                return False, "ID_VERIFICATION_REQUIRED", "Manual ID verification required"
            
            # If we're logged in, return success
            if any(domain in current_url for domain in ["myaccount.google.com", "mail.google.com", "accounts.google.com/b/0", "accounts.google.com/servicelogin"]):
                logger.info("[STEP] Login success - reached account page")
                return True, None, None
            
            # Check for various challenge types
            challenge_indicators = [
                "challenge" in current_url,
                "signin/challenge" in current_url,
                element_exists(driver, "//input[@type='tel' or @autocomplete='one-time-code']", timeout=5),
                element_exists(driver, "//input[contains(@aria-label, 'code') or contains(@aria-label, 'Code')]", timeout=5),
                element_exists(driver, "//div[contains(text(), 'Enter the code') or contains(text(), 'verification code')]", timeout=5),
            ]
            
            if any(challenge_indicators):
                logger.info(f"[STEP] Challenge page detected (attempt {attempt + 1})")
                break
            
            # If no challenge detected and not logged in, continue waiting
            if attempt < max_wait_attempts - 1:
                logger.info(f"[STEP] Waiting for page to load... ({attempt + 1}/{max_wait_attempts})")
        
        # Handle challenge page if detected
        if current_url and ("challenge" in current_url or "signin/challenge" in current_url or any(challenge_indicators)):
            logger.info("[STEP] Processing challenge page")
            
            # Check if it's a TOTP challenge (we can handle) or other challenge (phone, etc.)
            otp_input_xpaths = [
                "//input[@type='tel']",
                "//input[@autocomplete='one-time-code']",
                "//input[@type='text' and contains(@aria-label, 'code')]",
                "//input[contains(@aria-label, 'Code')]",
            ]
            
            otp_input = None
            for xpath in otp_input_xpaths:
                try:
                    otp_input = wait_for_xpath(driver, xpath, timeout=15)
                    if otp_input:
                        break
                except:
                    continue
            
            if otp_input:
                # It's a TOTP challenge - handle it with retries
                logger.info("[STEP] TOTP challenge detected")
                if not known_totp_secret:
                    logger.error("[STEP] 2FA is required but no TOTP secret is available")
                    return False, "2FA_REQUIRED", "2FA required but secret is unknown"
                
                # Generate TOTP code with retries
                otp_code = None
                totp = None
                for retry in range(3):
                    try:
                        clean_secret = known_totp_secret.replace(" ", "").upper()
                        totp = pyotp.TOTP(clean_secret)
                        otp_code = totp.now()
                        logger.info(f"[STEP] Generated TOTP code (attempt {retry + 1}): {otp_code}")
                        break
                    except Exception as e:
                        logger.warning(f"[STEP] TOTP generation failed (attempt {retry + 1}): {e}")
                        if retry < 2:
                            time.sleep(1)
                        else:
                            return False, "TOTP_GENERATION_FAILED", str(e)
                
                # Fill and submit OTP with retries
                for retry in range(3):
                    try:
                        # Clear and set OTP value
                        driver.execute_script("arguments[0].value = '';", otp_input)
                        driver.execute_script("arguments[0].value = arguments[1];", otp_input, otp_code)
                        logger.info(f"[STEP] OTP code entered (attempt {retry + 1}): {otp_code}")
                        
                        # Submit OTP
                        submit_btn_xpaths = [
                            "//button[contains(@type,'submit')]",
                            "//button[@role='button' and contains(., 'Next')]",
                            "//span[contains(text(), 'Next')]/ancestor::button",
                            "//button[contains(., 'Verify')]",
                        ]
                        
                        submitted = False
                        for btn_xpath in submit_btn_xpaths:
                            if element_exists(driver, btn_xpath, timeout=5):
                                click_xpath(driver, btn_xpath, timeout=10)
                                submitted = True
                                break
                        
                        if not submitted:
                            otp_input.send_keys(Keys.RETURN)
                        
                        # Wait and check result
                        time.sleep(5)
                        current_url = driver.current_url
                        
                        # Check if login succeeded
                        if any(domain in current_url for domain in ["myaccount.google.com", "mail.google.com", "accounts.google.com/b/0"]):
                            logger.info("[STEP] Login success after 2FA")
                            return True, None, None
                        
                        # If still on challenge page, might need new code
                        if "challenge" in current_url:
                            if retry < 2:
                                logger.warning(f"[STEP] Still on challenge page, retrying with new code (attempt {retry + 1})")
                                try:
                                    otp_code = totp.now()
                                except:
                                    pass
                                time.sleep(2)
                                continue
                            else:
                                return False, "2FA_VERIFICATION_FAILED", "OTP submitted but verification failed after 3 attempts"
                        else:
                            # Different page - might be success
                            logger.info(f"[STEP] Navigated to different page: {current_url}")
                            break
                            
                    except Exception as e:
                        logger.error(f"[STEP] Failed to submit 2FA code (attempt {retry + 1}): {e}")
                        if retry < 2:
                            time.sleep(2)
                        else:
                            return False, "2FA_SUBMIT_FAILED", str(e)
                
                # Final check
                time.sleep(2)
                current_url = driver.current_url
                if any(domain in current_url for domain in ["myaccount.google.com", "mail.google.com", "accounts.google.com/b/0"]):
                    logger.info("[STEP] Login success after 2FA (final check)")
                    return True, None, None
                else:
                    return False, "2FA_VERIFICATION_FAILED", f"OTP submitted but still on: {current_url}"
            else:
                # Other challenge type (phone verification, etc.) - cannot handle automatically
                logger.error(f"[STEP] Unsupported challenge type detected on: {current_url}")
                return False, "UNSUPPORTED_CHALLENGE", f"Challenge page detected but not TOTP: {current_url}"
        
        # If we get here, we waited but didn't detect login or challenge
        logger.warning(f"[STEP] Login status unclear after waiting. Final URL: {current_url}")
        return False, "LOGIN_TIMEOUT", f"Could not determine login status. URL: {current_url}"

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
    """
    From myaccount, navigate to Security / 2-Step Verification section.
    """
    try:
        logger.info("[STEP] Navigating to Security page")
        driver.get("https://myaccount.google.com/security")
        time.sleep(4)
        
        logger.info("[STEP] Navigating to 2-Step Verification page")
        driver.get("https://myaccount.google.com/signinoptions/two-step-verification")
        time.sleep(4)
        
        logger.info("[STEP] Navigation to security completed")
        return True, None, None
    except Exception as e:
        logger.error(f"[STEP] Failed to navigate to security: {e}")
        logger.error(traceback.format_exc())
        return False, "NAVIGATION_FAILED", str(e)


# =====================================================================
# Step 3: Setup Authenticator App (extract secret)
# =====================================================================


def setup_authenticator_app(driver, email):
    """
    Enable/Setup Authenticator App and extract the TOTP secret key.
    Then save it to SFTP server.
    """
    try:
        logger.info("[STEP] Setting up Authenticator App")
        
        # Click "GET STARTED" or "Set up" button
        setup_button_xpaths = [
            "//button[contains(., 'GET STARTED')]",
            "//button[contains(., 'Set up')]",
            "//button[contains(., 'Add')]",
            "//span[contains(text(), 'GET STARTED')]/ancestor::button",
            "//span[contains(text(), 'Set up')]/ancestor::button",
        ]
        
        setup_clicked = False
        for xpath in setup_button_xpaths:
            if element_exists(driver, xpath, timeout=5):
                click_xpath(driver, xpath, timeout=5)
                setup_clicked = True
                logger.info("[STEP] Clicked setup button for Authenticator App")
                time.sleep(3)
                break
        
        if not setup_clicked:
            logger.warning("[STEP] Could not find setup button, trying to navigate directly")
            driver.get("https://myaccount.google.com/signinoptions/two-step-verification/enroll-welcome")
            time.sleep(4)
        
        # Look for "Authenticator app" option
        authenticator_xpaths = [
            "//div[contains(., 'Authenticator app')]",
            "//span[contains(text(), 'Authenticator app')]",
            "//button[contains(., 'Authenticator app')]",
        ]
        
        for xpath in authenticator_xpaths:
            if element_exists(driver, xpath, timeout=5):
                try:
                    click_xpath(driver, xpath, timeout=5)
                    logger.info("[STEP] Selected Authenticator app option")
                    time.sleep(3)
                    break
                except:
                    continue
        
        # Look for secret key display
        secret_key = None
        
        # Method 1: Look for "Can't scan it?" or "Enter this text code" link
        cant_scan_xpaths = [
            "//a[contains(., \"Can't scan\")]",
            "//button[contains(., \"Can't scan\")]",
            "//span[contains(text(), \"Can't scan\")]/ancestor::*[@role='button' or @role='link']",
        ]
        
        for xpath in cant_scan_xpaths:
            if element_exists(driver, xpath, timeout=5):
                click_xpath(driver, xpath, timeout=5)
                logger.info("[STEP] Clicked 'Can't scan' to reveal text code")
                time.sleep(2)
                break
        
        # Method 2: Extract secret from displayed text
        secret_xpaths = [
            "//code",
            "//span[contains(@class, 'secret')]",
            "//*[contains(text(), ' ') and string-length(text()) >= 16]",
        ]
        
        for xpath in secret_xpaths:
            try:
                elements = driver.find_elements(By.XPATH, xpath)
                for elem in elements:
                    text = elem.text.strip().replace(" ", "").upper()
                    # TOTP secrets are typically 16-32 chars, base32
                    if len(text) >= 16 and text.isalnum():
                        secret_key = text
                        logger.info(f"[STEP] Secret key extracted: {secret_key[:4]}****")
                        break
                if secret_key:
                    break
            except:
                continue
        
        if not secret_key:
            logger.error("[STEP] Could not extract secret key")
            return False, None, "SECRET_KEY_NOT_FOUND", "Could not find or extract TOTP secret"
        
        # Save secret to SFTP
        logger.info(f"[STEP] Saving secret key to SFTP server")
        sftp_host, sftp_path = upload_secret_to_sftp(email, secret_key)
        if not sftp_host:
            logger.warning("[STEP] SFTP upload failed or not configured")
        else:
            logger.info(f"[STEP] Secret saved to SFTP: {sftp_host}:{sftp_path}")
        
        # Enter secret into authenticator to verify
        try:
            totp = pyotp.TOTP(secret_key)
            code = totp.now()
            logger.info(f"[STEP] Generated verification code: {code}")
            
            # Look for verification code input
            code_input_xpaths = [
                "//input[@type='tel']",
                "//input[@autocomplete='one-time-code']",
                "//input[contains(@aria-label, 'code') or contains(@aria-label, 'Code')]",
            ]
            
            code_input = find_element_with_fallback(driver, code_input_xpaths, timeout=10, description="verification code input")
            if code_input:
                code_input.clear()
                code_input.send_keys(code)
                logger.info("[STEP] Entered verification code")
                time.sleep(1)
                
                # Submit
                submit_xpaths = [
                    "//button[contains(., 'Next')]",
                    "//button[contains(., 'Verify')]",
                    "//button[@type='submit']",
                    "//span[contains(text(), 'Next')]/ancestor::button",
                ]
                
                for xpath in submit_xpaths:
                    if element_exists(driver, xpath, timeout=5):
                        click_xpath(driver, xpath, timeout=5)
                        logger.info("[STEP] Submitted verification code")
                        time.sleep(3)
                        break
        except Exception as e:
            logger.warning(f"[STEP] Could not verify authenticator: {e}")
        
        logger.info("[STEP] Authenticator app setup completed")
        return True, secret_key, None, None
        
    except Exception as e:
        logger.error(f"[STEP] Exception during authenticator setup: {e}")
        logger.error(traceback.format_exc())
        return False, None, "AUTHENTICATOR_EXCEPTION", str(e)


# =====================================================================
# Step 4: Ensure 2-Step Verification is enabled
# =====================================================================


def ensure_two_step_enabled(driver, email):
    """
    Ensure 2-Step Verification is fully enabled.
    """
    try:
        logger.info("[STEP] Ensuring 2-Step Verification is enabled")
        
        # Navigate to 2SV page
        driver.get("https://myaccount.google.com/signinoptions/two-step-verification")
        time.sleep(4)
        
        # Look for "Turn on" or "Enable" button
        enable_xpaths = [
            "//button[contains(., 'Turn on')]",
            "//button[contains(., 'Enable')]",
            "//span[contains(text(), 'Turn on')]/ancestor::button",
            "//span[contains(text(), 'Enable')]/ancestor::button",
        ]
        
        for xpath in enable_xpaths:
            if element_exists(driver, xpath, timeout=5):
                click_xpath(driver, xpath, timeout=5)
                logger.info("[STEP] Clicked enable 2SV button")
                time.sleep(3)
                break
        
        # Handle any confirmation prompts
        confirm_xpaths = [
            "//button[contains(., 'Continue')]",
            "//button[contains(., 'Done')]",
            "//button[contains(., 'Got it')]",
            "//span[contains(text(), 'Continue')]/ancestor::button",
            "//span[contains(text(), 'Done')]/ancestor::button",
        ]
        
        for xpath in confirm_xpaths:
            if element_exists(driver, xpath, timeout=5):
                try:
                    click_xpath(driver, xpath, timeout=5)
                    logger.info("[STEP] Clicked confirmation button")
                    time.sleep(2)
                except:
                    continue
        
        logger.info("[STEP] 2-Step Verification is enabled")
        return True, None, None
        
    except Exception as e:
        logger.error(f"[STEP] Exception ensuring 2SV: {e}")
        logger.error(traceback.format_exc())
        return False, "2SV_ENABLE_FAILED", str(e)


# =====================================================================
# Step 5: Generate App password
# =====================================================================


def generate_app_password(driver, email):
    """
    Navigate to app passwords page and generate a new app password.
    Includes page refresh logic if input field not found.
    Uses random app name.
    Return the password string.
    """
    import random
    import string
    
    try:
        logger.info("[STEP] Opening App Passwords page")
        driver.get("https://myaccount.google.com/apppasswords?hl=en")
        time.sleep(5)

        # Check if we need to select app type first
        app_name_xpaths = [
            "//input[@aria-label='App name']",
            "//input[contains(@placeholder, 'app') or contains(@placeholder, 'name')]",
            "//input[@type='text' and not(@type='password')]",
            "//input[contains(@class, 'app-name')]",
        ]
        
        # Try to find app name field - with retry and refresh
        max_attempts = 3
        app_name_field = None
        
        for attempt in range(max_attempts):
            app_name_field = find_element_with_fallback(driver, app_name_xpaths, timeout=10, description="app name field")
            
            if app_name_field:
                break
            else:
                if attempt < max_attempts - 1:
                    logger.warning(f"[STEP] App name field not found (attempt {attempt + 1}/{max_attempts}), refreshing page...")
                    driver.refresh()
                    time.sleep(5)
                else:
                    logger.error("[STEP] App name field not found after multiple attempts")
                    return False, None, "APP_NAME_FIELD_NOT_FOUND", "Could not locate app name input field"
        
        # Generate random app name
        random_suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        app_name = f"SMTP-{random_suffix}"
        
        app_name_field.clear()
        time.sleep(0.5)
        app_name_field.send_keys(app_name)
        logger.info(f"[STEP] Entered random app name: {app_name}")
        time.sleep(1)

        # Click Generate/Create button
        generate_button_xpaths = [
            "//button[contains(., 'Generate')]",
            "//button[contains(., 'Create')]",
            "//button[contains(@aria-label, 'Generate')]",
            "//button[contains(@aria-label, 'Create')]",
            "//span[contains(text(), 'Generate')]/ancestor::button",
            "//span[contains(text(), 'Create')]/ancestor::button",
            "//button[contains(., 'Next')]",
        ]
        
        generate_clicked = False
        for xpath in generate_button_xpaths:
            if element_exists(driver, xpath, timeout=5):
                click_xpath(driver, xpath, timeout=5)
                generate_clicked = True
                logger.info("[STEP] Clicked generate/create button")
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


def process_one_account(driver, email, password, known_totp_secret=None):
    """
    Complete workflow for one Google Workspace account:
    1. Login
    2. Handle post-login pages
    3. Navigate to security
    4. Setup authenticator (extract secret, save to SFTP)
    5. Enable 2-step verification
    6. Generate app password
    7. Save app password to S3
    
    Returns:
      (success, step_completed, error_code, error_message, secret_key, app_password, s3_bucket, s3_key, timings)
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

    # 1b. HANDLE POST-LOGIN PAGES (Speedbump, verification, etc.)
    step = "post_login_navigation"
    step_start = time.time()
    ok, err_type, err_msg = handle_post_login_pages(driver, max_attempts=20)
    timings["post_login_navigation"] = time.time() - step_start
    if not ok:
        return False, step, err_type, err_msg, secret_key, app_password, s3_bucket, s3_key, timings
    
    logger.info("[STEP] Successfully reached myaccount.google.com, proceeding to security setup")

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
      timings         = dict of timing information
    """
    logger.info("=" * 60)
    logger.info("[LAMBDA] Handler invoked")
    logger.info(f"[LAMBDA] Event type: {type(event)}")
    logger.info(f"[LAMBDA] Event content: {event}")
    logger.info(f"[LAMBDA] Context: {context}")
    logger.info("=" * 60)

    # Parse input
    email = event.get("email") or os.environ.get("GW_EMAIL")
    password = event.get("password") or os.environ.get("GW_PASSWORD")
    known_totp_secret = event.get("known_totp_secret") or os.environ.get("KNOWN_TOTP_SECRET")

    if not email or not password:
        return {
            "status": "failed",
            "error_step": "init",
            "error_message": "Missing email or password in event/environment",
            "step_completed": "init"
        }

    driver = None
    try:
        # Initialize Chrome driver
        driver = get_chrome_driver()
        logger.info(f"[LAMBDA] Chrome driver started for {email}")

        # Process account
        success, step, err_type, err_msg, secret_key, app_password, s3_bucket, s3_key, timings = process_one_account(
            driver, email, password, known_totp_secret
        )

        # Clean response
        response = {
            "status": "ok" if success else "failed",
            "email": email,
            "step_completed": step,
            "timings": timings
        }

        if not success:
            response["error_step"] = step
            response["error_message"] = err_msg or "Unknown error"
        else:
            response["app_password"] = app_password
            response["secret_key"] = f"{secret_key[:4]}****" if secret_key else None
            response["app_passwords_s3_bucket"] = s3_bucket
            response["app_passwords_s3_key"] = s3_key

        return response

    except Exception as e:
        logger.error(f"[LAMBDA] Unhandled exception: {e}")
        logger.error(traceback.format_exc())
        return {
            "status": "failed",
            "error_step": "init",
            "error_message": str(e),
            "step_completed": "init"
        }
    finally:
        if driver:
            try:
                driver.quit()
                logger.info("[LAMBDA] Chrome driver closed")
            except:
                pass
