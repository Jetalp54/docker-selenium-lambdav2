"""
AWS Lambda Handler: Google Workspace Automation
- Logs into Google account
- Sets up 2-Step Verification with Authenticator
- Extracts TOTP secret and saves to SFTP
- Creates App Password
- Saves App Password to DynamoDB (reliable, atomic storage)

Usage:
Event must contain: {"email": "...", "password": "..."}
"""

import os
import re
import json
import time
import base64
import random
import string
import logging
import traceback
import subprocess

# 3rd-party libraries
import boto3
import paramiko
import pyotp

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =====================================================================
# Global boto3 clients/resources (reused across invocations for better performance)
# =====================================================================

# Lazy initialization of boto3 clients/resources
_dynamodb_resource = None
_s3_client = None

def get_dynamodb_resource():
    """Get or create DynamoDB resource (reused across invocations)"""
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    return _dynamodb_resource

def get_s3_client():
    """Get or create S3 client (reused across invocations)"""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client

# =====================================================================
# Chrome Driver Initialization for AWS Lambda (with anti-detection)
# =====================================================================

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
    
    # Log /opt contents for debugging
    if os.path.exists('/opt'):
        logger.info(f"[LAMBDA] Contents of /opt: {os.listdir('/opt')}")
        if os.path.exists('/opt/chrome'):
            logger.info(f"[LAMBDA] Contents of /opt/chrome: {os.listdir('/opt/chrome')}")
    
    # Common paths for Chrome binary
    chrome_paths = [
        '/opt/chrome/chrome',
        '/opt/chrome/headless-chromium',
        '/opt/chrome/chrome-wrapper',
        '/usr/bin/chromium',
        '/usr/bin/chromium-browser',
        '/usr/bin/google-chrome',
    ]
    
    for path in chrome_paths:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            chrome_binary = path
            logger.info(f"[LAMBDA] Found Chrome binary at: {chrome_binary}")
            break
    
    # If not found by direct paths, try using 'which'
    if not chrome_binary:
        try:
            result = subprocess.run(['which', 'chrome'], capture_output=True, text=True)
            if result.returncode == 0:
                chrome_binary = result.stdout.strip()
                logger.info(f"[LAMBDA] Found Chrome via which: {chrome_binary}")
        except Exception as e:
            logger.debug(f"[LAMBDA] 'which chrome' failed: {e}")
    
    if not chrome_binary:
        logger.error("[LAMBDA] Chrome binary not found! Cannot proceed without Chrome binary path.")
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
        try:
            result = subprocess.run(['which', 'chromedriver'], capture_output=True, text=True)
            if result.returncode == 0:
                chromedriver_path = result.stdout.strip()
                logger.info(f"[LAMBDA] Found ChromeDriver via which: {chromedriver_path}")
        except Exception as e:
            logger.debug(f"[LAMBDA] 'which chromedriver' failed: {e}")
    
    if not chromedriver_path:
        logger.error("[LAMBDA] ChromeDriver not found! This should not happen with umihico base image.")
        raise Exception("ChromeDriver not found in Lambda environment")

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
# Selenium Helper Functions
# =====================================================================

def wait_for_xpath(driver, xpath, timeout=30):
    """Wait for an element by XPath and return it."""
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        return element
    except TimeoutException:
        logger.error(f"[SELENIUM] Timeout waiting for XPath: {xpath}")
        return None

def wait_for_clickable_xpath(driver, xpath, timeout=30):
    """Wait for an element to be clickable and return it."""
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        return element
    except TimeoutException:
        logger.error(f"[SELENIUM] Timeout waiting for clickable XPath: {xpath}")
        return None

def click_xpath(driver, xpath, timeout=30):
    """Click an element by XPath."""
    element = wait_for_clickable_xpath(driver, xpath, timeout=timeout)
    if element:
        element.click()
        return True
    return False

def element_exists(driver, xpath, timeout=10):
    """Check if an element exists without throwing exception."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        return True
    except TimeoutException:
        return False

def find_element_with_fallback(driver, xpath_list, timeout=30, description="element"):
    """Try multiple XPaths and return the first found element."""
    for xpath in xpath_list:
        try:
            element = wait_for_xpath(driver, xpath, timeout=timeout)
            if element:
                logger.info(f"[STEP] Found {description} using xpath: {xpath}")
                return element
        except:
            continue
    logger.error(f"[STEP] Could not find {description} with any of the provided xpaths")
    return None


# =====================================================================
# SFTP upload for TOTP secrets
# =====================================================================

def upload_secret_to_sftp(email, secret_key):
    """
    Upload the TOTP secret key to SFTP server.
    Environment vars:
      SECRET_SFTP_HOST         (required)
      SECRET_SFTP_USER         (required)
      SECRET_SFTP_PASSWORD     (required)
      SECRET_SFTP_PORT         (optional, default 22)
      SECRET_SFTP_REMOTE_DIR   (optional, default /root/gw_secrets)
    """
    host = os.environ.get("SECRET_SFTP_HOST", "46.224.9.127")
    port = int(os.environ.get("SECRET_SFTP_PORT", "22"))
    user = os.environ.get("SECRET_SFTP_USER")
    password = os.environ.get("SECRET_SFTP_PASSWORD")
    remote_dir = os.environ.get("SECRET_SFTP_REMOTE_DIR", "/home/brightmindscampus/")

    if not all([host, user, password]):
        logger.error("[SFTP] Missing SFTP credentials in environment.")
        return None, None

    # Extract alias from email (part before @)
    alias = email.split("@")[0] if "@" in email else email
    
    try:
        transport = paramiko.Transport((host, port))
        # Set short timeouts to fail fast if blocked
        transport.banner_timeout = 5
        transport.auth_timeout = 5
        transport.connect(username=user, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Create remote directory if it doesn't exist
        try:
            sftp.chdir(remote_dir)
        except IOError:
            try:
                sftp.mkdir(remote_dir)
                sftp.chdir(remote_dir)
            except Exception as mkdir_err:
                logger.warning(f"[SFTP] Could not create/chdir to {remote_dir}: {mkdir_err}")

        # Create alias folder (from reference script structure)
        alias_dir = f"{remote_dir.rstrip('/')}/{alias}"
        try:
            sftp.mkdir(alias_dir)
        except IOError:
            pass  # Directory probably exists
            
        # Define filename (matching reference script format)
        filename = f"{email}_authenticator_secret_key.txt"
        remote_path = f"{alias_dir}/{filename}"

        # Write secret to file
        with sftp.open(remote_path, 'w') as f:
            f.write(secret_key)
        
        logger.info(f"[SFTP] Secret uploaded to {host}:{remote_path}")
        sftp.close()
        transport.close()
        
        return host, remote_path

    except Exception as e:
        logger.error(f"[SFTP] Failed to upload secret: {e}")
        # Do NOT log full traceback for timeouts to keep logs clean
        return None, None


# =====================================================================
# S3 upload for App Passwords (REMOVED)
# =====================================================================
# Function append_app_password_to_s3 removed to prevent race conditions.
# We now use DynamoDB for reliable, atomic storage.



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
            
            # Handle Speedbump page (especially gaplustos - Google Terms of Service)
            if "speedbump" in current_url:
                logger.info(f"[STEP] Speedbump page detected: {current_url}")
                
                # Check if it's the gaplustos page specifically
                if "speedbump/gaplustos" in current_url:
                    logger.info("[STEP] Google+ TOS speedbump detected, using JavaScript click...")
                    try:
                        # Use JavaScript to click the confirm button (more reliable)
                        driver.execute_script("document.querySelector('#confirm').click()")
                        logger.info("[STEP] Clicked #confirm button via JavaScript")
                        time.sleep(2)
                        continue  # Go to next iteration
                    except Exception as e:
                        logger.warning(f"[STEP] JavaScript click failed: {e}")
                
                # Generic speedbump or fallback handling
                logger.info("[STEP] Attempting to click speedbump/confirmation buttons...")
                
                # Try multiple button selectors for Continue/Next/Confirm
                continue_button_xpaths = [
                    "//button[@id='confirm']",
                    "//button[contains(., 'Continue')]",
                    "//button[contains(., 'Next')]",
                    "//button[contains(., 'I agree')]",
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
                    "//button[contains(., 'Not now')]",
                    "//span[contains(text(), 'Continue')]/ancestor::button",
                    "//span[contains(text(), 'Next')]/ancestor::button",
                ]
                
                for xpath in verify_button_xpaths:
                    try:
                        if element_exists(driver, xpath, timeout=2):
                            click_xpath(driver, xpath, timeout=5)
                            logger.info(f"[STEP] Clicked button on verification page: {xpath}")
                            time.sleep(2)
                            break
                    except Exception as e:
                        logger.debug(f"[STEP] Could not click verification button with xpath {xpath}: {e}")
                        continue
                
                continue
            
            # Handle "Review your account info" or similar pages
            if element_exists(driver, "//h1[contains(., 'Review')]", timeout=2):
                logger.info("[STEP] Review page detected")
                
                review_button_xpaths = [
                    "//button[contains(., 'Done')]",
                    "//button[contains(., 'Continue')]",
                    "//button[contains(., 'I agree')]",
                    "//span[contains(text(), 'Done')]/ancestor::button",
                ]
                
                for xpath in review_button_xpaths:
                    try:
                        if element_exists(driver, xpath, timeout=2):
                            click_xpath(driver, xpath, timeout=5)
                            logger.info(f"[STEP] Clicked button on review page: {xpath}")
                            time.sleep(2)
                            break
                    except Exception as e:
                        logger.debug(f"[STEP] Could not click review button with xpath {xpath}: {e}")
                        continue
                
                continue
            
            # Generic prompt handling - look for any Continue/Next/Done/Skip buttons
            generic_button_xpaths = [
                "//button[contains(., 'Continue')]",
                "//button[contains(., 'Next')]",
                "//button[contains(., 'Done')]",
                "//button[contains(., 'Skip')]",
                "//button[contains(., 'Not now')]",
                "//button[contains(., 'I agree')]",
            ]
            
            for xpath in generic_button_xpaths:
                try:
                    if element_exists(driver, xpath, timeout=2):
                        click_xpath(driver, xpath, timeout=5)
                        logger.info(f"[STEP] Clicked generic button: {xpath}")
                        time.sleep(2)
                        break  # Found and clicked a button, check new page
                except Exception as e:
                    logger.debug(f"[STEP] Could not click generic button with xpath {xpath}: {e}")
                    continue
            
            # If we're still not at myaccount after trying all buttons, try direct navigation
            if attempt >= max_attempts - 3:  # Last 3 attempts
                logger.warning(f"[STEP] Stuck on intermediate page, attempting direct navigation (attempt {attempt + 1})")
                try:
                    driver.get("https://myaccount.google.com/")
                    time.sleep(3)
                except Exception as e:
                    logger.error(f"[STEP] Direct navigation failed: {e}")
        
        except Exception as e:
            logger.error(f"[STEP] Error handling post-login pages: {e}")
            logger.error(traceback.format_exc())
    
    # If we've exhausted all attempts
    current_url = driver.current_url
    logger.error(f"[STEP] Failed to reach myaccount.google.com after {max_attempts} attempts. Last URL: {current_url}")
    return False, "POST_LOGIN_TIMEOUT", f"Could not bypass intermediate pages. Last URL: {current_url}"


def login_google(driver, email, password, known_totp_secret=None):
    """
    Login to Google. If a 2FA code is requested and we know a TOTP secret,
    we will try to solve it; otherwise we fail with an explicit error.
    
    Enhanced to handle challenge/pwd and other intermediate pages.
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

        # Wait for potential challenge pages, intermediate pages, or account home
        # Google may show: speedbump, verification, phone prompt, TOTP, recovery email, etc.
        # We'll wait longer and handle what we can, skip what we can't
        max_wait_attempts = 30  # Increased from 15 to 30 (90 seconds total)
        wait_interval = 3
        current_url = None
        
        for attempt in range(max_wait_attempts):
            time.sleep(wait_interval)
            try:
                current_url = driver.current_url
                logger.info(f"[STEP] Post-login check {attempt + 1}/{max_wait_attempts}: URL = {current_url}")
            except Exception as e:
                logger.error(f"[STEP] Failed to get current URL: {e}")
                return False, "driver_crashed", f"Driver crashed while checking URL: {e}"
            
            # Check for account verification/ID verification required
            if "speedbump/idvreenable" in current_url or "idvreenable" in current_url:
                logger.error("[STEP] ID verification required - manual intervention needed")
                return False, "ID_VERIFICATION_REQUIRED", "Manual ID verification required"
            
            # Success conditions - we're logged in
            if any(domain in current_url for domain in ["myaccount.google.com", "mail.google.com", "accounts.google.com/b/0", "accounts.google.com/servicelogin"]):
                logger.info("[STEP] Login success - reached account page")
                return True, None, None
            
            # Handle speedbump/gaplustos page (Google Terms of Service)
            if "speedbump" in current_url:
                logger.info(f"[STEP] Speedbump page detected: {current_url}")
                
                # Check if it's the gaplustos page specifically
                if "speedbump/gaplustos" in current_url:
                    logger.info("[STEP] Google+ TOS speedbump detected, clicking confirm with JavaScript...")
                    try:
                        # Use JavaScript to click the confirm button (more reliable for this page)
                        driver.execute_script("document.querySelector('#confirm').click()")
                        logger.info("[STEP] Clicked #confirm button via JavaScript")
                        time.sleep(2)
                    except Exception as e:
                        logger.warning(f"[STEP] JavaScript click failed, trying XPath: {e}")
                        # Fallback to XPath click
                        try:
                            if element_exists(driver, "//button[@id='confirm']", timeout=2):
                                click_xpath(driver, "//button[@id='confirm']", timeout=5)
                                logger.info("[STEP] Clicked #confirm button via XPath")
                                time.sleep(2)
                        except Exception as e2:
                            logger.warning(f"[STEP] XPath click also failed: {e2}")
                else:
                    # Generic speedbump handling
                    logger.info("[STEP] Generic speedbump page, attempting to continue...")
                    try:
                        # Try to click continue/confirm button
                        speedbump_xpaths = [
                            "//button[@id='confirm']",
                            "//button[contains(., 'Continue')]",
                            "//button[contains(., 'Next')]",
                            "//button[contains(., 'I agree')]",
                            "//div[@role='button' and contains(., 'Continue')]",
                        ]
                        for xpath in speedbump_xpaths:
                            if element_exists(driver, xpath, timeout=2):
                                click_xpath(driver, xpath, timeout=5)
                                logger.info(f"[STEP] Clicked speedbump button: {xpath}")
                                time.sleep(2)
                                break
                    except Exception as e:
                        logger.warning(f"[STEP] Could not click speedbump button: {e}")
                continue
            
            # Handle 2SV required page
            if "twosvrequired" in current_url:
                logger.info("[STEP] Two-step verification required page detected, navigating to setup...")
                try:
                    driver.get("https://myaccount.google.com/two-step-verification/authenticator?hl=en")
                    time.sleep(2)
                except Exception as e:
                    logger.warning(f"[STEP] Could not navigate from twosvrequired: {e}")
                continue
            
            # Handle challenge pages (TOTP, phone, recovery, etc.)
            if "challenge" in current_url or "signin/challenge" in current_url:
                logger.info(f"[STEP] Challenge page detected: {current_url}")
                
                # Check if it's challenge/pwd - this usually means additional verification
                if "challenge/pwd" in current_url:
                    logger.info("[STEP] Password challenge page detected - looking for continue buttons...")
                    
                    # Try to find and click any continue/next/skip buttons
                    continue_xpaths = [
                        "//button[contains(., 'Continue')]",
                        "//button[contains(., 'Next')]",
                        "//button[contains(., 'Skip')]",
                        "//button[contains(., 'Not now')]",
                        "//button[contains(., 'Done')]",
                        "//span[contains(text(), 'Continue')]/ancestor::button",
                        "//span[contains(text(), 'Next')]/ancestor::button",
                        "//span[contains(text(), 'Skip')]/ancestor::button",
                        "//div[@role='button' and contains(., 'Continue')]",
                        "//div[@role='button' and contains(., 'Next')]",
                    ]
                    
                    clicked = False
                    for xpath in continue_xpaths:
                        try:
                            if element_exists(driver, xpath, timeout=2):
                                click_xpath(driver, xpath, timeout=5)
                                logger.info(f"[STEP] Clicked button on challenge/pwd page: {xpath}")
                                clicked = True
                                time.sleep(2)
                                break
                        except Exception as e:
                            logger.debug(f"[STEP] Could not click xpath {xpath}: {e}")
                            continue
                    
                    if clicked:
                        continue  # Go to next iteration to check new page
                    else:
                        # No button found - try to navigate directly to myaccount
                        logger.info("[STEP] No actionable button found on challenge/pwd, attempting direct navigation...")
                        try:
                            driver.get("https://myaccount.google.com/")
                            time.sleep(3)
                            continue
                        except Exception as e:
                            logger.warning(f"[STEP] Direct navigation failed: {e}")
                
                # Check if it's a TOTP challenge (we can handle this)
                if "challenge/totp" in current_url:
                    logger.info("[STEP] TOTP challenge detected")
                    
                    # Check for OTP input field
                    otp_input_xpaths = [
                        "//input[@type='tel']",
                        "//input[@autocomplete='one-time-code']",
                        "//input[@type='text' and contains(@aria-label, 'code')]",
                        "//input[contains(@aria-label, 'Code')]",
                    ]
                    
                    otp_input = None
                    for xpath in otp_input_xpaths:
                        try:
                            otp_input = wait_for_xpath(driver, xpath, timeout=5)
                            if otp_input:
                                break
                        except:
                            continue
                    
                    if otp_input:
                        if not known_totp_secret:
                            logger.error("[STEP] 2FA is required but no TOTP secret is available")
                            return False, "2FA_REQUIRED", "2FA required but secret is unknown"
                        
                        # Generate and submit TOTP code with retries
                        for retry in range(3):
                            try:
                                # Generate fresh TOTP code
                                clean_secret = known_totp_secret.replace(" ", "").upper()
                                totp = pyotp.TOTP(clean_secret)
                                otp_code = totp.now()
                                logger.info(f"[STEP] Generated TOTP code (attempt {retry + 1}): {otp_code}")
                                
                                # Clear and enter OTP
                                driver.execute_script("arguments[0].value = '';", otp_input)
                                driver.execute_script("arguments[0].value = arguments[1];", otp_input, otp_code)
                                logger.info(f"[STEP] OTP code entered (attempt {retry + 1})")
                                
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
                                
                                # Check if we left the TOTP page
                                if "challenge/totp" not in current_url:
                                    logger.info("[STEP] OTP verified successfully")
                                    break
                                
                                # Still on TOTP page - generate new code for retry
                                if retry < 2:
                                    logger.warning(f"[STEP] Still on TOTP page, retrying with new code...")
                                    time.sleep(3)  # Wait for new time window
                                else:
                                    logger.error("[STEP] OTP verification failed after 3 attempts")
                                    return False, "OTP_REJECTED", "OTP code was rejected by Google"
                            
                            except Exception as otp_e:
                                logger.error(f"[STEP] OTP submission error (attempt {retry + 1}): {otp_e}")
                                if retry == 2:
                                    return False, "OTP_SUBMISSION_ERROR", str(otp_e)
                        
                        # After TOTP success, continue waiting loop
                        continue
                    else:
                        logger.warning("[STEP] On challenge/totp page but no OTP input found")
                
                # Other challenge types - log and continue waiting
                logger.info(f"[STEP] Unhandled challenge type: {current_url}, waiting to see if it auto-resolves...")
                continue
            
            # If we're here, not on any recognized page yet - keep waiting
            if attempt < max_wait_attempts - 1:
                logger.info(f"[STEP] Still waiting for login to complete... ({attempt + 1}/{max_wait_attempts})")
        
        # If we've exhausted all attempts and not logged in, fail
        logger.error(f"[STEP] Login failed - did not reach myaccount.google.com after {max_wait_attempts} attempts")
        logger.error(f"[STEP] Final URL: {current_url}")
        return False, "LOGIN_TIMEOUT", f"Login timed out after {max_wait_attempts * wait_interval} seconds. Last URL: {current_url}"

    except Exception as e:
        logger.error(f"[STEP] Login exception: {e}")
        logger.error(traceback.format_exc())
        return False, "LOGIN_EXCEPTION", str(e)


# =====================================================================
# Step 2: Setup Authenticator (extract TOTP secret)
# =====================================================================


def setup_authenticator(driver, email):
    """
    Navigate to the authenticator setup page and extract the secret key.
    Based on reference script G_Ussers_No_Timing.py
    Returns (success: bool, secret_key: str|None, error_code: str|None, error_message: str|None)
    """
    logger.info(f"[STEP] Setting up Authenticator for {email}")
    
    try:
        # Navigate to authenticator setup page
        logger.info("[STEP] Navigating to Authenticator setup page...")
        driver.get("https://myaccount.google.com/two-step-verification/authenticator?hl=en")
        time.sleep(3)
        
        # Step 1: Click "Set up authenticator" button
        # Try multiple XPath patterns for the setup button
        logger.info("[STEP] Looking for 'Set up authenticator' button...")
        setup_button_xpaths = [
            "/html/body/c-wiz/div/div[2]/div[2]/c-wiz/div/div/div[3]/div[2]/div/div/div/button/span[5]",
            "/html/body/c-wiz/div/div[2]/div[3]/c-wiz/div/div/div[3]/div[2]/div/div/div/button",
            "/html/body/c-wiz/div/div[2]/div[2]/c-wiz/div/div/div[3]/div[2]/div/div/div/button",
            "//button[contains(., 'Set up') or contains(., 'SET UP')]",
            "//span[contains(text(), 'Set up')]/ancestor::button",
            "//button[contains(., 'Get started') or contains(., 'GET STARTED')]",
        ]
        
        setup_clicked = False
        for xpath in setup_button_xpaths:
            try:
                if element_exists(driver, xpath, timeout=3):
                    # Use JavaScript click for better reliability
                    element = wait_for_xpath(driver, xpath, timeout=3)
                    if element:
                        driver.execute_script("arguments[0].click();", element)
                        logger.info(f"[STEP] Clicked 'Set up authenticator' button using: {xpath}")
                        time.sleep(2)
                        setup_clicked = True
                        break
            except Exception as e:
                logger.debug(f"[STEP] Could not click setup button with xpath {xpath}: {e}")
                continue
        
        if not setup_clicked:
            logger.warning("[STEP] Could not find 'Set up authenticator' button, continuing anyway...")
        
        # Step 2: Click "Can't scan it?" link to show text version
        logger.info("[STEP] Looking for 'Can't scan it?' link...")
        
        # Build comprehensive list of XPath patterns
        cant_scan_xpaths = [
            "//span[contains(text(), 'Can't scan it?')]",
            "//a[contains(text(), 'Can't scan it?')]",
            "//button[contains(text(), 'Can't scan it?')]",
            "//*[contains(text(), 'Can't scan it?')]",
            "//span[contains(text(), 'Can\\'t scan it?')]",
            "//*[contains(text(), 'Can\\'t scan it?')]",
            "//span[contains(text(), 'cant scan')]",
            "//*[contains(text(), 'cant scan')]",
        ]
        
        # Add dynamic div paths
        for div_index in range(9, 14):
            cant_scan_xpaths.extend([
                f"/html/body/div[{div_index}]/div/div[2]/span/div/div/div/div[2]/center/div/div/button/span[5]",
                f"/html/body/div[{div_index}]/div/div[2]/span/div/div/div/div[2]/center/div/div/button/span[4]",
                f"/html/body/div[{div_index}]/div/div[2]/span/div/div/div/div[2]/center/div/div/button/span[3]",
                f"/html/body/div[{div_index}]/div/div[2]/span/div/div/div/div[2]/center/div/div/button",
            ])
        
        # Add class-based patterns
        cant_scan_xpaths.extend([
            "//button[contains(@class, 'VfPpkd-LgbsSe')]//span[contains(text(), 'Can')]",
            "//button[contains(@class, 'VfPpkd-LgbsSe')]//span[contains(text(), 'scan')]",
        ])
        
        cant_scan_clicked = False
        for xpath in cant_scan_xpaths:
            try:
                element = wait_for_xpath(driver, xpath, timeout=2)
                if element:
                    # Try JavaScript click first
                    try:
                        driver.execute_script("arguments[0].click();", element)
                        logger.info(f"[STEP] Clicked 'Can't scan it?' link using JavaScript: {xpath}")
                        time.sleep(2)
                        cant_scan_clicked = True
                        break
                    except:
                        # Fallback to regular click
                        element.click()
                        logger.info(f"[STEP] Clicked 'Can't scan it?' link using regular click: {xpath}")
                        time.sleep(2)
                        cant_scan_clicked = True
                        break
            except:
                continue
        
        if not cant_scan_clicked:
            logger.warning("[STEP] Could not find 'Can't scan it?' link")
        
        # Step 3: Extract the secret key
        # Use the EXACT XPath pattern from the reference script
        logger.info("[STEP] Extracting secret key...")
        secret_key = None
        
        # Try the reference script's exact pattern first (most reliable)
        for div_index in range(9, 14):
            try:
                # Reference script's exact XPath
                xpath = f"/html/body/div[{div_index}]/div/div[2]/span/div/div/ol/li[2]/div/strong"
                logger.debug(f"[STEP] Trying XPath: {xpath}")
                element = wait_for_xpath(driver, xpath, timeout=3)
                if element:
                    text = element.text.strip()
                    # Clean up the secret (remove spaces)
                    cleaned = text.replace(" ", "").upper()
                    if len(cleaned) >= 16:  # TOTP secrets are usually 16+ characters
                        secret_key = cleaned
                        logger.info(f"[STEP] Extracted secret key using div[{div_index}]: {secret_key[:4]}****{secret_key[-4:]}")
                        break
            except:
                continue
        
        # Fallback: Try alternative XPath patterns
        if not secret_key:
            logger.info("[STEP] Trying alternative secret key XPaths...")
            alternative_xpaths = [
                "//strong[string-length(normalize-space(text())) >= 16]",
                "//div[contains(@class, 'key')]//div[contains(@class, 'value')]",
                "//span[contains(@class, 'secret')]",
                "//code[string-length(normalize-space(text())) >= 16]",
                "//pre[string-length(normalize-space(text())) >= 16]",
            ]
            
            # Add more dynamic div patterns
            for div_index in range(9, 14):
                alternative_xpaths.extend([
                    f"/html/body/div[{div_index}]/div/div[2]/span/div/div/ol/li[2]/div",
                    f"/html/body/div[{div_index}]/div/div[2]/span/div/div/ol/li[2]",
                    f"/html/body/div[{div_index}]//strong",
                ])
            
            for xpath in alternative_xpaths:
                try:
                    element = wait_for_xpath(driver, xpath, timeout=2)
                    if element:
                        text = element.text.strip()
                        cleaned = text.replace(" ", "").upper()
                        if len(cleaned) >= 16 and cleaned.isalnum():
                            secret_key = cleaned
                            logger.info(f"[STEP] Extracted secret key using alternative XPath: {secret_key[:4]}****{secret_key[-4:]}")
                            break
                except:
                    continue
        
        if not secret_key:
            logger.error("[STEP] Could not extract secret key from authenticator setup page")
            return False, None, "SECRET_EXTRACTION_FAILED", "Failed to extract TOTP secret key"
        
        logger.info(f"[STEP] Secret key successfully extracted: {secret_key[:4]}****{secret_key[-4:]}")
        
        # Step 4: Click "Next" button to proceed to verification
        # Based on G_Ussers_No_Timing.py click_continue_button logic
        logger.info("[STEP] Clicking 'Next' button to proceed to verification...")
        next_clicked = False
        
        # Try dynamic div indices for the Next button
        for div_index in range(9, 14):
            try:
                # Reference script XPath for Next button
                xpath = f"/html/body/div[{div_index}]/div/div[2]/div[3]/div/div[2]/div[2]/button"
                if element_exists(driver, xpath, timeout=2):
                    element = wait_for_xpath(driver, xpath, timeout=2)
                    if element:
                        driver.execute_script("arguments[0].scrollIntoView(true);", element)
                        driver.execute_script("arguments[0].click();", element)
                        logger.info(f"[STEP] Clicked 'Next' button using div[{div_index}]")
                        time.sleep(2)
                        next_clicked = True
                        break
            except Exception as e:
                continue
        
        if not next_clicked:
            # Fallback generic Next buttons
            logger.info("[STEP] Trying generic Next button XPaths...")
            generic_next_xpaths = [
                "//button[contains(., 'Next')]",
                "//span[contains(text(), 'Next')]/ancestor::button",
                "//div[contains(text(), 'Next')]/ancestor::button"
            ]
            for xpath in generic_next_xpaths:
                try:
                    if element_exists(driver, xpath, timeout=2):
                        click_xpath(driver, xpath, timeout=5)
                        logger.info(f"[STEP] Clicked Next button: {xpath}")
                        time.sleep(2)
                        next_clicked = True
                        break
                except:
                    continue
            
        if not next_clicked:
            logger.warning("[STEP] Could not find/click 'Next' button. Verification might fail if we are not on the input screen.")

        return True, secret_key, None, None
    
    except Exception as e:
        logger.error(f"[STEP] Authenticator setup exception: {e}")
        logger.error(traceback.format_exc())
        return False, None, "AUTHENTICATOR_SETUP_EXCEPTION", str(e)


# =====================================================================
# Step 3: Enable 2-Step Verification
# =====================================================================


def verify_authenticator_setup(driver, email, secret_key):
    """
    Verify the Authenticator setup by entering the TOTP code.
    This happens on the modal after clicking "Next" in setup_authenticator.
    Returns (success: bool, error_code: str|None, error_message: str|None)
    """
    logger.info(f"[STEP] Verifying Authenticator setup for {email}")
    
    try:
        # Generate TOTP code from the secret we extracted
        totp = pyotp.TOTP(secret_key.replace(" ", ""))
        otp_code = totp.now()
        logger.info(f"[STEP] Generated TOTP code for verification: {otp_code}")
        
        # Find the OTP input field
        # Use comprehensive XPaths from reference script
        otp_input = None
        
        # Try dynamic div indices first (most specific)
        for div_index in range(9, 14):
            xpaths = [
                f"/html/body/div[{div_index}]/div/div[2]/span/div/div/div/div[2]/div/div/label/input",
                f"/html/body/div[{div_index}]/div/div[2]/span/div/div/div/div[2]/div/div/div[1]/span[2]/input"
            ]
            for xpath in xpaths:
                if element_exists(driver, xpath, timeout=1):
                    otp_input = wait_for_xpath(driver, xpath, timeout=1)
                    if otp_input:
                        logger.info(f"[STEP] Found OTP input using div[{div_index}]")
                        break
            if otp_input: break
        
        if not otp_input:
            logger.info("[STEP] Trying generic OTP input XPaths...")
            otp_input_xpaths = [
                "//input[@type='tel']",
                "//input[@autocomplete='one-time-code']",
                "//input[@type='text' and contains(@aria-label, 'code')]",
                "//input"
            ]
            for xpath in otp_input_xpaths:
                try:
                    otp_input = wait_for_xpath(driver, xpath, timeout=2)
                    if otp_input:
                        logger.info(f"[STEP] Found OTP input field: {xpath}")
                        break
                except:
                    continue
        
        if not otp_input:
            logger.error("[STEP] Could not find OTP input field for verification")
            return False, "OTP_INPUT_NOT_FOUND", "OTP input field not found"
        
        # Enter the TOTP code
        otp_input.clear()
        time.sleep(0.5)
        otp_input.send_keys(otp_code)
        logger.info("[STEP] Entered TOTP code")
        time.sleep(1)
        
        # Click Verify button
        verify_clicked = False
        
        # Try dynamic div indices for Verify button
        for div_index in range(9, 14):
            xpath = f"/html/body/div[{div_index}]/div/div[2]/div[3]/div/div[2]/div[2]/button"
            try:
                if element_exists(driver, xpath, timeout=1):
                    btn = wait_for_xpath(driver, xpath, timeout=1)
                    if btn:
                        driver.execute_script("arguments[0].click();", btn)
                        logger.info(f"[STEP] Clicked Verify button using div[{div_index}]")
                        verify_clicked = True
                        break
            except: continue

        if not verify_clicked:
            verify_button_xpaths = [
                "//button[contains(., 'Verify')]",
                "//span[contains(text(), 'Verify')]/ancestor::button",
                "//div[contains(text(), 'Verify')]/ancestor::button",
                "//button[contains(., 'Next')]",
            ]
            
            for xpath in verify_button_xpaths:
                if element_exists(driver, xpath, timeout=2):
                    click_xpath(driver, xpath, timeout=5)
                    logger.info(f"[STEP] Clicked Verify button: {xpath}")
                    verify_clicked = True
                    time.sleep(2)
                    break
        
        if not verify_clicked:
             # Try hitting Enter key on the input if button fails
            logger.warning("[STEP] Could not click Verify button, trying Enter key...")
            otp_input.send_keys(Keys.RETURN)
        
        time.sleep(3)
        logger.info("[STEP] Authenticator verified successfully")
        return True, None, None
    
    except Exception as e:
        logger.error(f"[STEP] Authenticator verification exception: {e}")
        logger.error(traceback.format_exc())
        return False, "AUTH_VERIFY_EXCEPTION", str(e)


def enable_two_step_verification(driver, email):
    """
    Enable Two-Step Verification for the given account.
    Based on reference script G_Ussers_No_Timing.py enable_two_step_verification function.
    Navigates to 2SV page, clicks Turn On, and skips phone number.
    """
    logger.info(f"[STEP] Navigating to 2-Step Verification page for {email}...")
    
    try:
        # Navigate to 2-Step Verification page (with hl=en for English)
        driver.get("https://myaccount.google.com/signinoptions/twosv?hl=en")
        time.sleep(3)
        
        # Check if 2-step verification is already enabled
        if element_exists(driver, "//button[contains(., 'Turn off')]", timeout=3):
            logger.info(f"[STEP] 2-Step Verification is already enabled for {email}")
            return True, None, None

        # Try the original xpath first (from reference script)
        turn_on_clicked = False
        try:
            turn_on_button = wait_for_clickable_xpath(driver, '/html/body/c-wiz/div/div[2]/div[2]/c-wiz/div/div[2]/div[4]/div/button/span[6]', timeout=5)
            if turn_on_button:
                driver.execute_script("arguments[0].click();", turn_on_button)
                logger.info(f"[STEP] Clicked on 'Turn On 2-Step Verification' using original xpath for {email}")
                turn_on_clicked = True
                time.sleep(2)
        except TimeoutException:
            logger.info("[STEP] Original 2-step verification xpath not found, trying fallback xpath...")
            
            # Fallback to the new xpath for updated accounts (from reference script)
            try:
                turn_on_button = wait_for_clickable_xpath(driver, '/html/body/c-wiz/div/div[2]/div[2]/c-wiz/div/div[2]/div[4]/div/button', timeout=5)
                if turn_on_button:
                    driver.execute_script("arguments[0].click();", turn_on_button)
                    logger.info(f"[STEP] Clicked on 'Turn On 2-Step Verification' using fallback xpath for {email}")
                    turn_on_clicked = True
                    time.sleep(2)
            except TimeoutException:
                logger.warning("[STEP] Both xpaths failed, trying generic patterns...")
                
                # Generic fallback patterns
                generic_xpaths = [
                    "//button[contains(., 'Turn on')]",
                    "//button[contains(., 'TURN ON')]",
                    "//span[contains(text(), 'Turn on')]/ancestor::button",
                ]
                
                for xpath in generic_xpaths:
                    if element_exists(driver, xpath, timeout=2):
                        try:
                            element = wait_for_clickable_xpath(driver, xpath, timeout=2)
                            driver.execute_script("arguments[0].click();", element)
                            logger.info(f"[STEP] Clicked 'Turn On' using generic xpath: {xpath}")
                            turn_on_clicked = True
                            time.sleep(2)
                            break
                        except:
                            continue

        # Handle skip phone number (from reference script handle_skip_phone_number)
        try:
            skip_link = wait_for_clickable_xpath(driver, '//button//span[contains(text(), "Skip")]', timeout=5)
            if skip_link:
                driver.execute_script("arguments[0].click();", skip_link)
                logger.info("[STEP] Clicked 'Skip' to bypass phone number setup.")
                time.sleep(2)
        except TimeoutException:
            logger.info("[STEP] No 'Skip' link found for phone number setup.")

        logger.info(f"[STEP] 2-Step Verification enabled successfully for {email}")
        return True, None, None

    except TimeoutException as e:
        logger.error(f"[STEP] Timeout while enabling 2-Step Verification for {email}: {e}")
        return False, "2SV_TIMEOUT", str(e)
    except Exception as e:
        logger.error(f"[STEP] Error during 2-Step Verification setup for {email}: {e}")
        logger.error(traceback.format_exc())
        return False, "2SV_EXCEPTION", str(e)


# =====================================================================
# Step 4: Generate App Password
# =====================================================================


def generate_app_password(driver, email):
    """
    Navigate to App Passwords page and generate a new app password.
    Based on reference script G_Ussers_No_Timing.py generate_app_password function.
    Returns (success: bool, app_password: str|None, error_code: str|None, error_message: str|None)
    """
    logger.info(f"[STEP] Generating App Password for {email}")
    
    try:
        # Wait up to 30 seconds after enabling 2SV for app password page to be ready
        logger.info("[STEP] Waiting for app password page to be ready (may take up to 30 seconds after enabling 2SV)...")
        
        # Navigate to app passwords page with hl=en for English
        driver.get("https://myaccount.google.com/apppasswords?hl=en")
        
        # Wait for page to be ready
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(2)  # Additional wait for dynamic content
            logger.info("[STEP] App passwords page loaded")
        except TimeoutException:
            logger.warning("[STEP] App passwords page load timeout, proceeding anyway...")
        
        max_retries = 3
        initial_timeout = 30
        
        for attempt in range(max_retries):
            try:
                # Comprehensive XPath variations for app name input (from reference script)
                app_name_xpath_variations = [
                    "/html/body/c-wiz/div/div[2]/div[3]/c-wiz/div/div[4]/div/div[3]/div/div[1]/div/div/div[1]/span[3]/input",
                    "/html/body/c-wiz/div/div[2]/div[2]/c-wiz/div/div[4]/div/div[3]/div/div[1]/div/div/label/input",
                    "/html/body/c-wiz/div/div[2]/div[2]/c-wiz/div/div[4]/div/div[3]/div/div[1]/div/div/div[1]/span[3]/input",
                    "//input[@aria-label='App name']",
                    "//input[contains(@placeholder, 'app') or contains(@placeholder, 'name')]",
                    "//input[@type='text' and contains(@class, 'input')]",
                    "//input[@type='text']",
                    "//label[contains(text(), 'App name')]/following::input",
                    "//div[contains(@class, 'app')]//input[@type='text']",
                    "//form//input[@type='text'][1]",
                    "//c-wiz//input[@type='text']"
                ]
                
                app_name_field = None
                for xpath in app_name_xpath_variations:
                    try:
                        element = wait_for_xpath(driver, xpath, timeout=5)
                        if element:
                            # Check if element is interactable
                            try:
                                # Try to scroll into view and check if visible
                                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                                time.sleep(0.5)
                                if element.is_displayed() and element.is_enabled():
                                    app_name_field = element
                                    logger.info(f"[STEP] Found app name input field: {xpath}")
                                    break
                            except:
                                continue
                    except:
                        continue
                
                if not app_name_field:
                    logger.warning(f"[STEP] App name input field not detected on attempt {attempt + 1}, refreshing page...")
                    driver.refresh()
                    time.sleep(3)
                    if attempt < max_retries - 1:
                        continue
                    else:
                        raise TimeoutException("Failed to locate app name input field after retries")
                
                # Generate random app name (matching reference script format)
                app_name = f"App-{int(time.time())}"
                logger.info(f"[STEP] Generated app name: {app_name}")
                
                # Clear and enter app name using JavaScript if regular methods fail
                try:
                    app_name_field.clear()
                    app_name_field.send_keys(app_name)
                    logger.info(f"[STEP] Entered app name using regular method")
                except Exception as clear_err:
                    # Fallback to JavaScript if element not interactable
                    logger.warning(f"[STEP] Regular input failed, using JavaScript: {clear_err}")
                    driver.execute_script("arguments[0].value = '';", app_name_field)
                    driver.execute_script("arguments[0].value = arguments[1];", app_name_field, app_name)
                    # Trigger input event
                    driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", app_name_field)
                    logger.info(f"[STEP] Entered app name using JavaScript")
                
                time.sleep(1)
                
                # Click Generate button with comprehensive XPaths (from reference script)
                generate_button_xpath_variations = [
                    "/html/body/c-wiz[1]/div/div[2]/div[3]/c-wiz/div/div[4]/div/div[3]/div/div[2]/div/div/div/button",
                    "/html/body/c-wiz/div/div[2]/div[2]/c-wiz/div/div[4]/div/div[3]/div/div[2]/div/div/div/button/span[5]",
                    "/html/body/c-wiz/div/div[2]/div[2]/c-wiz/div/div[4]/div/div[3]/div/div[2]/div/div/div/button/span[2]",
                    "//button[contains(., 'Generate')]",
                    "//button[contains(@aria-label, 'Generate')]",
                    "//button[@type='button' and contains(text(), 'Generate')]",
                    "//span[contains(text(), 'Generate')]/parent::button",
                    "//div[contains(@class, 'generate')]//button",
                    "//button[contains(@class, 'generate')]",
                    "//form//button[@type='button']",
                    "//c-wiz//button[not(contains(@aria-label, 'Close'))]"
                ]
                
                generate_clicked = False
                for xpath in generate_button_xpath_variations:
                    try:
                        if element_exists(driver, xpath, timeout=3):
                            element = wait_for_clickable_xpath(driver, xpath, timeout=5)
                            if element:
                                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                                driver.execute_script("arguments[0].click();", element)
                                logger.info(f"[STEP] Clicked Generate button: {xpath}")
                                generate_clicked = True
                                time.sleep(2)
                                break
                    except:
                        continue
                
                if not generate_clicked:
                    raise TimeoutException("Failed to click Generate button")
                
                # Wait for app password dialog to appear (from reference script)
                logger.info("[STEP] Waiting for app password dialog to appear...")
                dialog_appeared = False
                dialog_selectors = [
                    "//div[@aria-modal='true']",
                    "//div[@role='dialog']",
                    "//div[@class='uW2Fw-P5QLlc']",
                    "//span[contains(text(), 'Generated app password')]",
                    "//h2[contains(., 'Generated app password')]"
                ]
                
                for selector in dialog_selectors:
                    try:
                        WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.XPATH, selector))
                        )
                        logger.info(f"[STEP] App password dialog detected: {selector}")
                        dialog_appeared = True
                        break
                    except TimeoutException:
                        continue
                
                if not dialog_appeared:
                    logger.error("[STEP] App password dialog did not appear after clicking Generate")
                    if attempt < max_retries - 1:
                        driver.refresh()
                        time.sleep(3)
                        continue
                    else:
                        raise TimeoutException("App password dialog did not appear")
                
                # Extract app password from spans first (from reference script extract_app_password_from_spans)
                logger.info("[STEP] Attempting to extract password from span elements...")
                app_password = None
                
                span_container_xpaths = [
                    "//strong[@class='v2CTKd KaSAf']//div[@dir='ltr']",
                    "//strong[@class='v2CTKd KaSAf']//div",
                    "//div[@class='lY6Rwe riHXqb']//strong//div",
                    "//h2[@class='XfTrZ']//strong//div",
                    "//article//strong//div[@dir='ltr']"
                ]
                
                for xpath in span_container_xpaths:
                    try:
                        container = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, xpath))
                        )
                        spans = container.find_elements(By.TAG_NAME, "span")
                        if spans:
                            password_chars = []
                            for span in spans:
                                char = span.text.strip()
                                if char:
                                    password_chars.append(char)
                            
                            if password_chars:
                                full_password = ''.join(password_chars)
                                clean_password = full_password.replace(' ', '')
                                
                                # Reconstruct dashes if needed
                                if len(clean_password) >= 16 and '-' not in clean_password:
                                    if len(clean_password) == 16:
                                        clean_password = f"{clean_password[:4]}-{clean_password[4:8]}-{clean_password[8:12]}-{clean_password[12:16]}"
                                
                                if len(clean_password) >= 16 and (clean_password.count('-') >= 3 or len(clean_password) == 19):
                                    app_password = clean_password
                                    logger.info(f"[STEP] Extracted app password from spans: {app_password[:4]}****{app_password[-4:]}")
                                    break
                    except:
                        continue
                
                # Fallback to dynamic XPath patterns if span extraction failed (from reference script)
                if not app_password:
                    logger.info("[STEP] Span extraction failed, trying dynamic XPath patterns...")
                    priority_xpaths = [
                        "//strong[@class='v2CTKd KaSAf']//div[@dir='ltr']",
                        "//strong[@class='v2CTKd KaSAf']//div",
                        "//strong[@class='v2CTKd KaSAf']",
                        "//div[@class='lY6Rwe riHXqb']//strong",
                        "//h2[@class='XfTrZ']//strong",
                        "//header[@class='VuF2Pd lY6Rwe']//strong",
                        "//article//strong[@class='v2CTKd KaSAf']",
                    ]
                    
                    # Add dynamic div patterns (from reference script)
                    for div_num in range(14, 23):
                        priority_xpaths.extend([
                            f"/html/body/div[{div_num}]/div[2]/div/div[1]/div/div[1]/article/header/div/h2/div/strong/div",
                            f"/html/body/div[{div_num}]/div[2]/div/div[1]/div/div[1]/article/header/div/h2/div/strong",
                            f"/html/body/div[{div_num}]/div[2]/div/div[1]/div/div[1]/article/header/div/h2/div",
                            f"/html/body/div[{div_num}]//strong[contains(text(), '-')]",
                        ])
                    
                    for i, xpath in enumerate(priority_xpaths):
                        try:
                            element = WebDriverWait(driver, 2).until(
                                EC.presence_of_element_located((By.XPATH, xpath))
                            )
                            potential_password = element.text.strip().replace(" ", "")
                            if len(potential_password) >= 16 and '-' in potential_password and potential_password.count('-') >= 3:
                                app_password = potential_password
                                logger.info(f"[STEP] App password found using XPath #{i+1}: {app_password[:4]}****{app_password[-4:]}")
                                break
                        except:
                            continue
                
                if not app_password or len(app_password) < 16:
                    raise TimeoutException("Failed to locate valid app password element")
                
                logger.info("[STEP] App Password generated successfully")
                return True, app_password, None, None
                
            except TimeoutException as e:
                logger.warning(f"[STEP] Attempt {attempt + 1} failed to generate app password: {e}")
                if attempt < max_retries - 1:
                    driver.refresh()
                    time.sleep(3)
                else:
                    raise e
        
        logger.error("[STEP] App Password generation failed after all retries")
        return False, None, "APP_PASSWORD_GENERATION_FAILED", "Failed to generate app password after retries"
    
    except Exception as e:
        logger.error(f"[STEP] App Password generation exception: {e}")
        logger.error(traceback.format_exc())
        return False, None, "APP_PASSWORD_EXCEPTION", str(e)


# =====================================================================
# DynamoDB Storage
# =====================================================================

def save_to_dynamodb(email, app_password, secret_key=None):
    """
    Save app password to DynamoDB for reliable storage and retrieval.
    Table: gbot-app-passwords
    Primary Key: email
    Attributes: email, app_password, secret_key, created_at, updated_at
    """
    table_name = os.environ.get("DYNAMODB_TABLE_NAME", "gbot-app-passwords")
    
    try:
        # Use shared DynamoDB resource for better connection pooling and performance
        dynamodb = get_dynamodb_resource()
        table = dynamodb.Table(table_name)
        
        # Use Unix timestamp (integer) for better DynamoDB performance and querying
        timestamp = int(time.time())
        
        item = {
            "email": email,
            "app_password": app_password,
            "created_at": timestamp,
            "updated_at": timestamp
        }
        
        # Add secret_key if provided (masked for security)
        if secret_key:
            item["secret_key"] = secret_key[:4] + "****" + secret_key[-4:]
        
        # Put item (upsert - creates or updates)
        table.put_item(Item=item)
        
        logger.info(f"[DYNAMODB] Successfully saved {email} to {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"[DYNAMODB] Failed to save {email}: {e}")
        logger.error(f"[DYNAMODB] Traceback: {traceback.format_exc()}")
        return False

# =====================================================================
# Lambda Handler
# =====================================================================


def handler(event, context):
    """
    AWS Lambda handler function.
    
    Expected event format (single user - backward compatible):
    {
        "email": "user@example.com",
        "password": "userpassword"
    }
    
    Expected event format (batch processing - up to 10 users):
    {
        "users": [
            {"email": "user1@example.com", "password": "password1"},
            {"email": "user2@example.com", "password": "password2"},
            ...
        ]
    }
    
    Returns JSON with status, results (for batch) or single user fields (for backward compatibility).
    """
    start_time = time.time()
    timings = {}
    
    logger.info("=" * 60)
    logger.info("[LAMBDA] Handler invoked")
    logger.info(f"[LAMBDA] Event type: {type(event)}")
    logger.info(f"[LAMBDA] Event content: {event}")
    logger.info(f"[LAMBDA] Context: {context}")
    logger.info("=" * 60)
    
    # Check if this is a batch request (new format) or single user (backward compatible)
    users_batch = event.get("users")
    
    if users_batch:
        # Batch processing mode (up to 10 users)
        if not isinstance(users_batch, list):
            return {
                "status": "failed",
                "error_message": "Invalid 'users' field - must be a list",
                "results": []
            }
        
        if len(users_batch) > 10:
            return {
                "status": "failed",
                "error_message": f"Too many users in batch: {len(users_batch)}. Maximum is 10.",
                "results": []
            }
        
        logger.info(f"[LAMBDA] Batch processing mode: {len(users_batch)} user(s)")
        
        # Process each user sequentially (Selenium can only handle one browser session)
        results = []
        for idx, user_data in enumerate(users_batch):
            email = user_data.get("email", "").strip()
            password = user_data.get("password", "").strip()
            
            if not email or not password:
                results.append({
                    "email": email or "unknown",
                    "status": "failed",
                    "error_message": "Email or password not provided",
                    "app_password": None,
                    "secret_key": None
                })
                continue
            
            logger.info(f"[LAMBDA] Processing user {idx + 1}/{len(users_batch)}: {email}")
            user_result = process_single_user(email, password, start_time)
            results.append(user_result)
        
        # Calculate total time
        total_time = round(time.time() - start_time, 2)
        
        # Count successes and failures
        success_count = sum(1 for r in results if r.get("status") == "success")
        failed_count = len(results) - success_count
        
        logger.info(f"[LAMBDA] Batch processing completed: {success_count} success, {failed_count} failed in {total_time}s")
        
        return {
            "status": "completed",
            "batch_size": len(users_batch),
            "success_count": success_count,
            "failed_count": failed_count,
            "total_time": total_time,
            "results": results
        }
    
    else:
        # Single user mode (backward compatible)
        email = event.get("email", os.environ.get("GW_EMAIL"))
        password = event.get("password", os.environ.get("GW_PASSWORD"))
        
        if not email or not password:
            return {
                "status": "failed",
                "step_completed": "init",
                "error_step": "init",
                "error_message": "Email or password not provided in event or environment",
                "app_password": None,
                "secret_key": None,
                "timings": timings
            }
        
        logger.info(f"[LAMBDA] Single user mode: {email}")
        return process_single_user(email, password, start_time)


def process_single_user(email, password, batch_start_time=None):
    """
    Process a single user account through all steps.
    Returns result dictionary with status, app_password, secret_key, etc.
    """
    user_start_time = time.time() if batch_start_time is None else batch_start_time
    timings = {}
    
    driver = None
    secret_key = None
    app_password = None
    step_completed = "init"
    error_code = None
    error_message = None
    
    try:
        # Step 0: Initialize Chrome driver
        step_start = time.time()
        driver = get_chrome_driver()
        timings["driver_init"] = round(time.time() - step_start, 2)
        logger.info(f"[LAMBDA] Chrome driver started for {email}")
        
        # Step 1: Login
        step_completed = "login"
        step_start = time.time()
        success, error_code, error_message = login_google(driver, email, password)
        timings["login"] = round(time.time() - step_start, 2)
        
        if not success:
            logger.error(f"[STEP] Login failed: {error_message}")
            return {
                "email": email,
                "status": "failed",
                "step_completed": step_completed,
                "error_step": step_completed,
                "error_message": error_message,
                "app_password": None,
                "secret_key": None,
                "timings": timings
            }
        
        # Step 2: Setup Authenticator (extract secret)
        step_completed = "authenticator_setup"
        step_start = time.time()
        success, secret_key, error_code, error_message = setup_authenticator(driver, email)
        timings["authenticator_setup"] = round(time.time() - step_start, 2)
        
        if not success:
            logger.error(f"[STEP] Authenticator setup failed: {error_message}")
            return {
                "email": email,
                "status": "failed",
                "step_completed": step_completed,
                "error_step": step_completed,
                "error_message": error_message,
                "app_password": None,
                "secret_key": None,
                "timings": timings
            }
        
        # Step 2.5: Upload secret to SFTP
        step_start = time.time()
        sftp_host, sftp_path = upload_secret_to_sftp(email, secret_key)
        timings["sftp_upload"] = round(time.time() - step_start, 2)
        
        if not sftp_host:
            logger.warning("[SFTP] Could not upload secret to SFTP, continuing anyway...")
        
        # Step 3a: Verify Authenticator Setup (Enter OTP and click Verify)
        step_completed = "verify_authenticator"
        step_start = time.time()
        success, error_code, error_message = verify_authenticator_setup(driver, email, secret_key)
        timings["verify_authenticator"] = round(time.time() - step_start, 2)
        
        if not success:
            logger.error(f"[STEP] Authenticator verification failed: {error_message}")
            return {
                "email": email,
                "status": "failed",
                "step_completed": step_completed,
                "error_step": step_completed,
                "error_message": error_message,
                "app_password": None,
                "secret_key": secret_key[:4] + "****" + secret_key[-4:] if secret_key else None,
                "timings": timings
            }
        
        # Step 3b: Enable 2-Step Verification (Navigate to 2SV page and click Turn On)
        step_completed = "enable_2sv"
        step_start = time.time()
        success, error_code, error_message = enable_two_step_verification(driver, email)
        timings["enable_2sv"] = round(time.time() - step_start, 2)
        
        if not success:
            logger.error(f"[STEP] 2-Step Verification enable failed: {error_message}")
            return {
                "email": email,
                "status": "failed",
                "step_completed": step_completed,
                "error_step": step_completed,
                "error_message": error_message,
                "app_password": None,
                "secret_key": secret_key[:4] + "****" + secret_key[-4:] if secret_key else None,
                "timings": timings
            }
        
        # Wait for app password page to be ready (may take up to 30 seconds after enabling 2SV)
        logger.info("[STEP] Waiting for app password authorization (may take up to 30 seconds)...")
        time.sleep(5)  # Initial wait
        
        # Step 4: Generate App Password
        step_completed = "app_password"
        step_start = time.time()
        success, app_password, error_code, error_message = generate_app_password(driver, email)
        timings["app_password"] = round(time.time() - step_start, 2)
        
        if not success:
            logger.error(f"[STEP] App Password generation failed: {error_message}")
            return {
                "email": email,
                "status": "failed",
                "step_completed": step_completed,
                "error_step": step_completed,
                "error_message": error_message,
                "app_password": None,
                "secret_key": secret_key[:4] + "****" + secret_key[-4:] if secret_key else None,
                "timings": timings
            }
        
        # Step 4.5: Save App Password to DynamoDB
        step_start = time.time()
        dynamo_success = save_to_dynamodb(email, app_password, secret_key)
        timings["dynamodb_save"] = round(time.time() - step_start, 2)
        
        if dynamo_success:
            logger.info(f"[DYNAMODB] ✓ Password saved successfully for {email}")
        else:
            logger.warning(f"[DYNAMODB] ⚠️ Could not save to DynamoDB for {email}, continuing anyway...")
        
        # All steps completed successfully
        step_completed = "completed"
        total_time = round(time.time() - user_start_time, 2)
        timings["total"] = total_time
        
        logger.info(f"[LAMBDA] All steps completed successfully for {email} in {total_time} seconds")
        
        return {
            "email": email,
            "status": "success",
            "step_completed": step_completed,
            "error_step": None,
            "error_message": None,
            "app_password": app_password,
            "secret_key": secret_key[:4] + "****" + secret_key[-4:] if secret_key else None,  # Masked for security
            "timings": timings
        }
    
    except Exception as e:
        logger.error(f"[LAMBDA] Unhandled exception for {email}: {e}")
        logger.error(traceback.format_exc())
        
        total_time = round(time.time() - user_start_time, 2)
        timings["total"] = total_time
        
        return {
            "email": email,
            "status": "failed",
            "step_completed": step_completed,
            "error_step": step_completed,
            "error_message": f"Unhandled exception: {str(e)}",
            "app_password": app_password,
            "secret_key": secret_key[:4] + "****" + secret_key[-4:] if secret_key else None,
            "timings": timings
        }
    
    finally:
        # Always cleanup driver
        if driver:
            try:
                driver.quit()
                logger.info(f"[LAMBDA] Chrome driver closed for {email}")
            except:
                pass
