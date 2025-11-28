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
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 3rd-party libraries
import boto3
from botocore.exceptions import ClientError
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
    """Get or create DynamoDB resource (reused across invocations)
    Uses a fixed region (eu-west-1) so all Lambda functions save to the same table
    This saves resources by having 1 table instead of 1 per region
    """
    global _dynamodb_resource
    if _dynamodb_resource is None:
        # Use fixed region for DynamoDB - all Lambda functions save to same table
        # This saves resources (1 table instead of 17 tables)
        dynamodb_region = os.environ.get("DYNAMODB_REGION", "eu-west-1")
        _dynamodb_resource = boto3.resource("dynamodb", region_name=dynamodb_region)
        logger.info(f"[DYNAMODB] Using DynamoDB region: {dynamodb_region} (centralized storage)")
    return _dynamodb_resource

def get_s3_client():
    """Get or create S3 client (reused across invocations)"""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client

def ensure_s3_bucket_exists(bucket_name, region='us-east-1'):
    """Create S3 bucket if it doesn't exist"""
    try:
        s3_client = get_s3_client()
        s3_client.head_bucket(Bucket=bucket_name)
        logger.debug(f"[S3] Bucket {bucket_name} already exists")
        return True
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == '404' or error_code == 'NoSuchBucket':
            try:
                # Try to create bucket
                if region == 'us-east-1':
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                logger.info(f"[S3] Created bucket {bucket_name} in region {region}")
                return True
            except Exception as create_err:
                logger.warning(f"[S3] Could not create bucket {bucket_name}: {create_err}")
                return False
        else:
            logger.warning(f"[S3] Could not access bucket {bucket_name}: {e}")
            return False
    except Exception as e:
        logger.warning(f"[S3] Error checking bucket {bucket_name}: {e}")
        return False

# =====================================================================
# Chrome Driver Initialization for AWS Lambda (with anti-detection)
# =====================================================================

# Global proxy list and rotation counter (for batch processing)
import threading
_proxy_list_cache = None
_proxy_rotation_counter = 0
_proxy_lock = threading.Lock()

def get_proxy_list_from_env():
    """Get and parse proxy list from environment variable"""
    global _proxy_list_cache
    
    if _proxy_list_cache is not None:
        return _proxy_list_cache
    
    proxy_enabled = os.environ.get('PROXY_ENABLED', 'false').lower() == 'true'
    if not proxy_enabled:
        _proxy_list_cache = []
        return []
    
    proxy_list_str = os.environ.get('PROXY_LIST', '').strip()
    if not proxy_list_str:
        _proxy_list_cache = []
        return []
    
    proxies = []
    for line in proxy_list_str.split('\n'):
        line = line.strip()
        if not line:
            continue
        
        parts = line.split(':')
        if len(parts) == 4:
            ip, port, username, password = parts
            proxies.append({
                'ip': ip,
                'port': port,
                'username': username,
                'password': password,
                'full': line
            })
    
    _proxy_list_cache = proxies
    logger.info(f"[PROXY] Loaded {len(proxies)} proxy/proxies from environment")
    return proxies

def get_rotated_proxy_for_user():
    """Get next proxy from list using round-robin rotation (thread-safe)"""
    proxies = get_proxy_list_from_env()
    if not proxies:
        return None
    
    global _proxy_rotation_counter
    with _proxy_lock:
        proxy = proxies[_proxy_rotation_counter % len(proxies)]
        _proxy_rotation_counter += 1
        return proxy

def get_proxy_from_env():
    """
    Get proxy configuration for current user (with rotation).
    Format: IP:PORT:USERNAME:PASSWORD
    Returns: dict with proxy config or None if not set
    """
    proxy = get_rotated_proxy_for_user()
    if not proxy:
        return None
    
    try:
        return {
            'ip': proxy['ip'],
            'port': proxy['port'],
            'username': proxy['username'],
            'password': proxy['password'],
            'http': f'http://{proxy["username"]}:{proxy["password"]}@{proxy["ip"]}:{proxy["port"]}',
            'https': f'http://{proxy["username"]}:{proxy["password"]}@{proxy["ip"]}:{proxy["port"]}'  # Use http for SOCKS proxies
        }
    except Exception as e:
        logger.warning(f"[PROXY] Error formatting proxy config: {e}")
        return None

# =====================================================================
# Anti-Detection Constants
# =====================================================================

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

def get_chrome_driver():
    """
    Initialize Selenium Chrome driver for AWS Lambda environment.
    Uses standard Selenium with CDP-based anti-detection (Lambda-compatible).
    Supports proxy configuration if PROXY_CONFIG environment variable is set.
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
    
    # Get proxy configuration if enabled
    proxy_config = get_proxy_from_env()
    if proxy_config:
        logger.info(f"[PROXY] Using proxy: {proxy_config['ip']}:{proxy_config['port']}")
        chrome_options.add_argument(f"--proxy-server={proxy_config['http']}")
    
    # Randomize User-Agent
    user_agent = random.choice(USER_AGENTS)
    chrome_options.add_argument(f"--user-agent={user_agent}")
    logger.info(f"[ANTI-DETECT] Using User-Agent: {user_agent}")

    # Randomize Window Size
    window_size = random.choice(WINDOW_SIZES)
    chrome_options.add_argument(f"--window-size={window_size}")
    logger.info(f"[ANTI-DETECT] Using Window Size: {window_size}")
    
    # Core stability options for Lambda
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
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
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
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
        
        # Inject comprehensive anti-detection scripts AFTER driver is stable
        # Do this BEFORE any navigation to ensure it's applied to all pages
        try:
            # Enhanced anti-detection script with multiple techniques
            anti_detection_script = '''
                // 1. Hide webdriver property
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                
                // 2. Spoof plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                    configurable: true
                });
                
                // 3. Spoof languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                    configurable: true
                });
                
                // 4. Spoof platform
                Object.defineProperty(navigator, 'platform', {
                    get: () => 'Win32',
                    configurable: true
                });
                
                // 5. Add chrome runtime
                window.chrome = {runtime: {}};
                
                // 6. Spoof permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
                
                // 7. Spoof WebGL vendor and renderer (Intel)
                const getParameter = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(parameter) {
                    if (parameter === 37445) {
                        return 'Intel Inc.';
                    }
                    if (parameter === 37446) {
                        return 'Intel Iris OpenGL Engine';
                    }
                    return getParameter.call(this, parameter);
                };
                
                // 8. Randomize canvas fingerprinting (Canvas Noise)
                const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
                HTMLCanvasElement.prototype.toDataURL = function() {
                    const context = this.getContext('2d');
                    if (context) {
                        const imageData = context.getImageData(0, 0, this.width, this.height);
                        for (let i = 0; i < imageData.data.length; i += 4) {
                            imageData.data[i] += Math.floor(Math.random() * 3) - 1;
                        }
                        context.putImageData(imageData, 0, 0);
                    }
                    return originalToDataURL.apply(this, arguments);
                };
                
                // 9. Spoof mediaDevices (enumerateDevices)
                if (navigator.mediaDevices && navigator.mediaDevices.enumerateDevices) {
                    const originalEnumerateDevices = navigator.mediaDevices.enumerateDevices;
                    navigator.mediaDevices.enumerateDevices = function() {
                        return Promise.resolve([
                            {
                                deviceId: "default",
                                kind: "audioinput",
                                label: "Default Audio Input",
                                groupId: "group1"
                            },
                            {
                                deviceId: "default",
                                kind: "videoinput",
                                label: "Default Video Input",
                                groupId: "group1"
                            },
                            {
                                deviceId: "default",
                                kind: "audiooutput",
                                label: "Default Audio Output",
                                groupId: "group1"
                            }
                        ]);
                    };
                }
                
                // 10. Spoof Hardware Concurrency and Device Memory
                Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
                Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
                
                // 11. Add realistic mouse movement simulation
                let mouseMoveCount = 0;
                document.addEventListener('DOMContentLoaded', function() {
                    setInterval(function() {
                        if (mouseMoveCount < 10) {
                            const event = new MouseEvent('mousemove', {
                                view: window,
                                bubbles: true,
                                cancelable: true,
                                clientX: Math.random() * window.innerWidth,
                                clientY: Math.random() * window.innerHeight
                            });
                            document.dispatchEvent(event);
                            mouseMoveCount++;
                        }
                    }, 2000 + Math.random() * 3000);
                });
            '''
            
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': anti_detection_script
            })
            logger.info("[LAMBDA] Enhanced anti-detection script injected successfully")
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
# Anti-Detection Helper Functions
# =====================================================================

def random_scroll_and_mouse_move(driver):
    """Perform random scroll and mouse movements to simulate human behavior"""
    try:
        # Random scroll
        scroll_amount = random.randint(100, 500)
        driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
        time.sleep(random.uniform(0.3, 0.8))
        
        # Random mouse move simulation via JavaScript
        driver.execute_script("""
            const event = new MouseEvent('mousemove', {
                view: window,
                bubbles: true,
                cancelable: true,
                clientX: Math.random() * window.innerWidth,
                clientY: Math.random() * window.innerHeight
            });
            document.dispatchEvent(event);
        """)
        time.sleep(random.uniform(0.2, 0.5))
    except Exception as e:
        logger.debug(f"[ANTI-DETECT] Random scroll/mouse move failed: {e}")

def adaptive_wait(driver, condition, timeout=8):
    """Optimized adaptive wait with shorter timeout"""
    try:
        return WebDriverWait(driver, timeout).until(condition)
    except TimeoutException:
        return None

def inject_randomized_javascript(driver):
    """Inject randomized JavaScript to make detection harder"""
    try:
        script = """
        // Modify navigator properties to make detection harder
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
        Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
        
        // Randomize the timing of JavaScript actions
        setTimeout(function() {
            console.log("Randomized action");
        }, Math.floor(Math.random() * 3000) + 500);
        
        // Random mouse movements
        document.addEventListener('mousemove', function(e) {
            let randomX = Math.random() * window.innerWidth;
            let randomY = Math.random() * window.innerHeight;
            window.scrollBy(randomX, randomY);
        });
        """
        driver.execute_script(script)
        logger.debug("[ANTI-DETECT] Randomized JavaScript injected")
    except Exception as e:
        logger.debug(f"[ANTI-DETECT] Failed to inject randomized JS: {e}")

def simulate_human_typing(element, text, driver):
    """Simulate human-like typing with random delays"""
    try:
        element.clear()
        time.sleep(random.uniform(0.1, 0.3))
        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(0.05, 0.15))  # Random delay between keystrokes
        logger.debug(f"[ANTI-DETECT] Simulated human typing for {len(text)} characters")
    except Exception as e:
        logger.warning(f"[ANTI-DETECT] Human typing simulation failed, using normal send_keys: {e}")
        element.send_keys(text)

def add_random_delays():
    """Add random delays to simulate human behavior"""
    time.sleep(random.uniform(0.5, 1.5))

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

def wait_for_visible_and_interactable(driver, xpath, timeout=30):
    """Wait for an element to be visible and interactable, then return it."""
    try:
        # Use element_to_be_clickable which ensures element is both visible and interactable
        element = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        # Scroll into view
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
        time.sleep(0.2)  # Reduced wait time
        # Focus the element to ensure it's ready for interaction
        try:
            element.click()  # Click to focus (will be cleared anyway)
            time.sleep(0.1)
        except:
            pass  # If click fails, try JavaScript focus
        return element
    except TimeoutException:
        logger.error(f"[SELENIUM] Timeout waiting for visible/interactable XPath: {xpath}")
        return None
    except Exception as e:
        logger.error(f"[SELENIUM] Error waiting for element: {e}")
        return None

def wait_for_password_clickable(driver, by_method, selector, timeout=10):
    """Wait for password field to be clickable using By.NAME or By.XPATH (like reference function)"""
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((by_method, selector))
        )
        # Focus the element
        element.click()  # Click to focus
        time.sleep(0.1)
        return element
    except TimeoutException:
        return None
    except Exception as e:
        logger.warning(f"[SELENIUM] Error waiting for password field: {e}")
        return None

def get_twocaptcha_config():
    """Get 2Captcha configuration from environment variables"""
    api_key = os.environ.get('TWOCAPTCHA_API_KEY', '').strip()
    enabled = os.environ.get('TWOCAPTCHA_ENABLED', 'false').lower() == 'true'
    
    if enabled and api_key:
        return {'enabled': True, 'api_key': api_key}
    return {'enabled': False, 'api_key': None}

def solve_recaptcha_v2(driver, api_key, site_key=None, page_url=None):
    """
    Solve reCAPTCHA v2 using 2Captcha API.
    
    Args:
        driver: Selenium WebDriver instance
        api_key: 2Captcha API key
        site_key: reCAPTCHA site key (optional, will be extracted from page if not provided)
        page_url: Current page URL (optional, will use driver.current_url if not provided)
    
    Returns:
        (success: bool, token: str|None, error: str|None)
    """
    try:
        logger.info("[2CAPTCHA] Starting reCAPTCHA v2 solving...")
        
        # Get page URL if not provided
        if not page_url:
            page_url = driver.current_url
        
        # Extract site key from page if not provided
        if not site_key:
            logger.info("[2CAPTCHA] Extracting reCAPTCHA site key from page...")
            try:
                # Try to find site key in page source
                page_source = driver.page_source
                
                # Pattern 1: data-sitekey attribute
                site_key_match = re.search(r'data-sitekey=["\']([^"\']+)["\']', page_source)
                if site_key_match:
                    site_key = site_key_match.group(1)
                    logger.info(f"[2CAPTCHA] Found site key in data-sitekey: {site_key[:20]}...")
                else:
                    # Pattern 2: recaptcha/api.js?render= or k= parameter
                    site_key_match = re.search(r'(?:recaptcha/api\.js\?render=|k=)([a-zA-Z0-9_-]+)', page_source)
                    if site_key_match:
                        site_key = site_key_match.group(1)
                        logger.info(f"[2CAPTCHA] Found site key in API URL: {site_key[:20]}...")
                    else:
                        # Pattern 3: Check iframe src
                        try:
                            iframes = driver.find_elements(By.XPATH, "//iframe[contains(@src, 'recaptcha')]")
                            for iframe in iframes:
                                iframe_src = iframe.get_attribute('src')
                                if iframe_src:
                                    site_key_match = re.search(r'k=([a-zA-Z0-9_-]+)', iframe_src)
                                    if site_key_match:
                                        site_key = site_key_match.group(1)
                                        logger.info(f"[2CAPTCHA] Found site key in iframe src: {site_key[:20]}...")
                                        break
                        except:
                            pass
                
                if not site_key:
                    logger.error("[2CAPTCHA] Could not extract reCAPTCHA site key from page")
                    return False, None, "Could not extract reCAPTCHA site key"
            except Exception as e:
                logger.error(f"[2CAPTCHA] Error extracting site key: {e}")
                return False, None, f"Error extracting site key: {e}"
        
        logger.info(f"[2CAPTCHA] Site key: {site_key[:20]}..., Page URL: {page_url[:80]}...")
        
        # Step 1: Submit CAPTCHA to 2Captcha
        logger.info("[2CAPTCHA] Submitting CAPTCHA to 2Captcha API...")
        submit_url = 'http://2captcha.com/in.php'
        submit_params = {
            'key': api_key,
            'method': 'userrecaptcha',
            'googlekey': site_key,
            'pageurl': page_url,
            'json': 1
        }
        
        submit_data = urllib.parse.urlencode(submit_params).encode('utf-8')
        submit_request = urllib.request.Request(submit_url, data=submit_data)
        
        try:
            with urllib.request.urlopen(submit_request, timeout=30) as response:
                submit_result = json.loads(response.read().decode('utf-8'))
                
                if submit_result.get('status') != 1:
                    error_msg = submit_result.get('request', 'Unknown error')
                    logger.error(f"[2CAPTCHA] Failed to submit CAPTCHA: {error_msg}")
                    return False, None, f"2Captcha submission failed: {error_msg}"
                
                task_id = submit_result.get('request')
                logger.info(f"[2CAPTCHA] CAPTCHA submitted successfully. Task ID: {task_id}")
        except Exception as e:
            logger.error(f"[2CAPTCHA] Error submitting CAPTCHA: {e}")
            return False, None, f"Error submitting CAPTCHA: {e}"
        
        # Step 2: Poll for solution (max 2 minutes, check every 5 seconds)
        logger.info("[2CAPTCHA] Waiting for 2Captcha to solve CAPTCHA (this may take 10-120 seconds)...")
        get_url = 'http://2captcha.com/res.php'
        max_wait = 120  # 2 minutes
        poll_interval = 5  # Check every 5 seconds
        waited = 0
        
        while waited < max_wait:
            time.sleep(poll_interval)
            waited += poll_interval
            
            get_params = {
                'key': api_key,
                'action': 'get',
                'id': task_id,
                'json': 1
            }
            
            get_url_with_params = f"{get_url}?{urllib.parse.urlencode(get_params)}"
            get_request = urllib.request.Request(get_url_with_params)
            
            try:
                with urllib.request.urlopen(get_request, timeout=10) as response:
                    get_result = json.loads(response.read().decode('utf-8'))
                    
                    if get_result.get('status') == 1:
                        token = get_result.get('request')
                        logger.info(f"[2CAPTCHA] ✓✓✓ CAPTCHA solved successfully! Token received (waited {waited}s)")
                        return True, token, None
                    elif get_result.get('request') == 'CAPCHA_NOT_READY':
                        if waited % 15 == 0:  # Log every 15 seconds
                            logger.info(f"[2CAPTCHA] Still solving... (waited {waited}s/{max_wait}s)")
                    else:
                        error_msg = get_result.get('request', 'Unknown error')
                        logger.error(f"[2CAPTCHA] Error getting solution: {error_msg}")
                        return False, None, f"2Captcha solution error: {error_msg}"
            except Exception as e:
                logger.warning(f"[2CAPTCHA] Error polling for solution: {e}")
                # Continue polling - might be transient error
        
        logger.error(f"[2CAPTCHA] Timeout waiting for CAPTCHA solution (waited {waited}s)")
        return False, None, "Timeout waiting for 2Captcha solution"
        
    except Exception as e:
        logger.error(f"[2CAPTCHA] Exception solving CAPTCHA: {e}")
        logger.error(traceback.format_exc())
        return False, None, f"Exception: {e}"

def inject_recaptcha_token(driver, token):
    """
    Inject the solved reCAPTCHA token into the page.
    This executes the callback function that Google reCAPTCHA expects.
    """
    try:
        logger.info("[2CAPTCHA] Injecting reCAPTCHA token into page...")
        
        # Method 1: Find and execute the callback function
        callback_script = f"""
        // Find the callback function name
        var callbackName = null;
        var scripts = document.getElementsByTagName('script');
        for (var i = 0; i < scripts.length; i++) {{
            var scriptText = scripts[i].innerHTML;
            // Fixed invalid escape sequences by double escaping backslashes
            var match = scriptText.match(/grecaptcha\\.execute\\([^,]+,\\s*{{[^}}]*callback:\\s*['"]([^'"]+)['"]/);
            if (match) {{
                callbackName = match[1];
                break;
            }}
        }}
        
        // If callback not found, try common patterns
        if (!callbackName) {{
            // Try window callbacks
            for (var key in window) {{
                if (key.startsWith('___grecaptcha_cfg') || key.includes('recaptcha')) {{
                    var cfg = window[key];
                    if (cfg && cfg.callback) {{
                        callbackName = cfg.callback;
                        break;
                    }}
                }}
            }}
        }}
        
        // Execute callback with token
        if (callbackName && window[callbackName]) {{
            window[callbackName]('{token}');
            return 'callback_executed';
        }} else {{
            // Fallback: Set token in common locations
            window.grecaptchaToken = '{token}';
            
            // Try to find and fill token input
            var tokenInputs = document.querySelectorAll('input[name*="recaptcha"], textarea[name*="recaptcha"]');
            for (var i = 0; i < tokenInputs.length; i++) {{
                tokenInputs[i].value = '{token}';
            }}
            
            // Trigger change events
            var event = new Event('change', {{ bubbles: true }});
            for (var i = 0; i < tokenInputs.length; i++) {{
                tokenInputs[i].dispatchEvent(event);
            }}
            
            return 'token_injected';
        }}
        """
        
        result = driver.execute_script(callback_script)
        logger.info(f"[2CAPTCHA] Token injection result: {result}")
        
        # Wait a moment for the page to process the token
        time.sleep(2)
        
        # Method 2: If callback method didn't work, try direct form submission
        # Check if we need to submit a form with the token
        try:
            # Look for forms that might need the token
            forms = driver.find_elements(By.TAG_NAME, "form")
            for form in forms:
                # Check if form has recaptcha-related inputs
                recaptcha_inputs = form.find_elements(By.XPATH, ".//input[contains(@name, 'recaptcha')]")
                if recaptcha_inputs:
                    for inp in recaptcha_inputs:
                        driver.execute_script("arguments[0].value = arguments[1];", inp, token)
                        driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", inp)
                    logger.info("[2CAPTCHA] Token injected into form inputs")
        except Exception as form_err:
            logger.debug(f"[2CAPTCHA] Form injection attempt: {form_err}")
        
        return True
    except Exception as e:
        logger.error(f"[2CAPTCHA] Error injecting token: {e}")
        logger.error(traceback.format_exc())
        return False

def solve_captcha_with_2captcha(driver):
    """
    Detect and solve CAPTCHA using 2Captcha API if enabled.
    Returns (solved: bool, error: str|None)
    """
    try:
        # Check if 2Captcha is enabled
        twocaptcha_config = get_twocaptcha_config()
        if not twocaptcha_config.get('enabled') or not twocaptcha_config.get('api_key'):
            logger.info("[2CAPTCHA] 2Captcha is not enabled or API key not configured")
            return False, "2Captcha not enabled"
        
        api_key = twocaptcha_config['api_key']
        logger.info("[2CAPTCHA] 2Captcha is enabled, attempting to solve CAPTCHA...")
        
        # Solve the CAPTCHA
        success, token, error = solve_recaptcha_v2(driver, api_key)
        
        if not success or not token:
            logger.error(f"[2CAPTCHA] Failed to solve CAPTCHA: {error}")
            return False, error or "Failed to solve CAPTCHA"
        
        # Inject the token into the page
        inject_success = inject_recaptcha_token(driver, token)
        
        if not inject_success:
            logger.warning("[2CAPTCHA] Token injection may have failed, but token was received")
            # Continue anyway - sometimes the page processes it even if injection seems to fail
        
        logger.info("[2CAPTCHA] ✓✓✓ CAPTCHA solved and token injected successfully!")
        return True, None
        
    except Exception as e:
        logger.error(f"[2CAPTCHA] Exception in solve_captcha_with_2captcha: {e}")
        logger.error(traceback.format_exc())
        return False, str(e)

def detect_captcha(driver):
    """Detect if Google CAPTCHA is present on the page - more accurate detection to avoid false positives"""
    try:
        # First check for explicit CAPTCHA iframes (most reliable indicator)
        try:
            captcha_iframes = driver.find_elements(By.XPATH, "//iframe[contains(@src, 'recaptcha') or contains(@src, 'google.com/recaptcha')]")
            if captcha_iframes:
                # Check if iframe is actually visible
                for iframe in captcha_iframes:
                    try:
                        if iframe.is_displayed():
                            logger.warning("[CAPTCHA] Detected visible reCAPTCHA iframe")
                            return True
                    except:
                        continue
        except:
            pass
        
        # Check for CAPTCHA-specific error messages (high confidence)
        high_confidence_indicators = [
            "//div[contains(text(), 'unusual traffic from your computer network')]",
            "//div[contains(text(), 'automated queries')]",
            "//div[contains(text(), 'verify you') and contains(text(), 'not a robot')]",
            "//span[contains(text(), 'unusual traffic from your computer network')]",
            "//span[contains(text(), 'automated queries')]",
            "//*[contains(text(), 'Try again later') and contains(text(), 'automated')]",
        ]
        
        for indicator in high_confidence_indicators:
            try:
                elements = driver.find_elements(By.XPATH, indicator)
                if elements:
                    # Verify element is visible
                    for element in elements:
                        try:
                            if element.is_displayed():
                                logger.warning(f"[CAPTCHA] Detected CAPTCHA message: {indicator}")
                                return True
                        except:
                            continue
            except:
                continue
        
        # Check for reCAPTCHA challenge container (more specific than generic captcha class)
        try:
            recaptcha_containers = driver.find_elements(By.XPATH, "//div[contains(@class, 'rc-') or contains(@id, 'recaptcha')]")
            if recaptcha_containers:
                # Only return True if we also see CAPTCHA-related text
                page_text = driver.page_source.lower()
                if any(keyword in page_text for keyword in ['verify', 'robot', 'unusual traffic', 'automated']):
                    logger.warning("[CAPTCHA] Detected reCAPTCHA container with related text")
                    return True
        except:
            pass
        
        # Last resort: check page source for multiple CAPTCHA indicators together (reduces false positives)
        page_source = driver.page_source.lower()
        captcha_keywords_found = []
        
        # Look for specific high-confidence phrases
        if 'unusual traffic from your computer network' in page_source:
            captcha_keywords_found.append('unusual traffic')
        if 'automated queries' in page_source and 'verify' in page_source:
            captcha_keywords_found.append('automated queries + verify')
        if 'try again later' in page_source and ('automated' in page_source or 'robot' in page_source):
            captcha_keywords_found.append('try again + automated/robot')
        
        # Only return True if we found multiple strong indicators
        if len(captcha_keywords_found) >= 1 and ('recaptcha' in page_source or 'captcha' in page_source):
            logger.warning(f"[CAPTCHA] Detected CAPTCHA indicators in page source: {', '.join(captcha_keywords_found)}")
            return True
        
        return False
    except Exception as e:
        logger.warning(f"[CAPTCHA] Error detecting CAPTCHA: {e}")
        return False

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
        logger.info("[STEP] Navigating to Google login page (English)...")
        driver.get("https://accounts.google.com/signin/v2/identifier?hl=en&flowName=GlifWebSignIn")
        logger.info("[STEP] Navigation to Google login page completed")
        
        # Add random delay to simulate human behavior
        add_random_delays()
        
        # Inject additional anti-detection scripts after page load
        inject_randomized_javascript(driver)
        
        # Perform random scroll and mouse movements
        random_scroll_and_mouse_move(driver)
        
        time.sleep(1)  # Additional wait for page to stabilize
        logger.info("[STEP] Page stabilized, proceeding with login")
        
        # NOTE: CAPTCHA check removed from here - CAPTCHA rarely appears before email entry
        # CAPTCHA typically appears after email submission, so we'll check after that
    except Exception as nav_error:
        logger.error(f"[STEP] Navigation failed: {nav_error}")
        logger.error(traceback.format_exc())
        return False, "navigation_failed", str(nav_error)

    try:
        # Enter email with human-like behavior
        email_input = wait_for_xpath(driver, "//input[@id='identifierId']", timeout=30)
        
        # Random scroll before interaction
        random_scroll_and_mouse_move(driver)
        
        email_input.clear()
        add_random_delays()
        
        # Simulate human typing for email
        simulate_human_typing(email_input, email, driver)
        logger.info("[STEP] Email entered with human-like typing")
        
        add_random_delays()
        
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

        # Wait for page to transition after email submission
        time.sleep(3)  # Increased wait to allow page transition
        
        # Add human-like behavior after email submission
        add_random_delays()
        random_scroll_and_mouse_move(driver)
        
        # Check if we're still on the identifier page (email submission failed or CAPTCHA appeared)
        current_url = driver.current_url
        page_title = driver.title
        logger.info(f"[STEP] After email submission - URL: {current_url[:100]}..., Title: {page_title}")
        
        # Check for CAPTCHA after email submission (this is when it typically appears)
        if detect_captcha(driver):
            logger.warning("[STEP] ⚠️ CAPTCHA detected after email submission!")
            
            # Try to solve CAPTCHA using 2Captcha if enabled
            solved, solve_error = solve_captcha_with_2captcha(driver)
            
            if solved:
                logger.info("[STEP] ✓✓✓ CAPTCHA solved using 2Captcha! Continuing with login...")
                # Wait a moment for page to process the solved CAPTCHA
                time.sleep(3)
                
                # Check if CAPTCHA is still present (should be gone if solved correctly)
                if detect_captcha(driver):
                    logger.warning("[STEP] ⚠️ CAPTCHA still present after solving attempt. Retrying...")
                    # Try one more time
                    time.sleep(2)
                    solved_retry, _ = solve_captcha_with_2captcha(driver)
                    if not solved_retry:
                        logger.error("[STEP] ✗✗✗ CAPTCHA solving failed after retry")
                        return False, "CAPTCHA_SOLVE_FAILED", "CAPTCHA detected and 2Captcha solving failed"
                else:
                    logger.info("[STEP] ✓ CAPTCHA cleared after solving! Proceeding...")
            else:
                # CAPTCHA solving failed or not enabled
                logger.error(f"[STEP] ✗✗✗ CAPTCHA BLOCKING LOGIN - Solving failed: {solve_error}")
                # Check for CAPTCHA text in page
                try:
                    page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
                    if any(indicator in page_text for indicator in ['type the text you hear or see', 'verify you\'re not a robot', 'unusual traffic']):
                        logger.error("[STEP] ✗✗✗ CAPTCHA BLOCKING LOGIN - Page text confirms CAPTCHA presence")
                        return False, "CAPTCHA_DETECTED", f"CAPTCHA detected after email submission. 2Captcha solving failed: {solve_error}"
                except:
                    pass
                return False, "CAPTCHA_DETECTED", f"CAPTCHA detected after email submission. 2Captcha solving failed: {solve_error}"
        
        # Check if we're still on identifier page (email might not have been submitted or page redirected back)
        if '/signin/identifier' in current_url or 'identifier' in current_url.lower():
            logger.warning("[STEP] ⚠️ Still on identifier page after email submission - checking for issues...")
            # Check for error messages
            try:
                error_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Couldn\\'t find your Google Account') or contains(text(), 'Enter a valid email') or contains(text(), 'error')]")
                if error_elements:
                    error_text = error_elements[0].text
                    logger.error(f"[STEP] ✗ Error on identifier page: {error_text}")
                    return False, "EMAIL_ERROR", f"Error after email submission: {error_text}"
            except:
                pass
            
            # Check if CAPTCHA is present (might not be detected by detect_captcha)
            try:
                captcha_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Type the text you hear or see') or contains(@class, 'captcha') or contains(@id, 'captcha')]")
                if captcha_elements:
                    logger.warning("[STEP] ⚠️ CAPTCHA FOUND ON IDENTIFIER PAGE - Attempting to solve...")
                    
                    # Try to solve CAPTCHA using 2Captcha if enabled
                    solved, solve_error = solve_captcha_with_2captcha(driver)
                    
                    if solved:
                        logger.info("[STEP] ✓✓✓ CAPTCHA solved using 2Captcha! Retrying email submission...")
                        time.sleep(3)
                        # Retry email submission after solving
                        try:
                            email_input = wait_for_xpath(driver, "//input[@id='identifierId']", timeout=10)
                            if email_input:
                                email_input.clear()
                                simulate_human_typing(email_input, email, driver)
                                email_input.send_keys(Keys.RETURN)
                                logger.info("[STEP] Email resubmitted after CAPTCHA solving")
                                time.sleep(3)
                            else:
                                logger.error("[STEP] Could not find email input after CAPTCHA solving")
                                return False, "CAPTCHA_SOLVE_FAILED", "CAPTCHA solved but could not resubmit email"
                        except Exception as retry_err:
                            logger.error(f"[STEP] Error resubmitting email after CAPTCHA solving: {retry_err}")
                            return False, "CAPTCHA_SOLVE_FAILED", f"CAPTCHA solved but email resubmission failed: {retry_err}"
                    else:
                        logger.error(f"[STEP] ✗✗✗ CAPTCHA solving failed: {solve_error}")
                        return False, "CAPTCHA_DETECTED", f"CAPTCHA detected on identifier page. 2Captcha solving failed: {solve_error}"
            except:
                pass
        
        time.sleep(2)  # Additional wait for password field to appear
        
        # Check for iframes first (Google sometimes uses iframes for password field)
        password_input = None
        try:
            # Primary method: Use By.NAME like reference function (most reliable)
            logger.info("[STEP] Trying to find password input using By.NAME='Passwd' (primary method)")
            password_input = wait_for_password_clickable(driver, By.NAME, "Passwd", timeout=10)
            if password_input:
                logger.info("[STEP] Found password input using By.NAME='Passwd'")
        except Exception as primary_err:
            logger.warning(f"[STEP] Primary method failed: {primary_err}")
        
        if not password_input:
            # Fallback: Try XPath methods (fixed invalid XPath syntax)
            password_input_xpaths = [
                "//input[@name='Passwd']",
                "//input[@type='password']",
                "/html/body/div[2]/div[1]/div[1]/div[2]/c-wiz/main/div[2]/div/div/div/form/span/section[2]/div/div/div[1]/div[1]/div/div/div/div/div[1]/div/div[1]/input",  # User-provided working XPath
                "//input[@id='password']",
                "//input[@name='password']",
                "//input[contains(@aria-label, 'password')]",
                "//input[contains(@aria-label, 'Password')]",
            ]
            
            # Try to find visible and interactable password field
            for xpath in password_input_xpaths:
                try:
                    logger.info(f"[STEP] Trying to find password input with XPath: {xpath}")
                    password_input = wait_for_visible_and_interactable(driver, xpath, timeout=8)
                    if password_input:
                        logger.info(f"[STEP] Found password input using xpath: {xpath}")
                        break
                except Exception as e:
                    logger.warning(f"[STEP] Failed to find password with {xpath}: {e}")
                    continue
        
        # If not found in main document, check iframes
        if not password_input:
                logger.info("[STEP] Password field not found in main document, checking iframes...")
                iframes = driver.find_elements(By.TAG_NAME, "iframe")
                for iframe in iframes:
                    try:
                        driver.switch_to.frame(iframe)
                        for xpath in password_input_xpaths:
                            try:
                                password_input = wait_for_visible_and_interactable(driver, xpath, timeout=5)
                                if password_input:
                                    logger.info(f"[STEP] Found password input in iframe using xpath: {xpath}")
                                    break
                            except:
                                continue
                        if password_input:
                            break
                        driver.switch_to.default_content()
                    except Exception as iframe_err:
                        logger.warning(f"[STEP] Error checking iframe: {iframe_err}")
                        driver.switch_to.default_content()
                        continue
            
        if not password_input:
            # Last resort: try JavaScript to find and interact with password field
            logger.info("[STEP] Trying JavaScript method to find password field...")
            try:
                password_input = driver.execute_script("""
                    var inputs = document.querySelectorAll('input[type="password"], input[name="Passwd"], input[name="password"]');
                    for (var i = 0; i < inputs.length; i++) {
                        var input = inputs[i];
                        if (input.offsetParent !== null) { // Check if visible
                            input.scrollIntoView({behavior: 'smooth', block: 'center'});
                            input.focus();
                            return input;
                        }
                    }
                    return null;
                """)
                if password_input:
                    logger.info("[STEP] Found password input using JavaScript")
            except Exception as js_err:
                logger.error(f"[STEP] JavaScript method failed: {js_err}")
        
        if not password_input:
                # DEBUG: Capture what's actually on the page when password field is not found
                logger.error("=" * 80)
                logger.error("[DEBUG] Password field not found - capturing page state for diagnosis")
                logger.error("=" * 80)
                
                # ALWAYS log page source to CloudWatch (even if S3 fails)
                try:
                    page_source = driver.page_source
                    current_url = driver.current_url
                    page_title = driver.title
                    
                    # Log critical information to CloudWatch
                    logger.error(f"[DEBUG] Current URL: {current_url}")
                    logger.error(f"[DEBUG] Page title: {page_title}")
                    
                    # Check for CAPTCHA indicators in page source
                    captcha_indicators = [
                        'recaptcha', 'g-recaptcha', 'captcha', 'challenge', 
                        'unusual traffic', 'automated queries', 'verify you\'re not a robot'
                    ]
                    found_indicators = []
                    page_source_lower = page_source.lower()
                    for indicator in captcha_indicators:
                        if indicator in page_source_lower:
                            found_indicators.append(indicator)
                    
                    if found_indicators:
                        logger.error(f"[DEBUG] ⚠️ CAPTCHA/BLOCKER DETECTED! Found indicators: {found_indicators}")
                    
                    # Log first 5000 chars of page source to CloudWatch
                    logger.error(f"[DEBUG] Page source (first 5000 chars): {page_source[:5000]}")
                    
                    # Try to extract visible text
                    try:
                        page_text = driver.find_element(By.TAG_NAME, "body").text
                        logger.error(f"[DEBUG] Visible page text (first 2000 chars): {page_text[:2000]}")
                    except:
                        logger.error("[DEBUG] Could not extract page text")
                    
                except Exception as log_err:
                    logger.error(f"[DEBUG] Failed to log page state: {log_err}")
                
                # Save screenshot and page source to S3 for investigation (optional)
                screenshot_saved = False
                page_source_saved = False
                try:
                    s3_bucket = os.environ.get("S3_DEBUG_BUCKET", "gbot-debug-screenshots")
                    
                    # Ensure bucket exists
                    s3_region = os.environ.get("AWS_REGION", "us-east-1")
                    ensure_s3_bucket_exists(s3_bucket, s3_region)
                    
                    timestamp = int(time.time())
                    email_safe = email.replace("@", "_at_").replace(".", "_")
                    screenshot_key = f"password-field-not-found/{email_safe}_{timestamp}_screenshot.png"
                    page_source_key = f"password-field-not-found/{email_safe}_{timestamp}_page_source.html"
                    
                    # Take screenshot
                    try:
                        screenshot_path = f"/tmp/password_not_found_{timestamp}.png"
                        driver.save_screenshot(screenshot_path)
                        
                        # Upload to S3
                        s3_client = get_s3_client()
                        s3_client.upload_file(
                            screenshot_path,
                            s3_bucket,
                            screenshot_key,
                            ExtraArgs={'ContentType': 'image/png'}
                        )
                        screenshot_saved = True
                        logger.error(f"[DEBUG] ✓ Screenshot saved to S3: s3://{s3_bucket}/{screenshot_key}")
                        
                        # Clean up local file
                        try:
                            os.remove(screenshot_path)
                        except:
                            pass
                    except Exception as screenshot_err:
                        logger.error(f"[DEBUG] ✗ Failed to save screenshot: {screenshot_err}")
                    
                    # Save page source (HTML) to S3
                    try:
                        if 'page_source' not in locals():
                            page_source = driver.page_source
                        page_source_path = f"/tmp/password_not_found_{timestamp}.html"
                        with open(page_source_path, 'w', encoding='utf-8') as f:
                            f.write(page_source)
                        
                        # Upload to S3
                        s3_client = get_s3_client()
                        s3_client.upload_file(
                            page_source_path,
                            s3_bucket,
                            page_source_key,
                            ExtraArgs={'ContentType': 'text/html'}
                        )
                        page_source_saved = True
                        logger.error(f"[DEBUG] ✓ Page source saved to S3: s3://{s3_bucket}/{page_source_key}")
                        
                        # Clean up local file
                        try:
                            os.remove(page_source_path)
                        except:
                            pass
                    except Exception as page_source_err:
                        logger.error(f"[DEBUG] ✗ Failed to save page source to S3: {page_source_err}")
                    
                    if screenshot_saved or page_source_saved:
                        logger.error(f"[DEBUG] Investigation files saved to S3 bucket: {s3_bucket}")
                        logger.error(f"[DEBUG] Check S3 for: {screenshot_key} and {page_source_key}")
                    
                except Exception as s3_err:
                    logger.error(f"[DEBUG] ✗ Error with S3 operations: {s3_err}")
                    logger.error(f"[DEBUG] Note: Page source and debug info are logged above in CloudWatch")
                
                # Additional detailed debugging: All input elements and their attributes
                try:
                    try:
                        all_inputs = driver.find_elements(By.TAG_NAME, "input")
                        logger.error(f"[DEBUG] Found {len(all_inputs)} input element(s) on page:")
                        for i, inp in enumerate(all_inputs[:20]):  # Limit to first 20
                            try:
                                inp_type = inp.get_attribute('type') or 'N/A'
                                inp_name = inp.get_attribute('name') or 'N/A'
                                inp_id = inp.get_attribute('id') or 'N/A'
                                inp_placeholder = inp.get_attribute('placeholder') or 'N/A'
                                inp_aria_label = inp.get_attribute('aria-label') or 'N/A'
                                is_displayed = inp.is_displayed()
                                is_enabled = inp.is_enabled()
                                logger.error(f"[DEBUG]   Input {i+1}: type={inp_type}, name={inp_name}, id={inp_id}, placeholder={inp_placeholder}, aria-label={inp_aria_label}, displayed={is_displayed}, enabled={is_enabled}")
                            except Exception as inp_err:
                                logger.error(f"[DEBUG]   Input {i+1}: Error reading attributes: {inp_err}")
                    except Exception as inputs_err:
                        logger.error(f"[DEBUG] Could not list input elements: {inputs_err}")
                    
                    # 5. Check for error messages or alerts
                    try:
                        error_selectors = [
                            "//*[contains(text(), 'error') or contains(text(), 'Error')]",
                            "//*[contains(text(), 'wrong') or contains(text(), 'Wrong')]",
                            "//*[contains(text(), 'invalid') or contains(text(), 'Invalid')]",
                            "//*[contains(text(), 'try again') or contains(text(), 'Try again')]",
                            "//*[@role='alert']",
                            "//*[contains(@class, 'error')]",
                        ]
                        for selector in error_selectors:
                            try:
                                error_elements = driver.find_elements(By.XPATH, selector)
                                if error_elements:
                                    for err_elem in error_elements[:5]:  # First 5 error elements
                                        try:
                                            err_text = err_elem.text.strip()
                                            if err_text:
                                                logger.error(f"[DEBUG] Error message found: {err_text}")
                                        except:
                                            pass
                            except:
                                pass
                    except Exception as err_check_err:
                        logger.error(f"[DEBUG] Could not check for error messages: {err_check_err}")
                    
                    # 6. Check for CAPTCHA elements
                    try:
                        captcha_indicators = [
                            "//iframe[contains(@src, 'recaptcha')]",
                            "//div[contains(@class, 'recaptcha')]",
                            "//*[contains(text(), 'unusual traffic')]",
                            "//*[contains(text(), 'verify you')]",
                        ]
                        for indicator in captcha_indicators:
                            try:
                                captcha_elements = driver.find_elements(By.XPATH, indicator)
                                if captcha_elements:
                                    logger.error(f"[DEBUG] CAPTCHA indicator found: {indicator}")
                            except:
                                pass
                    except Exception as captcha_check_err:
                        logger.error(f"[DEBUG] Could not check for CAPTCHA: {captcha_check_err}")
                    
                    # 7. Get page source snippet (first 5000 chars)
                    try:
                        page_source = driver.page_source
                        logger.error(f"[DEBUG] Page source snippet (first 5000 chars): {page_source[:5000]}")
                    except Exception as source_err:
                        logger.error(f"[DEBUG] Could not get page source: {source_err}")
                    
                    # 8. List all iframes
                    try:
                        iframes = driver.find_elements(By.TAG_NAME, "iframe")
                        logger.error(f"[DEBUG] Found {len(iframes)} iframe(s) on page")
                        for i, iframe in enumerate(iframes[:10]):  # First 10 iframes
                            try:
                                iframe_src = iframe.get_attribute('src') or 'N/A'
                                iframe_id = iframe.get_attribute('id') or 'N/A'
                                logger.error(f"[DEBUG]   Iframe {i+1}: src={iframe_src[:100]}, id={iframe_id}")
                            except:
                                pass
                    except Exception as iframe_err:
                        logger.error(f"[DEBUG] Could not list iframes: {iframe_err}")
                    
                    # 9. Check for any password-related elements (even if not visible)
                    try:
                        password_elements = driver.find_elements(By.XPATH, "//*[contains(@name, 'pass') or contains(@id, 'pass') or contains(@type, 'password') or contains(@aria-label, 'pass')]")
                        logger.error(f"[DEBUG] Found {len(password_elements)} password-related element(s):")
                        for i, pwd_elem in enumerate(password_elements[:10]):
                            try:
                                pwd_tag = pwd_elem.tag_name
                                pwd_type = pwd_elem.get_attribute('type') or 'N/A'
                                pwd_name = pwd_elem.get_attribute('name') or 'N/A'
                                pwd_id = pwd_elem.get_attribute('id') or 'N/A'
                                pwd_displayed = pwd_elem.is_displayed()
                                logger.error(f"[DEBUG]   Password element {i+1}: tag={pwd_tag}, type={pwd_type}, name={pwd_name}, id={pwd_id}, displayed={pwd_displayed}")
                            except:
                                pass
                    except Exception as pwd_elem_err:
                        logger.error(f"[DEBUG] Could not check password elements: {pwd_elem_err}")
                    
                    logger.error("=" * 80)
                    logger.error("[DEBUG] End of page state capture")
                    logger.error("=" * 80)
                except Exception as debug_err:
                    logger.error(f"[DEBUG] Error during page state capture: {debug_err}")
                    logger.error(traceback.format_exc())
                
                error_msg = "Password field not found after email submission (checked main document and iframes)."
                if screenshot_saved or page_source_saved:
                    error_msg += f" Screenshot and page source saved to S3 for investigation."
                else:
                    error_msg += " See DEBUG logs above for page state."
                return False, "LOGIN_PASSWORD_FIELD_NOT_FOUND", error_msg
        
        # Clear and enter password with multiple fallback methods
        try:
            # Add human-like behavior before password entry
            random_scroll_and_mouse_move(driver)
            add_random_delays()
            
            # Method 1: Focus and clear using JavaScript first (more reliable)
            driver.execute_script("arguments[0].focus();", password_input)
            time.sleep(random.uniform(0.1, 0.2))
            driver.execute_script("arguments[0].click();", password_input)
            time.sleep(random.uniform(0.1, 0.2))
            
            # Try standard clear first
            try:
                password_input.clear()
            except:
                # If clear fails, use JavaScript
                driver.execute_script("arguments[0].value = '';", password_input)
            
            time.sleep(random.uniform(0.2, 0.4))  # Random wait time
            
            # Enter password using human-like typing simulation
            simulate_human_typing(password_input, password, driver)
            logger.info("[STEP] Password entered using human-like typing simulation")
        except Exception as e1:
            logger.warning(f"[STEP] Standard method failed: {e1}, trying JavaScript...")
            try:
                # Method 2: JavaScript interaction (more reliable fallback)
                driver.execute_script("arguments[0].focus();", password_input)
                driver.execute_script("arguments[0].click();", password_input)
                driver.execute_script("arguments[0].value = '';", password_input)
                driver.execute_script("arguments[0].value = arguments[1];", password_input, password)
                driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", password_input)
                driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", password_input)
                logger.info("[STEP] Password entered using JavaScript method")
            except Exception as e2:
                logger.error(f"[STEP] JavaScript method also failed: {e2}")
                return False, "LOGIN_PASSWORD_INPUT_FAILED", f"Could not enter password: {e2}"
        
        # Verify password was entered
        try:
            entered_password = password_input.get_attribute('value')
            if not entered_password or entered_password != password:
                logger.warning(f"[STEP] Password verification failed. Expected length: {len(password)}, Got: {len(entered_password) if entered_password else 0}")
                # Try one more time with JavaScript
                try:
                    driver.execute_script("arguments[0].value = arguments[1];", password_input, password)
                    entered_password = password_input.get_attribute('value')
                    if entered_password != password:
                        logger.error("[STEP] Password still not entered correctly after retry")
                except:
                    pass
        except Exception as verify_err:
            logger.warning(f"[STEP] Could not verify password entry: {verify_err}")
        
        logger.info("[STEP] Password entered successfully")
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

        # Add human-like behavior after password submission
        add_random_delays()
        random_scroll_and_mouse_move(driver)

        # Wait for potential challenge pages, intermediate pages, or account home
        # Google may show: speedbump, verification, phone prompt, TOTP, recovery email, etc.
        # We'll wait longer and handle what we can, skip what we can't
        max_wait_attempts = 30  # Increased from 15 to 30 (90 seconds total)
        wait_interval = 3
        current_url = None
        
        for attempt in range(max_wait_attempts):
            time.sleep(wait_interval)
            
            # Add occasional random behavior during wait
            if attempt % 3 == 0:
                random_scroll_and_mouse_move(driver)
            
            try:
                current_url = driver.current_url
                logger.info(f"[STEP] Post-login check {attempt + 1}/{max_wait_attempts}: URL = {current_url}")
            except Exception as e:
                logger.error(f"[STEP] Failed to get current URL: {e}")
                return False, "driver_crashed", f"Driver crashed while checking URL: {e}"
            
            # Check for CAPTCHA after password submission (this is another common place for CAPTCHA)
            if detect_captcha(driver):
                logger.warning("[STEP] ⚠️ CAPTCHA detected after password submission!")
                
                # Try to solve CAPTCHA using 2Captcha if enabled
                solved, solve_error = solve_captcha_with_2captcha(driver)
                
                if solved:
                    logger.info("[STEP] ✓✓✓ CAPTCHA solved using 2Captcha! Continuing with login...")
                    # Wait a moment for page to process the solved CAPTCHA
                    time.sleep(3)
                    
                    # Check if CAPTCHA is still present
                    if detect_captcha(driver):
                        logger.warning("[STEP] ⚠️ CAPTCHA still present after solving. Retrying...")
                        time.sleep(2)
                        solved_retry, _ = solve_captcha_with_2captcha(driver)
                        if not solved_retry:
                            logger.error("[STEP] ✗✗✗ CAPTCHA solving failed after retry")
                            return False, "CAPTCHA_SOLVE_FAILED", "CAPTCHA detected after password submission and 2Captcha solving failed"
                    else:
                        logger.info("[STEP] ✓ CAPTCHA cleared after solving! Proceeding...")
                else:
                    logger.error(f"[STEP] ✗✗✗ CAPTCHA solving failed: {solve_error}")
                    return False, "CAPTCHA_DETECTED", f"CAPTCHA detected after password submission. 2Captcha solving failed: {solve_error}"
            
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
                    # Check for captcha
                    if detect_captcha(driver):
                        logger.warning("[STEP] ⚠️ CAPTCHA detected on 2SV authenticator page!")
                        
                        # Try to solve CAPTCHA using 2Captcha if enabled
                        solved, solve_error = solve_captcha_with_2captcha(driver)
                        
                        if solved:
                            logger.info("[STEP] ✓✓✓ CAPTCHA solved using 2Captcha! Continuing...")
                            time.sleep(3)
                            driver.refresh()
                            time.sleep(2)
                        else:
                            logger.error(f"[STEP] ✗✗✗ CAPTCHA solving failed: {solve_error}")
                            return False, None, "CAPTCHA_DETECTED", f"CAPTCHA detected on 2SV authenticator page. 2Captcha solving failed: {solve_error}"
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
    # Add human-like behavior before navigating
    add_random_delays()
    random_scroll_and_mouse_move(driver)
    logger.info(f"[STEP] Setting up Authenticator for {email}")
    
    try:
        # Navigate to authenticator setup page
        logger.info("[STEP] Navigating to Authenticator setup page...")
        driver.get("https://myaccount.google.com/two-step-verification/authenticator?hl=en")
        
        # Add human-like behavior after page load
        add_random_delays()
        random_scroll_and_mouse_move(driver)
        inject_randomized_javascript(driver)
        
        time.sleep(2)  # Reduced from 3 to 2
        
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
        
        # Check for captcha
        if detect_captcha(driver):
            logger.warning("[STEP] ⚠️ CAPTCHA detected on 2SV page!")
            
            # Try to solve CAPTCHA using 2Captcha if enabled
            solved, solve_error = solve_captcha_with_2captcha(driver)
            
            if solved:
                logger.info("[STEP] ✓✓✓ CAPTCHA solved using 2Captcha! Continuing...")
                time.sleep(3)
                # Refresh page to ensure CAPTCHA is cleared
                driver.refresh()
                time.sleep(2)
            else:
                logger.error(f"[STEP] ✗✗✗ CAPTCHA solving failed: {solve_error}")
                return False, None, "CAPTCHA_DETECTED", f"CAPTCHA detected on 2SV page. 2Captcha solving failed: {solve_error}"
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
        
        # Add human-like behavior after page load
        add_random_delays()
        random_scroll_and_mouse_move(driver)
        inject_randomized_javascript(driver)
        
        # Check for captcha
        if detect_captcha(driver):
            logger.warning("[STEP] ⚠️ CAPTCHA detected on app passwords page!")
            
            # Try to solve CAPTCHA using 2Captcha if enabled
            solved, solve_error = solve_captcha_with_2captcha(driver)
            
            if solved:
                logger.info("[STEP] ✓✓✓ CAPTCHA solved using 2Captcha! Continuing...")
                time.sleep(3)
                # Refresh page to ensure CAPTCHA is cleared
                driver.refresh()
                time.sleep(2)
            else:
                logger.error(f"[STEP] ✗✗✗ CAPTCHA solving failed: {solve_error}")
                return False, None, "CAPTCHA_DETECTED", f"CAPTCHA detected on app passwords page. 2Captcha solving failed: {solve_error}"
        
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

def ensure_dynamodb_table_exists(table_name="gbot-app-passwords"):
    """
    Ensure DynamoDB table exists. Creates it if it doesn't exist.
    Uses a fixed region (eu-west-1) so all Lambda functions use the same table.
    Returns True if table exists or was created, False on error.
    Note: Table creation is asynchronous, so we don't wait for it to be active.
    """
    try:
        # Use fixed region for DynamoDB - centralized storage
        dynamodb_region = os.environ.get("DYNAMODB_REGION", "eu-west-1")
        dynamodb_client = boto3.client("dynamodb", region_name=dynamodb_region)
        
        # Check if table exists
        try:
            dynamodb_client.describe_table(TableName=table_name)
            logger.info(f"[DYNAMODB] Table {table_name} already exists")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                logger.error(f"[DYNAMODB] Error checking table {table_name}: {e}")
                return False
            
            # Table doesn't exist, create it
            logger.info(f"[DYNAMODB] Table {table_name} not found. Creating...")
            try:
                dynamodb_client.create_table(
                    TableName=table_name,
                    KeySchema=[
                        {'AttributeName': 'email', 'KeyType': 'HASH'}  # Partition key
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'email', 'AttributeType': 'S'}
                    ],
                    BillingMode='PAY_PER_REQUEST'  # On-demand pricing (no provisioned capacity)
                )
                logger.info(f"[DYNAMODB] ✓ Table {table_name} creation initiated (will be active in ~10-30 seconds)")
                # Don't wait for table to be active - it's asynchronous
                # The first save attempt might fail, but subsequent attempts will succeed
                return True
            except ClientError as create_error:
                if create_error.response['Error']['Code'] == 'ResourceInUseException':
                    # Table is being created by another process, that's fine
                    logger.info(f"[DYNAMODB] Table {table_name} is being created by another process")
                    return True
                else:
                    logger.error(f"[DYNAMODB] Failed to create table {table_name}: {create_error}")
                    return False
                    
    except Exception as e:
        logger.error(f"[DYNAMODB] Exception ensuring table exists: {e}")
        logger.error(f"[DYNAMODB] Traceback: {traceback.format_exc()}")
        return False

def save_to_dynamodb(email, app_password, secret_key=None):
    """
    Save app password to DynamoDB for reliable storage and retrieval.
    Table: gbot-app-passwords
    Primary Key: email
    Attributes: email, app_password, secret_key, created_at, updated_at
    
    Automatically creates the table if it doesn't exist.
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
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        
        if error_code == 'ResourceNotFoundException':
            # Table doesn't exist, try to create it
            logger.warning(f"[DYNAMODB] Table {table_name} not found. Attempting to create...")
            if ensure_dynamodb_table_exists(table_name):
                # Wait a moment for table to become available (if just created)
                time.sleep(2)
                # Retry the save operation
                try:
                    table = dynamodb.Table(table_name)
                    table.put_item(Item=item)
                    logger.info(f"[DYNAMODB] Successfully saved {email} to {table_name} after table creation")
                    return True
                except Exception as retry_error:
                    logger.error(f"[DYNAMODB] Failed to save {email} after table creation: {retry_error}")
                    logger.error(f"[DYNAMODB] ⚠️ Table {table_name} may still be initializing. Please wait 10-30 seconds and retry.")
                    return False
            else:
                logger.error(f"[DYNAMODB] Failed to create table {table_name}. Please create it manually in AWS Console.")
                logger.error(f"[DYNAMODB] Table name: {table_name}")
                logger.error(f"[DYNAMODB] Primary key: email (String)")
                logger.error(f"[DYNAMODB] Billing mode: PAY_PER_REQUEST")
                return False
        else:
            logger.error(f"[DYNAMODB] Failed to save {email}: {e}")
            logger.error(f"[DYNAMODB] Error code: {error_code}")
            return False
        
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
        
        # CRITICAL: Enforce 10-user limit - truncate if exceeded
        MAX_USERS_PER_BATCH = 10
        if len(users_batch) > MAX_USERS_PER_BATCH:
            logger.warning(f"[LAMBDA] ⚠️ WARNING: Batch has {len(users_batch)} users, exceeding limit of {MAX_USERS_PER_BATCH}!")
            logger.warning(f"[LAMBDA] Truncating batch to {MAX_USERS_PER_BATCH} users")
            users_batch = users_batch[:MAX_USERS_PER_BATCH]
        
        logger.info(f"[LAMBDA] Batch processing mode: {len(users_batch)} user(s) (MAX: {MAX_USERS_PER_BATCH})")
        logger.info(f"[LAMBDA] Starting PARALLEL processing of {len(users_batch)} user(s)")
        
        # Ensure DynamoDB table exists before processing
        table_name = os.environ.get("DYNAMODB_TABLE_NAME", "gbot-app-passwords")
        logger.info(f"[LAMBDA] Ensuring DynamoDB table exists: {table_name}")
        ensure_dynamodb_table_exists(table_name)
        
        # Process all users in PARALLEL - each user gets their own Chrome driver instance
        results = []
        
        def process_user_wrapper(user_data, idx):
            """Wrapper function to process a single user with proper error handling"""
            email = user_data.get("email", "").strip()
            password = user_data.get("password", "").strip()
            
            # Get proxy for this user (rotation happens automatically)
            proxy = get_rotated_proxy_for_user()
            if proxy:
                logger.info(f"[PROXY] [{email}] Using proxy: {proxy['ip']}:{proxy['port']}")
                # Set proxy in environment for this user's Chrome driver
                os.environ['PROXY_CONFIG'] = proxy['full']
            else:
                # Clear proxy config if not available
                os.environ.pop('PROXY_CONFIG', None)
            
            if not email or not password:
                return {
                    "email": email or "unknown",
                    "status": "failed",
                    "error_message": "Email or password not provided",
                    "app_password": None,
                    "secret_key": None
                }
            
            # Stagger Chrome initialization to avoid resource contention
            # Each thread waits a bit before starting to spread out resource usage
            # This prevents all Chrome instances from initializing at exactly the same time
            stagger_delay = idx * 1.0  # 1 second between each Chrome start
            if stagger_delay > 0:
                logger.info(f"[LAMBDA] [THREAD] Staggering Chrome start for user {idx + 1}: waiting {stagger_delay}s")
                time.sleep(stagger_delay)
            
            logger.info(f"[LAMBDA] [THREAD] Starting parallel processing of user {idx + 1}/{len(users_batch)}: {email}")
            try:
                user_result = process_single_user(email, password, start_time)
                logger.info(f"[LAMBDA] [THREAD] Completed user {idx + 1}/{len(users_batch)}: {email} - Status: {user_result.get('status', 'unknown')}")
                return user_result
            except Exception as e:
                logger.error(f"[LAMBDA] [THREAD] Exception processing user {idx + 1}/{len(users_batch)}: {email} - {str(e)}")
                logger.error(f"[LAMBDA] [THREAD] Traceback: {traceback.format_exc()}")
                return {
                    "email": email,
                    "status": "failed",
                    "error_message": f"Exception during processing: {str(e)}",
                    "app_password": None,
                    "secret_key": None
                }
        
        # Use ThreadPoolExecutor to process users in parallel
        # Lambda has 2048 MB memory, each Chrome instance uses ~200-300 MB
        # We can safely run 15-20 concurrent instances (15 * 300MB = 4500MB theoretical, but Chrome instances share memory)
        # In practice, with 2048 MB, we can run 15-20 Chrome instances efficiently
        # For maximum speed, process all users in parallel (up to 20 per function)
        max_concurrent = min(20, len(users_batch))  # Process up to 20 users in parallel (all users in batch)
        logger.info(f"[LAMBDA] Using {max_concurrent} concurrent workers for {len(users_batch)} users (processing all users in parallel for maximum speed)")
        
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            # Submit all tasks
            future_to_user = {
                executor.submit(process_user_wrapper, user_data, idx): (idx, user_data)
                for idx, user_data in enumerate(users_batch)
            }
            
            # Collect results as they complete (maintain order)
            completed_results = {}
            for future in as_completed(future_to_user):
                idx, user_data = future_to_user[future]
                try:
                    result = future.result()
                    completed_results[idx] = result
                except Exception as e:
                    email = user_data.get("email", "unknown")
                    logger.error(f"[LAMBDA] [THREAD] Future exception for user {idx + 1}: {email} - {str(e)}")
                    completed_results[idx] = {
                        "email": email,
                        "status": "failed",
                        "error_message": f"Future exception: {str(e)}",
                        "app_password": None,
                        "secret_key": None
                    }
            
            # Reconstruct results in original order
            results = [completed_results[idx] for idx in sorted(completed_results.keys())]
        
        logger.info(f"[LAMBDA] All {len(users_batch)} users processed in parallel")
        
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
