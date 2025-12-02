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
import boto3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

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

def get_chrome_driver():
    """Initialize Chrome Driver (Headless)"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1280x1696")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--disable-application-cache")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--hide-scrollbars")
    chrome_options.add_argument("--enable-logging")
    chrome_options.add_argument("--log-level=0")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--homedir=/tmp")
    
    # Binary location (from base image)
    # Try common locations
    if os.path.exists("/opt/chrome/chrome"):
        chrome_options.binary_location = "/opt/chrome/chrome"
    elif os.path.exists("/usr/bin/google-chrome"):
        chrome_options.binary_location = "/usr/bin/google-chrome"

    service = Service("/usr/bin/chromedriver") # Default for base image
    
    driver = webdriver.Chrome(options=chrome_options)
    return driver

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
