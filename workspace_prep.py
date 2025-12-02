import os
import json
import time
import random
import logging
import subprocess
import traceback
import boto3
import re

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET_NAME = os.environ.get("PREP_S3_BUCKET_NAME", "edu-gw-service-accounts")


# ----------------------------------------------------------
# Chrome Setup (Headless Mode + Anti-Detection)
# ----------------------------------------------------------
def get_driver():
    logger.info("[CHROME] Starting headless undetected-chromedriver...")

    options = uc.ChromeOptions()
    options.headless = True

    # Anti-detection flags
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--lang=en-US")
    options.add_argument("--window-size=1280,800")

    # Random user-agent
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    ]
    ua = random.choice(user_agents)
    options.add_argument(f"--user-agent={ua}")

    logger.info(f"[CHROME] Using UA: {ua}")

    driver = uc.Chrome(
        options=options,
        driver_executable_path="/usr/bin/chromedriver",
        browser_executable_path="/usr/bin/google-chrome",
        version_main=None
    )

    driver.set_page_load_timeout(60)
    return driver


# ----------------------------------------------------------
# Google Login (Headless Safe Version)
# ----------------------------------------------------------
def login_google(driver, email, password):
    try:
        logger.info(f"[LOGIN] Navigating to Google Login for {email}...")
        driver.get(
            "https://accounts.google.com/signin/v2/identifier"
            "?hl=en&flowName=GlifWebSignIn&flowEntry=ServiceLogin"
        )
        time.sleep(3)

        # Enter email
        logger.info("[LOGIN] Entering email...")
        driver.find_element(By.ID, "identifierId").send_keys(email)
        driver.find_element(By.ID, "identifierNext").click()
        time.sleep(3)

        # Enter password
        logger.info("[LOGIN] Entering password...")
        pwd_field = driver.find_element(By.NAME, "password")
        pwd_field.send_keys(password)
        driver.find_element(By.ID, "passwordNext").click()
        time.sleep(5)

        # Check login success
        if "chrome" in driver.title.lower() or "myaccount" in driver.current_url:
            logger.info("[LOGIN] Login SUCCESS")
            return True, None

        logger.warning("[LOGIN] Login might have issues")
        return True, None

    except Exception as e:
        logger.error(f"[LOGIN] FAILED: {e}")
        return False, str(e)


# ----------------------------------------------------------
# gcloud OAuth Login using Selenium OOB Code
# ----------------------------------------------------------
def gcloud_auth(driver):
    logger.info("[GCLOUD] Starting OAuth login...")

    process = subprocess.Popen(
        ["gcloud", "auth", "login", "--no-launch-browser"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE,
        text=True,
        bufsize=1
    )

    auth_url = None
    t0 = time.time()

    # Extract URL from gcloud output
    while time.time() - t0 < 20:
        line = process.stdout.readline()
        if line:
            logger.info("[GCLOUD] " + line.strip())

        match = re.search(r"(https://accounts\.google\.com/[\w\-\.\~:/\?\#\[\]@\!\$&'\(\)\*\+,;=%]+)", line)
        if match:
            auth_url = match.group(1)
            break

    if not auth_url:
        process.kill()
        return False, "Failed to extract OAuth URL from gcloud"

    # Visit the URL
    driver.get(auth_url)
    time.sleep(4)

    # Approve Google consent page (if needed)
    try:
        btns = driver.find_elements(By.XPATH, "//button[contains(., 'Allow')]")
        if btns:
            btns[0].click()
            time.sleep(3)
    except Exception:
        pass

    # Extract OOB code
    try:
        code_el = driver.find_element(By.TAG_NAME, "textarea")
        code = code_el.get_attribute("value").strip()
    except:
        process.kill()
        return False, "Failed to read OAuth code"

    logger.info(f"[GCLOUD] Received OAuth code: {code[:10]}...")

    process.stdin.write(code + "\n")
    process.stdin.flush()

    process.wait(timeout=60)

    if process.returncode == 0:
        return True, None

    return False, f"gcloud auth exited with code {process.returncode}"


# ----------------------------------------------------------
# Create Project, Service Account & Key
# ----------------------------------------------------------
def create_gcloud_resources():
    project_id = f"prep-{int(time.time())}-{random.randint(1000,9999)}"
    sa_name = "prep-sa"
    sa_email = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
    key_path = f"/tmp/{project_id}-key.json"

    try:
        subprocess.check_call(["gcloud", "projects", "create", project_id, "--quiet"])
        subprocess.check_call(["gcloud", "iam", "service-accounts", "create", sa_name,
                               "--project", project_id, "--quiet"])
        subprocess.check_call([
            "gcloud", "iam", "service-accounts", "keys", "create", key_path,
            "--iam-account", sa_email,
            "--project", project_id,
            "--quiet"
        ])

        with open(key_path, "r") as f:
            return True, project_id, f.read()

    except Exception as e:
        return False, None, str(e)


# ----------------------------------------------------------
# MAIN PROCESS HANDLER
# ----------------------------------------------------------
def process_user(user):
    email = user["email"]
    password = user["password"]

    driver = None
    try:
        driver = get_driver()

        ok, err = login_google(driver, email, password)
        if not ok:
            return {"email": email, "success": False, "error": err}

        ok, err = gcloud_auth(driver)
        if not ok:
            return {"email": email, "success": False, "error": err}

        ok, project_id, key_content = create_gcloud_resources()
        if not ok:
            return {"email": email, "success": False, "error": key_content}

        # Upload to S3
        s3 = boto3.client("s3")
        s3_key = f"service-accounts/{email}/{project_id}.json"

        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=key_content,
            ContentType="application/json"
        )

        return {
            "email": email,
            "success": True,
            "project_id": project_id,
            "s3_key": s3_key
        }

    except Exception as e:
        return {"email": email, "success": False, "error": str(e)}

    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass


# ----------------------------------------------------------
# LAMBDA HANDLER
# ----------------------------------------------------------
def handler(event, context):
    logger.info("[PREP] Handler invoked")
    logger.info(json.dumps(event))

    users = event.get("users", [])
    if not users:
        return {"error": "no users provided"}

    results = []
    for user in users:
        results.append(process_user(user))

    return {"results": results}
