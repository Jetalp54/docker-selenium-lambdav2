import os
import json
import time
import logging
import traceback

import boto3
import paramiko
import pyotp

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =====================================================================
# Helpers: Selenium driver in Lambda
# =====================================================================


def get_chrome_driver():
    """
    Create a headless Chrome driver inside the umihico/aws-lambda-selenium-python image.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1280,800")
    chrome_options.add_argument("--lang=en-US")

    driver = webdriver.Chrome(options=chrome_options)
    return driver


def wait_for_xpath(driver, xpath, timeout=20):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, xpath))
    )


def click_xpath(driver, xpath, timeout=20):
    el = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, xpath))
    )
    el.click()
    return el


def element_exists(driver, xpath, timeout=5):
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        return True
    except TimeoutException:
        return False


# =====================================================================
# Helpers: SFTP for secret key (server storage)
# =====================================================================


def get_sftp_params():
    """
    SFTP parameters must be passed via environment variables:

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
    params = get_sftp_params()
    if not params:
        logger.warning(
            "SFTP parameters not fully configured (SECRET_SFTP_HOST / SECRET_SFTP_USER)."
        )
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
            pkey = paramiko.RSAKey.from_private_key(
                io.StringIO(key_content)
            )
            transport.connect(username=user, pkey=pkey)
        else:
            # Use password
            transport.connect(username=user, password=password)

        sftp = paramiko.SFTPClient.from_transport(transport)
        logger.info(f"SFTP connected to {host}:{port} as {user}")
        return sftp, params["remote_dir"]
    except Exception as e:
        logger.error(f"Failed to connect via SFTP: {e}")
        return None, None


def ensure_remote_dir(sftp, remote_dir):
    """
    Ensure the remote directory exists (create it if necessary).
    """
    parts = remote_dir.strip("/").split("/")
    path = ""
    for part in parts:
        path = path + "/" + part if path else "/" + part
        try:
            sftp.listdir(path)
        except IOError:
            sftp.mkdir(path)


def save_secret_key_to_server(email, secret_key):
    """
    Save the secret key for the account on the remote server via SFTP.
    Creates one file per email: <alias>_totp_secret.txt
    """
    if not secret_key:
        logger.warning("No secret key to save for server.")
        return False

    sftp, remote_dir = sftp_connect_from_env()
    if not sftp:
        logger.warning("Skipping server secret save due to missing SFTP connection.")
        return False

    try:
        ensure_remote_dir(sftp, remote_dir)
        alias = email.split("@")[0]
        remote_path = f"{remote_dir}/{alias}_totp_secret.txt"
        with sftp.open(remote_path, "w") as f:
            f.write(secret_key.strip() + "\n")
        logger.info(f"Secret key saved to server at {remote_path} for {email}")
        sftp.close()
        return True
    except Exception as e:
        logger.error(f"Failed to save secret key to server for {email}: {e}")
        return False


# =====================================================================
# Helpers: S3 global app_passwords.txt
# =====================================================================


def append_app_password_to_s3(email, app_password):
    """
    Maintain *one* text file on S3 that holds all app passwords.

      s3://APP_PASSWORDS_S3_BUCKET/APP_PASSWORDS_S3_KEY

    Format:
      email:password\n
    New entries overwrite existing entries for that email.
    """
    bucket = os.environ.get("APP_PASSWORDS_S3_BUCKET")
    key = os.environ.get("APP_PASSWORDS_S3_KEY", "app_passwords.txt")

    if not bucket:
        logger.warning(
            "APP_PASSWORDS_S3_BUCKET is not set. Skipping S3 app password storage."
        )
        return False, None, None

    s3 = boto3.client("s3")

    existing_body = ""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        existing_body = obj["Body"].read().decode("utf-8")
    except s3.exceptions.NoSuchKey:
        existing_body = ""
    except Exception as e:
        logger.warning(f"Could not read existing S3 app_passwords.txt: {e}")

    # Parse existing lines
    entries = {}
    if existing_body:
        for line in existing_body.splitlines():
            if ":" in line:
                e, p = line.split(":", 1)
                e = e.strip()
                p = p.strip()
                if e and p:
                    entries[e] = p

    # Update this email
    entries[email] = app_password.strip()

    new_body = "".join(f"{e}:{p}\n" for e, p in entries.items())

    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=new_body.encode("utf-8"),
            ContentType="text/plain",
        )
        logger.info(
            f"App password updated in global S3 file for {email}: s3://{bucket}/{key}"
        )
        return True, bucket, key
    except Exception as e:
        logger.error(f"Failed to write app_passwords.txt to S3: {e}")
        return False, bucket, key


# =====================================================================
# Step 1: Login + optional existing 2FA handling
# =====================================================================


def login_google(driver, email, password, known_totp_secret=None):
    """
    Login to Google. If a 2FA code is requested and we know a TOTP secret,
    we will try to solve it; otherwise we fail with an explicit error.
    """
    logger.info(f"[STEP login] Navigating to Google login for {email}")
    driver.get("https://accounts.google.com/signin/v2/identifier?hl=en&flowName=GlifWebSignIn")
    time.sleep(1)

    try:
        # Enter email
        email_input = wait_for_xpath(driver, "//input[@id='identifierId']")
        email_input.clear()
        email_input.send_keys(email)
        email_next = wait_for_xpath(driver, "//*[@id='identifierNext']")
        email_next.click()
        logger.info("[STEP login] Email submitted")

        # Enter password
        password_input = wait_for_xpath(driver, "//input[@name='Passwd']")
        time.sleep(1)
        password_input.send_keys(password)
        pw_next = wait_for_xpath(driver, "//*[@id='passwordNext']")
        pw_next.click()
        logger.info("[STEP login] Password submitted")

        # Wait a bit for potential 2FA challenge or account home
        time.sleep(3)

        current_url = driver.current_url
        logger.info(f"[STEP login] URL after login attempt: {current_url}")

        # If we're on MyAccount or Gmail, we are probably logged in
        if "myaccount.google.com" in current_url or "mail.google.com" in current_url:
            logger.info("[STEP login] Login success without visible 2FA step")
            return True, None, None

        # Check for 2FA challenge (TOTP)
        # This is simplified — real flows can be more complex.
        if "challenge" in current_url or "signin/challenge" in current_url:
            logger.info("[STEP login] 2FA challenge detected")
            if not known_totp_secret:
                logger.error(
                    "[STEP login] 2FA is required but no TOTP secret is available"
                )
                return False, "2FA_REQUIRED", "2FA required but secret is unknown"

            # Generate TOTP code
            totp = pyotp.TOTP(known_totp_secret.replace(" ", ""))
            otp_code = totp.now()
            logger.info(f"[STEP login] Generated TOTP code for challenge: {otp_code}")

            # Try to fill OTP input
            try:
                otp_input = wait_for_xpath(
                    driver,
                    "//input[@type='tel' or @type='text' and @autocomplete='one-time-code']",
                    timeout=20,
                )
                otp_input.send_keys(otp_code)
                submit_btn_xpath = "//button[contains(@type,'submit') or @role='button']"
                if element_exists(driver, submit_btn_xpath, timeout=5):
                    click_xpath(driver, submit_btn_xpath)
                time.sleep(3)
            except Exception as e:
                logger.error(f"[STEP login] Failed to submit 2FA code: {e}")
                return False, "2FA_SUBMIT_FAILED", str(e)

            # Check if we reached account home
            time.sleep(2)
            current_url = driver.current_url
            if "myaccount.google.com" in current_url or "mail.google.com" in current_url:
                logger.info("[STEP login] Login success after 2FA")
                return True, None, None
            else:
                logger.error(
                    f"[STEP login] 2FA submitted but not redirected to account. URL={current_url}"
                )
                return (
                    False,
                    "2FA_UNEXPECTED_FLOW",
                    f"After 2FA code, got URL={current_url}",
                )

        # No explicit 2FA page detected, but also not clearly logged in
        logger.error(
            f"[STEP login] Unknown login state, URL={current_url}. Manual check needed."
        )
        return False, "LOGIN_UNKNOWN_STATE", f"URL={current_url}"

    except TimeoutException as e:
        logger.error(f"[STEP login] Timeout during login: {e}")
        return False, "LOGIN_TIMEOUT", str(e)
    except Exception as e:
        logger.error(f"[STEP login] Unexpected error during login: {e}")
        return False, "LOGIN_EXCEPTION", str(e)


# =====================================================================
# Step 2: Navigate to security / authenticator / 2-step pages
# =====================================================================


def navigate_to_security(driver):
    try:
        logger.info("[STEP nav] Opening MyAccount Security page")
        driver.get("https://myaccount.google.com/security?hl=en")
        time.sleep(2)
        return True, None, None
    except Exception as e:
        logger.error(f"[STEP nav] Failed to open security page: {e}")
        return False, "NAV_SECURITY_FAILED", str(e)


# =====================================================================
# Step 3: Setup Authenticator app + capture secret
# =====================================================================


def setup_authenticator_app(driver, email):
    """
    Go to the Authenticator app setup page, start the flow,
    extract the secret key shown, confirm using TOTP,
    and return the secret.
    """
    try:
        logger.info("[STEP auth] Navigating to Authenticator app setup")
        driver.get(
            "https://myaccount.google.com/two-step-verification/authenticator?hl=en"
        )
        time.sleep(3)

        # If already set up, Google might show a 'Remove' or 'Change' option
        already_xpath = "//*[contains(text(),'Authenticator app') and (contains(text(),'On') or contains(text(),'Set up'))]"
        if element_exists(driver, already_xpath, timeout=5):
            logger.info(
                "[STEP auth] Authenticator app page loaded (we will attempt setup if needed)"
            )

        # Many flows: typically a "Set up" / "Get started" button
        for setup_xpath in [
            "//button//*[contains(text(),'Set up')]/ancestor::button",
            "//button//*[contains(text(),'Get started')]/ancestor::button",
            "//button//*[contains(text(),'Continue')]/ancestor::button",
        ]:
            if element_exists(driver, setup_xpath, timeout=5):
                click_xpath(driver, setup_xpath, timeout=5)
                time.sleep(3)
                break

        # Some pages ask "What kind of phone do you have?" – just click next
        if element_exists(driver, "//button//*[contains(text(),'Next')]/ancestor::button", 5):
            click_xpath(driver, "//button//*[contains(text(),'Next')]/ancestor::button", 5)
            time.sleep(3)

        # Extract secret key text – simple heuristic with your-style logic:
        secret_candidates_xpaths = [
            "//span[contains(text(), 'Key')]/following-sibling::span",
            "//span[contains(text(), 'Secret key')]/following-sibling::span",
            "//code[contains(text(), '-') or string-length(text()) >= 16]",
            "//span[contains(@class, 'secret-key')]",
        ]
        secret_key = None
        for sx in secret_candidates_xpaths:
            try:
                el = wait_for_xpath(driver, sx, timeout=5)
                text = el.text.strip()
                text = text.replace(" ", "")
                if 10 <= len(text) <= 64:
                    secret_key = text
                    break
            except TimeoutException:
                continue

        if not secret_key:
            logger.error("[STEP auth] Could not extract secret key from page")
            return False, None, "AUTH_SECRET_NOT_FOUND", "No secret key detected on page"

        logger.info(f"[STEP auth] Extracted secret key: {secret_key}")

        # Save secret key to your server
        save_secret_key_to_server(email, secret_key)

        # Now confirm with TOTP code (Google asks you to prove you added the app)
        totp = pyotp.TOTP(secret_key.replace(" ", ""))
        code = totp.now()
        logger.info(f"[STEP auth] Generated TOTP code for confirmation: {code}")

        otp_input_xpath = "//input[@type='tel' or @type='text' and @autocomplete='one-time-code']"
        try:
            otp_input = wait_for_xpath(driver, otp_input_xpath, timeout=20)
            otp_input.send_keys(code)
            time.sleep(1)
            if element_exists(
                driver,
                "//button//*[contains(text(),'Next') or contains(text(),'Verify')]/ancestor::button",
                timeout=5,
            ):
                click_xpath(
                    driver,
                    "//button//*[contains(text(),'Next') or contains(text(),'Verify')]/ancestor::button",
                    timeout=5,
                )
            time.sleep(3)
        except Exception as e:
            logger.error(f"[STEP auth] Failed to submit authenticator TOTP: {e}")
            return False, None, "AUTH_TOTP_SUBMIT_FAILED", str(e)

        logger.info("[STEP auth] Authenticator app setup flow completed (assumed)")
        return True, secret_key, None, None

    except Exception as e:
        logger.error(f"[STEP auth] Unexpected error: {e}")
        return False, None, "AUTH_EXCEPTION", str(e)


# =====================================================================
# Step 4: Ensure 2-step verification is enabled
# =====================================================================


def ensure_two_step_enabled(driver, email):
    """
    Open the 2-step verification page and enable it if needed.
    """
    try:
        logger.info("[STEP 2SV] Navigating to 2-Step Verification page")
        driver.get("https://myaccount.google.com/signinoptions/twosv?hl=en")
        time.sleep(3)

        # This is highly simplified: in reality, more flows may appear.
        on_xpath = "//*[contains(text(),'2-Step Verification') and contains(text(),'On')]"
        off_xpath = "//*[contains(text(),'2-Step Verification') and contains(text(),'Off')]"

        if element_exists(driver, on_xpath, timeout=5):
            logger.info("[STEP 2SV] 2-Step Verification already enabled")
            return True, None, None

        logger.info("[STEP 2SV] 2-Step is not clearly 'On', attempting to enable...")

        for button_xpath in [
            "//button//*[contains(text(),'Get started')]/ancestor::button",
            "//button//*[contains(text(),'Turn on')]/ancestor::button",
            "//button//*[contains(text(),'Continue')]/ancestor::button",
        ]:
            if element_exists(driver, button_xpath, timeout=5):
                click_xpath(driver, button_xpath, timeout=5)
                time.sleep(3)
                break

        # After this, Google may ask for password or 2FA; we assume previous auth covers this.
        time.sleep(5)

        if element_exists(driver, on_xpath, timeout=10):
            logger.info("[STEP 2SV] 2-Step Verification is now enabled")
            return True, None, None

        logger.error("[STEP 2SV] Could not confirm 2-Step is enabled")
        return False, "TWO_STEP_ENABLE_FAILED", "Unable to confirm 2-Step 'On' state"
    except Exception as e:
        logger.error(f"[STEP 2SV] Exception while enabling 2-Step: {e}")
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
        logger.info("[STEP app_pw] Opening App Passwords page")
        driver.get("https://myaccount.google.com/apppasswords?hl=en")
        time.sleep(5)

        # Sometimes there is an "Select app" dropdown, sometimes Google opens direct.
        # We'll try to click "Select app" and choose "Mail" or "Other".
        try:
            if element_exists(
                driver,
                "//div[contains(text(),'Select app') or contains(text(),'Select an app')]",
                timeout=5,
            ):
                click_xpath(
                    driver,
                    "//div[contains(text(),'Select app') or contains(text(),'Select an app')]",
                    timeout=5,
                )
                time.sleep(1)
                # Choose "Mail" if available, else "Other"
                if element_exists(
                    driver, "//span[contains(text(),'Mail')]", timeout=3
                ):
                    click_xpath(driver, "//span[contains(text(),'Mail')]", timeout=3)
                elif element_exists(
                    driver, "//span[contains(text(),'Other')]", timeout=3
                ):
                    click_xpath(driver, "//span[contains(text(),'Other')]", timeout=3)
                    # Enter label if "Other"
                    if element_exists(driver, "//input[@type='text']", timeout=3):
                        label_input = wait_for_xpath(
                            driver, "//input[@type='text']", timeout=3
                        )
                        label_input.send_keys("SMTP")
                time.sleep(1)

            # Click GENERATE / NEXT / DONE
            for gen_xpath in [
                "//button//*[contains(text(),'Generate')]/ancestor::button",
                "//button//*[contains(text(),'Next')]/ancestor::button",
            ]:
                if element_exists(driver, gen_xpath, timeout=5):
                    click_xpath(driver, gen_xpath, timeout=5)
                    time.sleep(3)
                    break
        except Exception as e:
            logger.warning(
                f"[STEP app_pw] Could not interact with app selection flow: {e}"
            )

        # App password is usually shown as 4 chunks with dashes
        candidates = [
            "//code[contains(text(), '-') and string-length(text()) >= 16]",
            "//span[contains(text(), '-') and string-length(text()) >= 16]",
            "//div[contains(text(), '-') and string-length(text()) >= 16]",
        ]
        password = None
        for xp in candidates:
            try:
                el = wait_for_xpath(driver, xp, timeout=10)
                text = el.text.strip()
                text = text.replace(" ", "")
                if "-" in text and len(text) >= 16:
                    password = text
                    break
            except TimeoutException:
                continue

        if not password:
            logger.error("[STEP app_pw] Could not extract app password from page")
            return False, None, "APP_PASSWORD_NOT_FOUND", "No app password text detected"

        logger.info(f"[STEP app_pw] Extracted app password for {email}: {password}")
        return True, password, None, None

    except Exception as e:
        logger.error(f"[STEP app_pw] Exception generating app password: {e}")
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
      s3_bucket (str or None),
      s3_key (str or None)
    """
    step = "login"
    s3_bucket = None
    s3_key = None

    # 1. LOGIN
    ok, err_type, err_msg = login_google(driver, email, password, known_totp_secret)
    if not ok:
        return False, step, err_type, err_msg, s3_bucket, s3_key

    # 2. NAVIGATE SECURITY
    step = "navigation"
    ok, err_type, err_msg = navigate_to_security(driver)
    if not ok:
        return False, step, err_type, err_msg, s3_bucket, s3_key

    # 3. AUTHENTICATOR APP
    step = "authenticator_setup"
    ok, secret_key, err_type, err_msg = setup_authenticator_app(driver, email)
    if not ok:
        return False, step, err_type, err_msg, s3_bucket, s3_key

    # 4. 2-STEP
    step = "2step_verification"
    ok, err_type, err_msg = ensure_two_step_enabled(driver, email)
    if not ok:
        return False, step, err_type, err_msg, s3_bucket, s3_key

    # 5. APP PASSWORD
    step = "app_password"
    ok, app_password, err_type, err_msg = generate_app_password(driver, email)
    if not ok:
        return False, step, err_type, err_msg, s3_bucket, s3_key

    # Save to S3 global file
    success_s3, s3_bucket, s3_key = append_app_password_to_s3(email, app_password)
    if not success_s3:
        return False, step, "S3_SAVE_FAILED", "App password generated but S3 save failed"

    return True, step, None, None, s3_bucket, s3_key


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

        // optional: if you already know the TOTP secret (when 2FA is already active)
        "known_totp_secret": "BASE32SECRET"
      }

    You can also provide these via environment variables:
      GW_EMAIL, GW_PASSWORD, KNOWN_TOTP_SECRET

    Returns JSON with:
      status          = "ok" | "error"
      email
      step_completed  = "login" | "navigation" | "authenticator_setup" | "2step_verification" | "app_password"
      error_type
      error_message
      app_passwords_s3_bucket
      app_passwords_s3_key
    """

    logger.info(f"[LAMBDA] Received event: {json.dumps(event)}")

    email = event.get("email") or os.environ.get("GW_EMAIL")
    password = event.get("password") or os.environ.get("GW_PASSWORD")
    known_totp_secret = event.get("known_totp_secret") or os.environ.get(
        "KNOWN_TOTP_SECRET"
    )

    if not email or not password:
        msg = "email and password must be provided via event or env (GW_EMAIL / GW_PASSWORD)."
        logger.error(msg)
        return {
            "status": "error",
            "email": email,
            "step_completed": "init",
            "error_type": "MISSING_CREDENTIALS",
            "error_message": msg,
            "app_passwords_s3_bucket": None,
            "app_passwords_s3_key": None,
        }

    driver = None
    step_completed = "init"
    error_type = None
    error_message = None
    s3_bucket = None
    s3_key = None

    try:
        driver = get_chrome_driver()
        logger.info(f"[LAMBDA] Chrome driver started for {email}")

        success, step_completed, error_type, error_message, s3_bucket, s3_key = (
            process_account(driver, email, password, known_totp_secret)
        )

        status = "ok" if success else "error"

        return {
            "status": status,
            "email": email,
            "step_completed": step_completed,
            "error_type": error_type,
            "error_message": error_message,
            "app_passwords_s3_bucket": s3_bucket,
            "app_passwords_s3_key": s3_key,
        }

    except Exception as e:
        logger.error(f"[LAMBDA] Unhandled exception: {e}")
        logger.error(traceback.format_exc())
        return {
            "status": "error",
            "email": email,
            "step_completed": step_completed,
            "error_type": "UNHANDLED_EXCEPTION",
            "error_message": str(e),
            "app_passwords_s3_bucket": s3_bucket,
            "app_passwords_s3_key": s3_key,
        }
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
