import os
import time
import json
import logging
import traceback
import base64
import struct
import hmac
import hashlib

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


# ============================================================
# Logging
# ============================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ============================================================
# TOTP helper (same logic as your worker Lambda)
# ============================================================

def get_totp_token(secret: str, interval: int = 30, digits: int = 6) -> str:
    """
    RFC 6238 TOTP generator.
    TOTP_SECRET is expected to be base32; spaces are ignored.
    """
    clean_secret = secret.replace(" ", "").upper()
    key = base64.b32decode(clean_secret)
    counter = int(time.time()) // interval
    msg = struct.pack(">Q", counter)
    h = hmac.new(key, msg, hashlib.sha1).digest()
    offset = h[19] & 0x0F
    code = (struct.unpack(">I", h[offset:offset + 4])[0] & 0x7FFFFFFF) % (10 ** digits)
    return f"{code:0{digits}d}"


# ============================================================
# Selenium helpers
# ============================================================

def create_driver() -> webdriver.Chrome:
    """
    Create Chrome driver using umihico/aws-lambda-selenium-python base image.
    """
    options = webdriver.ChromeOptions()
    # These flags are usually already configured but are safe to repeat
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,720")

    driver = webdriver.Chrome(options=options)
    return driver


def wait_and_type(driver, by, locator, text, timeout=30):
    el = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((by, locator))
    )
    el.clear()
    el.send_keys(text)
    return el


def wait_and_click(driver, by, locator, timeout=30):
    el = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((by, locator))
    )
    el.click()
    return el


# ============================================================
# Core Google Workspace login + TOTP flow
# ============================================================

def login_google_workspace_with_totp(driver, username, password, totp_secret, target_url):
    """
    1) Go to target_url (e.g. https://accounts.google.com/)
    2) Enter username
    3) Enter password
    4) Enter TOTP (Authenticator app) code
    5) Return final URL & page title + full log
    """
    log_events = []

    def log(msg: str):
        logger.info(msg)
        log_events.append(msg)

    url = target_url or "https://accounts.google.com/"
    log(f"[STEP] Opening URL: {url}")
    driver.get(url)

    # ----------------- Step 1: Email -----------------
    log("[STEP] Typing email...")
    try:
        # Default email field
        wait_and_type(driver, By.ID, "identifierId", username, timeout=40)
        wait_and_click(driver, By.ID, "identifierNext", timeout=40)
    except TimeoutException:
        # Fallback selectors if ID changed
        log("[WARN] identifierId not found, trying generic email selector...")
        wait_and_type(driver, By.CSS_SELECTOR, "input[type='email']", username, timeout=40)
        wait_and_click(driver, By.CSS_SELECTOR, "#identifierNext button, #identifierNext", timeout=40)

    # ----------------- Step 2: Password -----------------
    log("[STEP] Typing password...")
    try:
        wait_and_type(driver, By.NAME, "Passwd", password, timeout=40)
        wait_and_click(driver, By.ID, "passwordNext", timeout=40)
    except TimeoutException:
        log("[WARN] Passwd field not found by NAME, trying password CSS selector...")
        wait_and_type(driver, By.CSS_SELECTOR, "input[type='password']", password, timeout=40)
        wait_and_click(driver, By.CSS_SELECTOR, "#passwordNext button, #passwordNext", timeout=40)

    # ----------------- Step 3: TOTP -----------------
    log("[STEP] Waiting for TOTP challenge (Authenticator app code)...")
    time.sleep(3)  # small pause while Google renders 2FA page

    totp_code = get_totp_token(totp_secret)
    log(f"[INFO] Generated TOTP code: {totp_code} (only used inside this container run)")

    totp_filled = False

    possible_totp_selectors = [
        (By.ID, "totpPin"),
        (By.NAME, "totpPin"),
        (By.CSS_SELECTOR, "input[type='tel']"),
        (By.CSS_SELECTOR, "input[autocomplete='one-time-code']"),
    ]

    for by, sel in possible_totp_selectors:
        try:
            log(f"[TRY] Looking for TOTP input via {by} = {sel} ...")
            el = WebDriverWait(driver, 20).until(
                EC.visibility_of_element_located((by, sel))
            )
            el.clear()
            el.send_keys(totp_code)
            totp_filled = True
            log("[STEP] TOTP code typed successfully.")
            break
        except TimeoutException:
            log(f"[INFO] TOTP input not found with {by} = {sel}, trying next...")

    if not totp_filled:
        log("[WARN] Could not find TOTP input field. Google may have chosen a different 2FA challenge (phone prompt / SMS).")

    # Try to click "Next" or confirm button after TOTP
    if totp_filled:
        possible_next_selectors = [
            (By.ID, "totpNext"),
            (By.CSS_SELECTOR, "#totpNext button"),
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.CSS_SELECTOR, "div[role='button'] span span"),
        ]
        for by, sel in possible_next_selectors:
            try:
                log(f"[TRY] Clicking TOTP submit via {by} = {sel} ...")
                wait_and_click(driver, by, sel, timeout=20)
                log("[STEP] TOTP submit clicked.")
                break
            except TimeoutException:
                log(f"[INFO] TOTP submit not found with {by} = {sel}, trying next...")

    # ----------------- Step 4: Landing page -----------------
    log("[STEP] Waiting a few seconds for final redirect...")
    time.sleep(5)

    current_url = driver.current_url
    title = driver.title

    log(f"[RESULT] Current URL: {current_url}")
    log(f"[RESULT] Page title: {title}")

    return {
        "current_url": current_url,
        "title": title,
        "log": log_events,
    }


# ============================================================
# Lambda handler (used by CMD ["main.handler"])
# ============================================================

def handler(event, context):
    """
    AWS Lambda entrypoint for the container image.

    Reads environment variables:
      GW_USERNAME
      GW_PASSWORD
      GW_TOTP_SECRET
      TARGET_URL  (optional, default https://accounts.google.com/)

    Returns:
      {
        "statusCode": 200/500,
        "body": JSON-string with:
          {
            "status": "ok" | "error",
            "current_url": "...",
            "title": "...",
            "log": ["...", "..."],
            "elapsed_seconds": int
          }
      }
    """
    start_ts = int(time.time())

    username = os.environ.get("GW_USERNAME", "")
    password = os.environ.get("GW_PASSWORD", "")
    totp_secret = os.environ.get("GW_TOTP_SECRET", "")
    target_url = os.environ.get("TARGET_URL", "https://accounts.google.com/")

    missing = []
    if not username:
        missing.append("GW_USERNAME")
    if not password:
        missing.append("GW_PASSWORD")
    if not totp_secret:
        missing.append("GW_TOTP_SECRET")

    if missing:
        msg = f"Missing required environment variables: {', '.join(missing)}"
        logger.error(msg)
        body = {"status": "error", "message": msg}
        return {
            "statusCode": 500,
            "body": json.dumps(body)
        }

    driver = None
    try:
        logger.info("[LAMBDA] Creating Selenium driver...")
        driver = create_driver()

        result = login_google_workspace_with_totp(
            driver=driver,
            username=username,
            password=password,
            totp_secret=totp_secret,
            target_url=target_url,
        )

        elapsed = int(time.time()) - start_ts
        body = {
            "status": "ok",
            "current_url": result.get("current_url"),
            "title": result.get("title"),
            "log": result.get("log", []),
            "elapsed_seconds": elapsed,
        }

        logger.info(f"[LAMBDA] Finished in {elapsed}s, returning result.")
        return {
            "statusCode": 200,
            "body": json.dumps(body)
        }

    except Exception as e:
        logger.error("[LAMBDA] Exception during login flow:")
        logger.error(traceback.format_exc())
        body = {
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc(),
        }
        return {
            "statusCode": 500,
            "body": json.dumps(body)
        }
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                logger.warning("Failed to quit driver cleanly.")


# Optional: local test
if __name__ == "__main__":
    # For local debugging only (will use local env variables)
    print(handler({}, None))
