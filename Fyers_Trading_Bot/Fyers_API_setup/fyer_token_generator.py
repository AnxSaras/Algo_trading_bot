from dotenv import load_dotenv
import os
import hashlib
import json
import requests
import pyotp
from urllib import parse
import sys

load_dotenv()

# Load environment variables
FY_ID = os.getenv("FY_ID")
APP_ID_TYPE = os.getenv("APP_ID_TYPE")
TOTP_KEY = os.getenv("TOTP_KEY")
PIN = os.getenv("PIN")
APP_ID = os.getenv("APP_ID")
REDIRECT_URI = os.getenv("REDIRECT_URI")
APP_TYPE = os.getenv("APP_TYPE")
APP_SECRET = os.getenv("APP_SECRET")

# Generate app_id_hash
a_string = f"{APP_ID}-{APP_TYPE}:{APP_SECRET}"
APP_ID_HASH = hashlib.sha256(a_string.encode('utf-8')).hexdigest()
print(APP_ID_HASH)

# API endpoints
BASE_URL = "https://api-t2.fyers.in/vagator/v2"
BASE_URL_2 = "https://api-t1.fyers.in/api/v3"
URL_SEND_LOGIN_OTP = BASE_URL + "/send_login_otp"
URL_VERIFY_TOTP = BASE_URL + "/verify_otp"
URL_VERIFY_PIN = BASE_URL + "/verify_pin"
URL_TOKEN = BASE_URL_2 + "/token"
URL_VALIDATE_AUTH_CODE = BASE_URL_2 + "/validate-authcode"

SUCCESS = 1
ERROR = -1

def send_login_otp(fy_id, app_id):
    try:
        payload = {"fy_id": fy_id, "app_id": app_id}
        result_string = requests.post(url=URL_SEND_LOGIN_OTP, json=payload)
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        return [SUCCESS, result["request_key"]]
    except Exception as e:
        return [ERROR, e]

def generate_totp(secret):
    try:
        return [SUCCESS, pyotp.TOTP(secret).now()]
    except Exception as e:
        return [ERROR, e]

def verify_totp(request_key, totp):
    try:
        payload = {"request_key": request_key, "otp": totp}
        result_string = requests.post(url=URL_VERIFY_TOTP, json=payload)
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        return [SUCCESS, result["request_key"]]
    except Exception as e:
        return [ERROR, e]

def verify_PIN(request_key, pin):
    try:
        payload = {"request_key": request_key, "identity_type": "pin", "identifier": pin}
        result_string = requests.post(url=URL_VERIFY_PIN, json=payload)
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        return [SUCCESS, result["data"]["access_token"]]
    except Exception as e:
        return [ERROR, e]

def token(fy_id, app_id, redirect_uri, app_type, access_token):
    try:
        payload = {
            "fyers_id": fy_id,
            "app_id": app_id,
            "redirect_uri": redirect_uri,
            "appType": app_type,
            "code_challenge": "",
            "state": "sample_state",
            "scope": "",
            "nonce": "",
            "response_type": "code",
            "create_cookie": True
        }
        headers = {'Authorization': f'Bearer {access_token}'}
        result_string = requests.post(url=URL_TOKEN, json=payload, headers=headers)
        if result_string.status_code != 308:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        auth_code = parse.parse_qs(parse.urlparse(result["Url"]).query)['auth_code'][0]
        return [SUCCESS, auth_code]
    except Exception as e:
        return [ERROR, e]

def validate_authcode(app_id_hash, auth_code):
    try:
        payload = {
            "grant_type": "authorization_code",
            "appIdHash": app_id_hash,
            "code": auth_code,
        }
        result_string = requests.post(url=URL_VALIDATE_AUTH_CODE, json=payload)
        if result_string.status_code != 200:
            return [ERROR, result_string.text]
        result = json.loads(result_string.text)
        return [SUCCESS, result["access_token"]]
    except Exception as e:
        return [ERROR, e]

def update_env_file(key, value):
    """Update or add a key-value pair in the .env file."""
    env_file = ".env"
    new_line = f"{key}={value}"
    
    # Read existing .env content
    if os.path.exists(env_file):
        with open(env_file, 'r') as file:
            lines = file.readlines()
        
        # Check if key exists
        key_exists = False
        for i, line in enumerate(lines):
            if line.startswith(f"{key}="):
                lines[i] = new_line + "\n"
                key_exists = True
                break
        
        # If key doesn't exist, append it
        if not key_exists:
            lines.append(new_line + "\n")
        
        # Write updated content back to .env
        with open(env_file, 'w') as file:
            file.writelines(lines)
    else:
        # Create .env if it doesn't exist
        with open(env_file, 'w') as file:
            file.write(new_line + "\n")

def main():
    send_otp_result = send_login_otp(fy_id=FY_ID, app_id=APP_ID_TYPE)
    if send_otp_result[0] != SUCCESS:
        print(f"send_login_otp failure - {send_otp_result[1]}")
        sys.exit()
    print("send_login_otp success")

    generate_totp_result = generate_totp(secret=TOTP_KEY)
    if generate_totp_result[0] != SUCCESS:
        print(f"generate_totp failure - {generate_totp_result[1]}")
        sys.exit()
    print("generate_totp success")

    verify_totp_result = verify_totp(request_key=send_otp_result[1], totp=generate_totp_result[1])
    if verify_totp_result[0] != SUCCESS:
        print(f"verify_totp_result failure - {verify_totp_result[1]}")
        sys.exit()
    print("verify_totp_result success")

    verify_pin_result = verify_PIN(request_key=verify_totp_result[1], pin=PIN)
    if verify_pin_result[0] != SUCCESS:
        print(f"verify_pin_result failure - {verify_pin_result[1]}")
        sys.exit()
    print("verify_pin_result success")

    token_result = token(
        fy_id=FY_ID, app_id=APP_ID, redirect_uri=REDIRECT_URI,
        app_type=APP_TYPE, access_token=verify_pin_result[1]
    )
    if token_result[0] != SUCCESS:
        print(f"token_result failure - {token_result[1]}")
        sys.exit()
    print("token_result success")

    validate_authcode_result = validate_authcode(APP_ID_HASH, token_result[1])
    if validate_authcode_result[0] != SUCCESS:
        print(f"validate_authcode failure - {validate_authcode_result[1]}")
        sys.exit()
    print("validate_authcode success")

    access_token = APP_ID + "-" + APP_TYPE + ":" + validate_authcode_result[1]
    
    # Update FYERS_ACCESS_TOKEN in .env
    update_env_file("FYERS_ACCESS_TOKEN", access_token)
    print("FYERS_ACCESS_TOKEN updated in .env")

if __name__ == "__main__":
    main()