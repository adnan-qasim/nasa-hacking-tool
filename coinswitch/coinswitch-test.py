from cryptography.hazmat.primitives.asymmetric import ed25519
from urllib.parse import urlparse, urlencode,unquote_plus
import requests, pprint, json, urllib
import datetime, pytz, fake_useragent
import pymongo, time, threading,traceback,os


def get_signature(method, endpoint, payload, params):
    unquote_endpoint = endpoint
    if method == "GET" and len(params) != 0:
        endpoint += ("&", "?")[urlparse(endpoint).query == ""] + urlencode(params)
        unquote_endpoint = urllib.parse.unquote_plus(endpoint)

    signature_msg = (
        method
        + unquote_endpoint
        + json.dumps(payload, separators=(",", ":"), sort_keys=True)
    )

    secret_key = "f0b6def0f7db9e72d919f2c99e4c72249bb9df13e9b90c139d030f1a83eccf05"
    request_string = bytes(signature_msg, "utf-8")
    secret_key_bytes = bytes.fromhex(secret_key)
    secret_key = ed25519.Ed25519PrivateKey.from_private_bytes(secret_key_bytes)
    signature_bytes = secret_key.sign(request_string)
    signature = signature_bytes.hex()
    # print(signature)
    return signature


def validate_signature():
    endpoint = "/trade/api/v2/validate/keys"
    method = "GET"
    payload = {}

    api_key = "fe7bb54023d1c7995ee6da858846fae1ef2fc6aeda53d52384f4fc3b810b2d37" 

    sign = get_signature(method,endpoint,payload,{})

    url = "https://coinswitch.co" + endpoint

    headers = {
    'Content-Type': 'application/json',
    'X-AUTH-SIGNATURE': sign,
    'X-AUTH-APIKEY': api_key
    }

    response = requests.request("GET", url, headers=headers, json=payload)
    print(response.json())


def GetCandles():
    params = {
        "end_time": "1694771637000",
        "start_time": "1694319804000",
        "symbol": "BTC/INR",
        "interval": "60",
        "exchange": "coinswitchx"
    }

    endpoint = "/trade/api/v2/candles"
    endpoint += ('&', '?')[urlparse(endpoint).query == ''] + urlencode(params)
    method = "GET"
    payload = {}

    api_key = "fe7bb54023d1c7995ee6da858846fae1ef2fc6aeda53d52384f4fc3b810b2d37" 

    signature =  get_signature(method,endpoint,payload,params)

    url = "https://coinswitch.co" + endpoint
    print(url)

    headers = {
    'Content-Type': 'application/json',
    'X-AUTH-SIGNATURE': signature,
    'X-AUTH-APIKEY': api_key
    }

    response = requests.request("GET", url, headers=headers, json=payload)
    print(response.text)


validate_signature()
GetCandles()