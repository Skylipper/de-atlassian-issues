import logging
import requests
import time

def execute_request(method, url, headers, payload, expected_code):
    logging.info(f"method: {method}, url: {url}")

    response_code = 0
    response = None
    retries = 0

    for i in range (0, var.REQUEST_RETRY_LIMIT):
        response = requests.request("GET", url, headers=headers, data=payload)
        response_code = response.status_code
        if response_code == expected_code:
            break
        retries += 1
        time.sleep(3)

    if response_code != expected_code:
        raise Exception(f"Request failed with code {response_code}, expected {expected_code}")

    return response.json()