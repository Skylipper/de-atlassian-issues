import logging
import time

import requests

import src.utils.variables as var


def execute_request(method, url, headers, payload):
    logging.info(f"method: {method}, url: {url}")

    response_code = 0
    response = None
    retries = 0

    for i in range(0, var.REQUEST_RETRY_LIMIT):
        response = requests.request("GET", url, headers=headers, data=payload, timeout=15)

        response_code = response.status_code
        if response.ok:
            break
        retries += 1
        time.sleep(3)

    if not response.ok:
        raise Exception(f"Request failed with code {response_code}")

    return response.json()
