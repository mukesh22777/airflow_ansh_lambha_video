import requests

def call_api(url, start_date, end_date, auth_header):
    payload = {
        "start_date": start_date,
        "end_date": end_date
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": auth_header
    }

    response = requests.post(url, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()
