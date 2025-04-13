import requests

headers = {
    "Authorization": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiZDoxIiwiZDoyIiwieDowIiwieDoxIiwieDoyIl0sImF0X2hhc2giOiJnQUFBQUFCbjVpMWxpZ285cm5wc2I0UWJJLWhzOGEyZmttODRHODMwcGo3WWNkRU9XRWplUWVXNHhBZ1NMcFRtVmh6b3BGdE5pUjBRN05hWXU5QXI3WGNSYnhhakd4TjhSc0ExQm5jRTVGdEp3a1pjbXc4S0NVdz0iLCJkaXNwbGF5X25hbWUiOiIiLCJvbXMiOiJSMCIsImhzbV9rZXkiOiIyOTAzNWE4YjAzMTJkMmZjYWFmYzY1YTA1MDhlNTIzZjg5YzRiMWNmOTYxMjhkOThkNjg0ODhjMSIsImlzRGRwaUVuYWJsZWQiOiJOIiwiaXNNdGZFbmFibGVkIjoiTiIsImZ5X2lkIjoiWUE0NjIxOCIsImFwcFR5cGUiOjEwMCwiZXhwIjoxNzQzMjA4MjAwLCJpYXQiOjE3NDMxMzgxNDksImlzcyI6ImFwaS5meWVycy5pbiIsIm5iZiI6MTc0MzEzODE0OSwic3ViIjoiYWNjZXNzX3Rva2VuIn0.eiik7ExmeZ",
    "Content-Type": "application/json"
}

payload = {
    "symbol": "NSE:ADANIENT-EQ",
    "resolution": "D",
    "date_format": "0",
    "range_from": "2025-03-10",
    "range_to": "2025-03-25",
    "cont_flag": "1"
}

response = requests.post("https://api.fyers.in/api/v2/history", json=payload, headers=headers)

print(response.status_code)
print(response.text)  # See the response
