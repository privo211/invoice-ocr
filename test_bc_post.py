import requests
import os
from dotenv import load_dotenv

# Load your existing credentials
load_dotenv()
BC_TENANT = os.getenv("AZURE_TENANT_ID")
BC_COMPANY = os.getenv("BC_COMPANY")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
# Set this to "Production" or your Sandbox name
BC_ENV = "SANDBOX-25C" 

def get_token():
    token_url = f"https://login.microsoftonline.com/{BC_TENANT}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://api.businesscentral.dynamics.com/.default"
    }
    resp = requests.post(token_url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

def test_post():
    token = get_token()
    
    # The endpoint from your AL code and app.py
    url = f"https://api.businesscentral.dynamics.com/v2.0/{BC_TENANT}/{BC_ENV}/ODataV4/Company('{BC_COMPANY}')/Lot_Info_Card"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Prefer": "odata.maxversion=4.0;IEEE754Compatible=true"
    }

    # Test payload exactly as seen in your webapp logs
    payload = {
        "Item_No": "1290123-MS",
        "Lot_No": "AUTO",
        "TMG_Vendor_Lot_No": "TEST_LOT_001",
        "TMG_Treatment_Description": "Thiram", # The problematic field
        "TMG_Treatment_Description_2": "Filmcoat",
        "TMG_Treated": "Yes",
        "TMG_Country_Of_Origin": "US"
    }

    print(f"Sending POST to: {url}")
    response = requests.post(url, json=payload, headers=headers)
    
    print(f"Status Code: {response.status_code}")
    if response.status_code in [200, 201]:
        result = response.json()
        print("Success! Response from BC:")
        print(f"Generated Lot_No: {result.get('Lot_No')}")
        print(f"TMG_Treatment_Description in response: '{result.get('TMG_Treatment_Description')}'")
        print(f"TMG_Treatment_Description in response: '{result.get('TMG_Treatment_Description_2')}'")
    else:
        print("Error details:")
        print(response.text)

if __name__ == "__main__":
    test_post()