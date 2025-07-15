from fivetran_connector_sdk import Connector, State
import requests
import json
import time
import os
from datetime import datetime
from flask import Flask, request

app = Flask(__name__)

class BuildOpsConnector(Connector):
    def __init__(self, config):
        super().__init__(config)
        self.base_url = "https://api.buildops.com"
        self.token = None
        self.token_expiry = None

    def authenticate(self):
        """Fetch JWT token from BuildOps API."""
        if not self.token or datetime.utcnow().timestamp() >= self.token_expiry:
            url = f"{self.base_url}/v1/auth/token"
            payload = {
                "clientId": self.config["client_id"],
                "clientSecret": self.config["client_secret"]
            }
            response = requests.post(url, json=payload)
            response.raise_for_status()
            token_data = response.json()
            self.token = token_data["access_token"]
            self.token_expiry = datetime.utcnow().timestamp() + token_data["expires_in"] - 300
        return {"Authorization": f"Bearer {self.token}", "tenantId": self.config["tenant_id"]}

    def fetch_paginated_data(self, endpoint, params=None):
        """Fetch all data from a paginated endpoint."""
        headers = self.authenticate()
        all_data = []
        page = 1
        limit = 100

        while True:
            if params is None:
                params = {}
            params.update({"page": page, "limit": limit})
            response = requests.get(f"{self.base_url}{endpoint}", headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            items = data.get("items", [data]) if "items" in data else [data]
            all_data.extend(items)
            if len(items) < limit:
                break
            page += 1
            time.sleep(1)  # Avoid rate limits

        return all_data

    def flatten_record(self, record, table_name):
        """Flatten nested JSON for Snowflake compatibility."""
        flattened = {
            "id": str(record.get("id")),
            "sync_timestamp": datetime.utcnow().isoformat()
        }
        if table_name == "customers":
            flattened.update({
                "name": str(record.get("name")),
                "email": str(record.get("email")),
                "address_street": str(record.get("addressList", [{}])[0].get("street")),
                "phone_primary": str(record.get("phonePrimary"))
            })
        elif table_name == "invoices":
            flattened.update({
                "invoice_id": str(record.get("id")),
                "amount": float(record.get("amount", 0.0))
            })
        elif table_name == "vendors":
            flattened.update({
                "name": str(record.get("name")),
                "email": str(record.get("email")),
                "vendor_code": str(record.get("vendorCode"))
            })
        return flattened

    def sync(self, state: State) -> State:
        """Sync data from BuildOps API."""
        endpoints = [
            ("/v1/customers", "customers"),
            ("/v1/invoices", "invoices"),
            ("/v1/vendors", "vendors")
        ]

        for endpoint, table_name in endpoints:
            data = self.fetch_paginated_data(endpoint)
            for record in data:
                flattened_record = self.flatten_record(record, table_name)
                self.write(table_name, flattened_record)

        return state

    def write(self, table_name, record):
        """Write record to Fivetran output."""
        self.output.write({
            "schema": "public",
            "table": table_name,
            "data": [record]
        })

@app.route('/sync', methods=['POST'])
def handle_sync():
    """Handle Fivetran's HTTP request with Basic Authentication."""
    auth = request.authorization
    if not auth or auth.username != "ft_pAb2Uzjbw4SKH" or auth.password != "QyskwxJ5XvewqnrxYpq0OVOsrQP7faZC":
        return {"error": "Unauthorized"}, 401
    config = {
        "client_id": os.environ.get("BUILDOPS_CLIENT_ID"),
        "client_secret": os.environ.get("BUILDOPS_CLIENT_SECRET"),
        "tenant_id": os.environ.get("BUILDOPS_TENANT_ID")
    }
    connector = BuildOpsConnector(config)
    state = State()
    connector.sync(state)
    return {"status": "success"}

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
