from fivetran_connector_sdk import Connector
import requests
import json
import time
import os
import logging
from datetime import datetime
from flask import Flask, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

class BuildOpsConnector(Connector):
    def __init__(self, config):
        # Fixed: removed `service=` from super().__init__()
        super().__init__(config)
        self.base_url = "https://api.buildops.com"
        self.token = None
        self.token_expiry = None

    def authenticate(self):
        if not self.token or datetime.utcnow().timestamp() >= self.token_expiry:
            logging.info("Fetching new JWT token from BuildOps")
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
        headers = self.authenticate()
        all_data = []
        page = 1
        limit = 100

        while True:
            if params is None:
                params = {}
            params.update({"page": page, "limit": limit})
            logging.info(f"Fetching page {page} from {endpoint}")
            response = requests.get(f"{self.base_url}{endpoint}", headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            items = data.get("items", [data]) if "items" in data else [data]
            all_data.extend(items)
            if len(items) < limit:
                break
            page += 1
            time.sleep(1)  # avoid rate limits

        return all_data

    def flatten_record(self, record, table_name):
        flattened = {
            "id": str(record.get("id")),
            "sync_timestamp": datetime.utcnow().isoformat()
        }

        if table_name == "customers":
            flattened.update({
                "name": str(record.get("name")),
                "email": str(record.get("email")),
                "phone_primary": str(record.get("phonePrimary")),
                "status": str(record.get("status")),
                "is_active": record.get("isActive")
            })
        elif table_name == "invoices":
            flattened.update({
                "invoice_number": str(record.get("invoiceNumber")),
                "total_amount": float(record.get("totalAmount", 0.0)),
                "status": str(record.get("status")),
                "customer_name": str(record.get("customerName"))
            })
        elif table_name == "vendors":
            flattened.update({
                "name": str(record.get("name")),
                "email": str(record.get("email")),
                "status": str(record.get("status"))
            })

        return flattened

    def sync(self, context):
        endpoints = [
            ("/v1/customers", "customers"),
            ("/v1/invoices", "invoices"),
            ("/v1/vendors", "vendors")
        ]

        for endpoint, table_name in endpoints:
            try:
                logging.info(f"Syncing table: {table_name}")
                data = self.fetch_paginated_data(endpoint)
                for record in data:
                    flat = self.flatten_record(record, table_name)
                    self.write(table_name, flat)
            except Exception as e:
                logging.exception(f"Error syncing {table_name}")

        return context.get("state", {})

    def write(self, table_name, record):
        logging.info(f"Writing to {table_name}: {record['id']}")
        self.output.write({
            "schema": "public",
            "table": table_name,
            "data": [record]
        })

@app.route('/sync', methods=['POST'])
def handle_sync():
    try:
        auth = request.authorization
        system_key = os.environ.get("FIVETRAN_SYSTEM_KEY")
        system_secret = os.environ.get("FIVETRAN_SYSTEM_SECRET")

        if not auth or auth.username != system_key or auth.password != system_secret:
            logging.error("Unauthorized sync attempt.")
            return {"error": "Unauthorized"}, 401

        logging.info("Fivetran sync request authorized.")

        config = {
            "client_id": os.environ.get("BUILDOPS_CLIENT_ID"),
            "client_secret": os.environ.get("BUILDOPS_CLIENT_SECRET"),
            "tenant_id": os.environ.get("BUILDOPS_TENANT_ID")
        }

        connector = BuildOpsConnector(config)
        state = connector.sync({})
        return {"status": "success", "state": state}

    except Exception as e:
        logging.exception("Error handling sync request")
        return {"error": "Internal Server Error", "message": str(e)}, 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
