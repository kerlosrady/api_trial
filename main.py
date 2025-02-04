import json
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS  # Import CORS
from google.cloud import bigquery

# Initialize Flask app
app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": "*"}})  # Allow all domains for testing

# Authenticate with BigQuery
service_account_json = "automatic-spotify-scraper.json"
client = bigquery.Client.from_service_account_json(service_account_json)

PROJECT_ID = "automatic-spotify-scraper"
DATASET_ID = "keywords_ranking_data_sheet1"
TABLE_ID = "afro"

@app.route('/query_bigquery', methods=['POST'])  # Ensure POST is listed here
def query_bigquery():
    try:
        # Get user input from request
        user_input = request.json.get("query_param")

        # Construct SQL Query (Modify based on your dataset)
        query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        LIMIT 10;
        """

        # Run the query
        query_job = client.query(query)
        results = query_job.result()

        # Convert results to JSON
        data = [dict(row) for row in results]
        return jsonify({"status": "success", "data": data})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8080)  # ✅ Cloud Run needs 8080
