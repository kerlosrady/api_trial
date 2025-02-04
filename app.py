import os
import json
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS  # Import CORS
from google.cloud import bigquery

# Initialize Flask app
app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": "*"}})  # Allow all domains for testing

from google.oauth2 import service_account

# Load credentials from the environment variable
credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Ensure the credentials are correctly formatted
if credentials_json:
    try:
        credentials_dict = json.loads(credentials_json)  # Convert to dictionary
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        client = bigquery.Client(credentials=credentials)  # Pass credentials to BigQuery
    except json.JSONDecodeError as e:
        print(f"❌ JSON Decode Error: {e}")
        credentials = None
        client = None
else:
    print("❌ No credentials found in environment variable!")
    credentials = None
    client = None

PROJECT_ID = "automatic-spotify-scraper"
DATASET_ID = "keywords_ranking_data_sheet1"
TABLE_ID = "afro"

@app.route('/query_bigquery', methods=['POST'])  # Ensure POST is listed here
import pandas as pd
from flask import Flask, request, jsonify
from google.cloud import bigquery

# Initialize Flask app
app = Flask(__name__)

@app.route('/query_bigquery', methods=['POST'])  # Ensure POST is listed here
def query_bigquery():
    try:
        # Get user input from request
        user_input = request.json.get("TABLE_ID")

        if not user_input:
            return jsonify({"status": "error", "message": "TABLE_ID is missing in request."})

        TABLE_ID = user_input  # Assign TABLE_ID from input

        # Initialize BigQuery client
        client = bigquery.Client()

        # List of dataset names to query
        dataset_list = [
            "keywords_ranking_data_sheet1",
            "keywords_ranking_data_sheet2",
            "keywords_ranking_data_sheet3",
            "keywords_ranking_data_sheet4"
        ]

        all_dataframes = []  # Store all dataframes

        for dataset in dataset_list:
            # Construct SQL Query
            query = f"SELECT * FROM `automatic-spotify-scraper.{dataset}.{TABLE_ID}`"

            # Run the query
            query_job = client.query(query)
            results = query_job.result()

            # Convert results to DataFrame
            data = [dict(row) for row in results]
            df = pd.DataFrame(data)

            if df.empty:
                print(f"❌ No data found for {dataset}.{TABLE_ID}")
                continue

            print(f"✅ Columns for {dataset}.{TABLE_ID}: {df.columns}")

            try:
                # Rename columns correctly
                df.columns = ['row_number'] + [int(x) for x in df.columns if 'row' not in x]
            except ValueError:
                print(f"❌ Column renaming issue in {dataset}.{TABLE_ID}")

            # Keep only the first 99 columns
            df = df[['row_number'] + list(range(1, 99))]

            # Append to list
            all_dataframes.append(df)

        # Concatenate all data
        if all_dataframes:
            final_df = pd.concat(all_dataframes, axis=0)
        else:
            return jsonify({"status": "error", "message": "No data found in any dataset."})

        # Convert DataFrame to JSON response
        return jsonify({"status": "success", "data": final_df.to_dict(orient="records")})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8080)  # ✅ Cloud Run needs 8080
