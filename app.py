import os
import json
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import bigquery
from google.oauth2 import service_account

# Initialize Flask app
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Allow all origins

# Load Google credentials
credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if credentials_json:
    try:
        credentials_dict = json.loads(credentials_json)
        temp_credentials_path = "/tmp/gcloud-credentials.json"
        with open(temp_credentials_path, "w") as f:
            json.dump(credentials_dict, f)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_credentials_path
        credentials = service_account.Credentials.from_service_account_file(temp_credentials_path)
        client = bigquery.Client(credentials=credentials)
    except json.JSONDecodeError as e:
        print(f"❌ JSON Decode Error: {e}")
        credentials = None
        client = None
else:
    print("❌ No credentials found in environment variable!")
    credentials = None
    client = None


@app.route('/query_bigquery', methods=['POST'])
def query_bigquery():
    try:
        user_input = request.json.get("TABLE_ID")
        if not user_input:
            return jsonify({"status": "error", "message": "TABLE_ID is missing in request."})

        TABLE_ID = user_input

        dataset_list = [
            "keywords_ranking_data_sheet1",
            "keywords_ranking_data_sheet2",
            "keywords_ranking_data_sheet3",
            "keywords_ranking_data_sheet4"
        ]

        all_dataframes = []

        for dataset in dataset_list:
            query = f"SELECT * FROM `automatic-spotify-scraper.{dataset}.{TABLE_ID}`"
            query_job = client.query(query)
            results = query_job.result()
            data = [dict(row) for row in results]
            df = pd.DataFrame(data)

            if df.empty:
                print(f"❌ No data found for {dataset}.{TABLE_ID}")
                continue

            print(f"✅ Columns for {dataset}.{TABLE_ID}: {df.columns}")

            # Convert column names to strings first
            df.columns = [str(x) for x in df.columns]

            # Separate numeric and non-numeric column names
            numeric_cols = []
            non_numeric_cols = ['row_number']  # Always keep row_number

            for col in df.columns:
                if col != 'row_number':  # Skip row_number
                    try:
                        numeric_cols.append(int(col))  # Convert purely numeric column names to int
                    except ValueError:
                        non_numeric_cols.append(col)  # Keep non-numeric names as strings

            # Sort numeric columns properly
            numeric_cols.sort()

            # Apply updated column order
            df = df[non_numeric_cols + numeric_cols[:98]]  # Keep first 98 columns only

            all_dataframes.append(df)

        # if all_dataframes:
        #     final_df = pd.concat(all_dataframes, axis=0)
        # else:
        #     return jsonify({"status": "error", "message": "No data found in any dataset."})

        return jsonify({"status": "success", "data": all_dataframes})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8080)
