import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify
from flask_cors import CORS
import io
import contextlib

# Initialize Flask App and enable CORS
app = Flask(__name__)
CORS(app)  # Allow all origins (for development); restrict later for production.

# -------------------------------
# Root Route (for basic testing)
# -------------------------------
@app.route('/')
def home():
    return jsonify({"message": "AI Excel Backend is running on Render!"}), 200


# -------------------------------
# Ping Route (for AI Studio / health check)
# -------------------------------
@app.route('/ping', methods=['GET'])
def ping():
    """A simple endpoint to check if the backend is running."""
    return jsonify({"status": "ok"}), 200


# -------------------------------
# Utility Function
# -------------------------------
def dataframe_to_sheet_format(df, original_sheet):
    """Converts a pandas DataFrame back into the list-of-lists format."""
    header = df.columns.tolist()
    data_rows = df.values.tolist()
    new_cells = [header] + data_rows

    # Ensure all values are strings for JSON serialization
    for r in range(len(new_cells)):
        for c in range(len(new_cells[r])):
            if pd.isna(new_cells[r][c]):
                new_cells[r][c] = ""
            else:
                new_cells[r][c] = str(new_cells[r][c])

    # Match original sheet dimensions (to avoid shrinking)
    num_original_rows = len(original_sheet['cells'])
    num_original_cols = len(original_sheet['cells'][0]) if num_original_rows > 0 else 0

    num_new_rows = len(new_cells)
    num_new_cols = max(len(row) for row in new_cells) if num_new_rows > 0 else 0

    final_rows = max(num_original_rows, num_new_rows)
    final_cols = max(num_original_cols, num_new_cols)

    final_cells = [["" for _ in range(final_cols)] for _ in range(final_rows)]

    for r in range(num_new_rows):
        for c in range(len(new_cells[r])):
            final_cells[r][c] = new_cells[r][c]

    return final_cells


# -------------------------------
# Execute Route (Main Logic)
# -------------------------------
@app.route('/execute', methods=['POST'])
def execute():
    try:
        # Get data from the frontend request
        data = request.get_json()
        code = data.get('code')
        sheets_data = data.get('sheets')

        if not code or not sheets_data:
            return jsonify({"error": "Missing 'code' or 'sheets' in request."}), 400

        # Convert the incoming sheets into DataFrames
        dfs = {}
        for sheet in sheets_data:
            sheet_name = sheet['name']
            cells = sheet.get('cells', [])
            if not cells or not cells[0]:
                dfs[sheet_name] = pd.DataFrame()
            else:
                header = cells[0]
                data_rows = cells[1:]
                df = pd.DataFrame(data_rows, columns=header)
                df.replace('', np.nan, inplace=True)
                dfs[sheet_name] = df

        # Prepare environment for execution
        local_scope = {
            'dfs': dfs,
            'pd': pd,
            'np': np,
            'requests': requests,
            'BeautifulSoup': BeautifulSoup
        }

        # Capture print() output
        stdout_capture = io.StringIO()
        with contextlib.redirect_stdout(stdout_capture):
            exec(code, local_scope)

        modified_dfs = local_scope.get('dfs', {})

        # Convert modified DataFrames back to frontend format
        updated_sheets_data = []
        for original_sheet in sheets_data:
            sheet_name = original_sheet['name']
            updated_sheet = original_sheet.copy()

            if sheet_name in modified_dfs:
                df = modified_dfs[sheet_name]
                updated_sheet['cells'] = dataframe_to_sheet_format(df, original_sheet)

            updated_sheets_data.append(updated_sheet)

        return jsonify({
            "sheets": updated_sheets_data,
            "stdout": stdout_capture.getvalue()
        })

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


# -------------------------------
# Run Flask (for local development)
# -------------------------------
if __name__ == '__main__':
    app.run(debug=True, port=5001)
