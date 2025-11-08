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
# Allow all origins for simplicity. For production, you might want to restrict this
# to your frontend's domain.
CORS(app)

# Utility function to convert a DataFrame back to the frontend's expected format.
def dataframe_to_sheet_format(df, original_sheet):
    """Converts a pandas DataFrame back into the list-of-lists format."""
    # The first row in the DataFrame becomes the header
    header = df.columns.tolist()
    # The rest of the rows are the data
    data_rows = df.values.tolist()
    
    # Combine header and data
    new_cells = [header] + data_rows
    
    # Ensure all values are strings for JSON serialization
    for r in range(len(new_cells)):
        for c in range(len(new_cells[r])):
            # Handle pandas/numpy specific types
            if pd.isna(new_cells[r][c]):
                new_cells[r][c] = ""
            else:
                new_cells[r][c] = str(new_cells[r][c])

    # Pad rows and columns to match the original sheet's dimensions if necessary
    # This prevents the sheet from shrinking unexpectedly
    num_original_rows = len(original_sheet['cells'])
    num_original_cols = len(original_sheet['cells'][0]) if num_original_rows > 0 else 0
    
    num_new_rows = len(new_cells)
    num_new_cols = max(len(row) for row in new_cells) if num_new_rows > 0 else 0

    final_rows = max(num_original_rows, num_new_rows)
    final_cols = max(num_original_cols, num_new_cols)

    # Create a new blank grid
    final_cells = [["" for _ in range(final_cols)] for _ in range(final_rows)]
    
    # Fill it with the new data
    for r in range(num_new_rows):
        for c in range(len(new_cells[r])):
            final_cells[r][c] = new_cells[r][c]
            
    return final_cells


@app.route('/execute', methods=['POST'])
def execute():
    try:
        # Get data from the frontend request
        data = request.get_json()
        code = data.get('code')
        sheets_data = data.get('sheets')
        
        if not code or not sheets_data:
            return jsonify({"error": "Missing 'code' or 'sheets' in request."}), 400

        # --- Data Preparation ---
        # Convert the incoming list of sheet objects into a dictionary of pandas DataFrames
        dfs = {}
        for sheet in sheets_data:
            sheet_name = sheet['name']
            cells = sheet.get('cells', [])
            if not cells or not cells[0]: # Handle empty sheet
                dfs[sheet_name] = pd.DataFrame()
            else:
                # The first row is the header, the rest is data
                header = cells[0]
                data_rows = cells[1:]
                df = pd.DataFrame(data_rows, columns=header)
                # An empty string is a better NaN representation for our use case
                df.replace('', np.nan, inplace=True)
                dfs[sheet_name] = df

        # --- Code Execution ---
        # Prepare a local scope for the exec function to run in.
        # This scope includes the 'dfs' dictionary and imported libraries.
        local_scope = {
            'dfs': dfs,
            'pd': pd,
            'np': np,
            'requests': requests,
            'BeautifulSoup': BeautifulSoup
        }

        # Capture any print() statements from the executed code
        stdout_capture = io.StringIO()
        with contextlib.redirect_stdout(stdout_capture):
            exec(code, local_scope)
        
        # Retrieve the modified DataFrames from the local scope
        modified_dfs = local_scope.get('dfs', {})

        # --- Data Post-processing ---
        # Convert the modified DataFrames back to the JSON format the frontend expects
        updated_sheets_data = []
        for original_sheet in sheets_data:
            sheet_name = original_sheet['name']
            updated_sheet = original_sheet.copy() # Start with a copy to preserve formats etc.
            
            if sheet_name in modified_dfs:
                df = modified_dfs[sheet_name]
                # Convert the DataFrame back to a list of lists (cells)
                updated_sheet['cells'] = dataframe_to_sheet_format(df, original_sheet)

            updated_sheets_data.append(updated_sheet)

        return jsonify({
            "sheets": updated_sheets_data,
            "stdout": stdout_capture.getvalue()
        })

    except Exception as e:
        # If any error occurs during execution, return it as a JSON response
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

if __name__ == '__main__':
    # This is for local development. Gunicorn will be used in production on Render.
    app.run(debug=True, port=5001)
