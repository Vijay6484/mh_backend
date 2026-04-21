import os
import json
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Path to the Scripts directory where index_output is kept
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX_DIR = os.path.join(BASE_DIR, 'Scripts', 'index_output')

@app.route('/api/locations', methods=['GET'])
def get_locations():
    """
    Returns the hierarchy of districts, talukas, and villages based on the directories
    available in Scripts/index_output.
    """
    data = {}
    if not os.path.exists(INDEX_DIR):
        return jsonify({"error": "Index directory not found"}), 404

    for district in os.listdir(INDEX_DIR):
        district_path = os.path.join(INDEX_DIR, district)
        if os.path.isdir(district_path) and not district.startswith('.'):
            data[district] = {}
            for taluka in os.listdir(district_path):
                taluka_path = os.path.join(district_path, taluka)
                if os.path.isdir(taluka_path) and not taluka.startswith('.'):
                    data[district][taluka] = []
                    for village in os.listdir(taluka_path):
                        village_path = os.path.join(taluka_path, village)
                        if os.path.isdir(village_path) and not village.startswith('.'):
                            data[district][taluka].append(village)
    
    return jsonify(data)

@app.route('/api/search', methods=['GET'])
def search_property():
    """
    Searches for a specific property number in the data.json of the given district, taluka, and village.
    Expected query parameters: district, taluka, village, query (number)
    """
    district = request.args.get('district')
    taluka = request.args.get('taluka')
    village = request.args.get('village')
    query = request.args.get('query')

    if not all([district, taluka, village, query]):
        return jsonify({"error": "Missing required parameters"}), 400

    if not query.isdigit():
        return jsonify({"error": "Query must be numeric"}), 400

    file_path = os.path.join(INDEX_DIR, district, taluka, village, 'data.json')
    if not os.path.exists(file_path):
        return jsonify({"error": "Data file not found for the selected location"}), 404

    try:
        results = []
        # If the file is extremely large, loading it entirely might be heavy,
        # but for this scale (35MB), `json.load` should handle it quickly enough in-memory.
        with open(file_path, 'r', encoding='utf-8') as f:
            records = json.load(f)
            
        for record in records:
            if 'property_numbers' in record:
                for prop in record['property_numbers']:
                    if prop.get('value') == query:
                        results.append(record)
                        break # Found a matching property number in this doc

        # Ensure we sort the results by date if possible (optional)
        return jsonify({"count": len(results), "results": results})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=8000)
