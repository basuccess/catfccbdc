# readme.md

CatFCCBDC
CatFCCBDC (Categorized FCC Broadband Data Collection) is a Python project designed to process and merge FCC Broadband Data Collection (BDC) datasets with Census Tabblock20 shapefiles, generating GeoJSON and GeoPackage outputs for broadband service analysis. Built for Tarana Wireless, it supports creating detailed broadband coverage maps to identify service gaps, aiding business case development for Fixed Wireless Broadband deployments under initiatives like BEAD.

Features
Data Processing: Reads BDC CSV files and Census Tabblock20 shapefiles (tl_XX_tabblock20.shp).
Merging: Combines BDC service statistics (e.g., Copper, Cable, Fiber) with spatial data.
Chunking: Handles large states (e.g., Texas with ~681k features) via memory-efficient chunked writing.
Output: Produces GeoJSON (XX_STATE_BB_4269.geojson) and GeoPackage (XX_STATE_BB.gpkg) files in EPSG:4269 (NAD83).
Logging: Detailed debug logs for troubleshooting (e.g., debug.log).

Prerequisites
Python: 3.11+

Dependencies:
fiona (spatial data handling)
geopandas (GeoDataFrame operations)
psutil (memory monitoring)
shapely (geometry validation)

Hardware:
Minimum: 16GB RAM, 8-core CPU (e.g., current MacBook Pro).
Recommended: 128GB RAM, 16-core CPU (e.g., M4 Max) for large datasets (e.g., TX >1.5GB).

Install dependencies:

pip install -r requirements.txt

Setup
Clone the Repository:
git clone (https://github.com/basuccess/catfccbdc.git)
cd catfccbdc

Virtual Environment:
python -m venv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate     # Windows

Install Requirements:
pip install fiona geopandas psutil shapely

Data Directory:
Place BDC CSVs in /Data/USA_FCC-bdc/XX_STATE_StateName/.
Place Tabblock20 shapefiles in /Data/USA_Census/XX_STATE_StateName/.
Usage

Run the script for a specific state (e.g., Texas):

python src/main.py --log-file debug.log --log-level DEBUG --log-parts main,tabblockmerge --base-dir "/Data" --state TX 2>&1 | tee output.log

Arguments:
--base-dir: Root data directory (default: current dir).
--state: State abbreviation(s) (e.g., TX, AL AK TX).
--log-file: Log output file (e.g., debug.log).
--log-level: Logging level (DEBUG, INFO, etc.).
--log-parts: Modules for detailed logging (e.g., main,tabblockmerge).

Output:

/Data/USA_FCC-bdc/48_TX_Texas/48_TX_BB_4269.geojson
/Data/USA_FCC-bdc/48_TX_Texas/48_TX_BB.gpkg

Current Status
Feature Processing: Successfully merges 668,757 features for Texas (7 chunks), but Census 2020 expects ~680,973—investigating shapefile (tl_48_tabblock20.shp) or JSON conversion limits.
Validation: Hits GDAL’s 200MB default size limit—set to 2048MB (2GB) to handle ~1.5GB files, testing underway.
Hardware: 16GB RAM (83.3% used) strains on TX—upgrade to M4 Max (128GB RAM, 16 cores) proposed for scalability.
Troubleshooting

Feature Count Mismatch:
Check shapefile: python -c "import fiona; print(len(list(fiona.open('/path/to/tl_48_tabblock20.shp'))))".
If <681k, redownload from Census TIGER 2020.

Size Errors: Adjust OGR_GEOJSON_MAX_OBJ_SIZE—current 2GB fits 16GB RAM, scales with M4 Max.
Logs: See debug.log—search Invalid feature or Processed X features.
Contributing

Tony Thouweling: Lead developer—optimizing for large-scale broadband analysis.
Submit issues/pull requests to enhance chunking, parallel processing, or memory efficiency.

License
Proprietary—Tarana Wireless internal use. Contact thouweling@taranawireless.com for permissions.



Example jq commands to inspect geojson:

Replace input.json with the actual filename containing your JSON data, or pipe the JSON into jq like this:
echo '<your_json_here>' | jq '.features[0].properties.Copper'

If you want a specific subfield within "Copper", such as the AT&T provider details, you can extend the query, e.g., jq '.features[0].properties.Copper."AT&T"'.

To use jq to print the "geometry" field from your JSON data, you can run the following command in your terminal:
jq '.features[0].geometry' input.json

If you want only specific parts of the "geometry" object (e.g., just the "type" or "coordinates"), you can refine the query:
For "type" only: jq '.features[0].geometry.type'
For "coordinates" only: jq '.features[0].geometry.coordinates'

If you want the values printed separately:

For just "id":

jq '.features[0].properties.id' input.json
Output: "481390602122026"

For just "Total_LocationCount":

jq '.features[0].properties.Total_LocationCount' input.json
Output: 6

Replace input.json with your actual JSON file name, or pipe the JSON into jq:


echo '<your_json_here>' | jq '.features[0].properties | {id: .id, Total_LocationCount: .Total_LocationCount}'
If you want the output in a different format (e.g., raw values without JSON structure), you can use the -r flag with jq and adjust the query accordingly.

To print the "id" field for all features in a GeoJSON file using jq, you can use the following command:

jq '.features[].properties.id' input.json

Explanation:
.features[] iterates over all elements in the "features" array.
.properties accesses the "properties" object within each feature.
.id extracts the "id" field from the "properties" object.

Output:
For your provided GeoJSON (which contains only one feature), the output would be:

"481390602122026"

If your GeoJSON file has multiple features, it will print the "id" for each feature on a new line. For example, if you had two features with "id" values "481390602122026" and "481390602122027", the output would be:

"481390602122026"
"481390602122027"

Notes:
Replace input.json with the name of your GeoJSON file.
If you want the output without quotes (raw strings), use the -r flag:

jq -r '.features[].properties.id' input.json
Output for your example:

481390602122026

If your file has no features or the "id" field is missing in some features, jq will skip those cases silently unless you add error handling (e.g., with // "missing").