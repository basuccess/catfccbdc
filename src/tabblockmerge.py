# tabblockmerge.py (updated)

import os
import logging
import json
import fiona
from fiona import Env
import psutil
import shutil
import glob  # Add this import for file pattern matching
import re    # Add this import for regex operations
import traceback  # Add this import for error tracing
from copy import deepcopy
from shapely.geometry import shape, mapping
from functools import wraps
import time
from constant import TECH_ABBR_MAPPING, SERVED_DL_SPEED, SERVED_UL_SPEED, LOW_LATENCY, UNDERSERVED_DL_SPEED, UNDERSERVED_UL_SPEED
# Import from constant.py
from prep import check_required_files, get_state_info

def decimal_to_json_serializable(obj):
    """Convert decimal.Decimal objects to float for JSON serialization."""
    import decimal
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    elif hasattr(obj, '__geo_interface__'):
        return obj.__geo_interface__
    return str(obj)

# Retry decorator for handling transient GDAL/Fiona errors
def retry(max_attempts=5, delay=3):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except fiona.errors.DriverError as e:
                    attempts += 1
                    if attempts == max_attempts:
                        raise
                    logging.warning(f"Retry {attempts}/{max_attempts} for {func.__name__}: {str(e)}")
                    time.sleep(delay)
        return wrapper
    return decorator

def process_tabblock_data(base_dir, state_abbr, temp_dir):
    logging.info(f"Processing Tabblock20 data for state: {state_abbr}")
    _, tabblock_files = check_required_files(base_dir, state_abbr)
    fips, abbr, name = get_state_info(state_abbr)
    state_dir = f"{fips}_{abbr}_{name}"
    
    # Use os.path.join for robust path handling with spaces/special characters
    tabblock_file = os.path.join(base_dir, r"USA_Census", state_dir, tabblock_files[0])
    logging.info(f"Reading tabblock file: {tabblock_file}")
    
    # Use raw string and os.path.join for temp file path
    temp_json_file = os.path.join(temp_dir, f"tabblock_{state_abbr}.json")
    with open(temp_json_file, 'w') as f:
        f.write('{"type": "FeatureCollection", "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4269"}}, "features": [')
        first = True
        with fiona.open(tabblock_file, 'r') as src:
            for feature in src:
                geoid20 = feature["properties"]["GEOID20"]
                properties = {
                    "block_geoid": geoid20,
                    "Copper": None,
                    "Cable": None,
                    "Fiber": None,
                    "GeoSat": None,
                    "NGeoSt": None,
                    "UnlFWA": None,
                    "LicFWA": None,
                    "LBRFWA": None,
                    "Other": None,
                    "TotalServed": 0,
                    "TotalUnderserved": 0,
                    "TotalUnserved": 0,
                    "stats": None
                }
                if "NAME20" in feature["properties"]:
                    del feature["properties"]["NAME20"]
                properties.update({k: v for k, v in feature["properties"].items() if k != "NAME20"})
                feature_dict = {
                    "type": "Feature",
                    "id": geoid20,
                    "properties": properties,
                    "geometry": mapping(shape(feature["geometry"]))
                }
                if not first:
                    f.write(',')
                else:
                    first = False
                json.dump(feature_dict, f, ensure_ascii=False)
        f.write(']}')
    
    logging.debug(f"Tabblock20 GeoJSON written to temp file: {temp_json_file}")
    return temp_json_file

@retry(max_attempts=5, delay=3)
def stream_merge_bdc_stats(tabblock_json_file, bdc_properties, output_file):
    logging.info("Streaming merge of BDC statistics with Tabblock20 data")
    mem_percent = psutil.virtual_memory().percent
    disk_free = shutil.disk_usage(os.path.dirname(output_file)).free / (1024 ** 2)
    logging.debug(f"Starting memory usage: {mem_percent:.1f}%, Disk free: {disk_free:.2f} MB")

    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    temp_file = f"{output_file}.tmp"
    try:
        with fiona.open(tabblock_json_file, 'r') as src:
            expected_features = len(src)
            logging.debug(f"Expected features to process: {expected_features}")
            tabblock_geojson = {'type': 'FeatureCollection', 'features': list(src)}

        chunk_size = 50000
        chunk_num = 0
        features_written_in_chunk = 0
        written_features = 0
        tech_types = [abbr for abbr, _ in TECH_ABBR_MAPPING.values()]  # e.g., ["Copper", "Cable", "Fiber", "GeoSat", "NGeoSt", "UnlFWA", "LicFWA", "LBRFWA", "Other"]

        chunk_dir = os.path.join(os.path.dirname(output_file), "chunks")
        if not os.path.exists(chunk_dir):
            os.makedirs(chunk_dir, exist_ok=True)
        for old_chunk in glob.glob(os.path.join(chunk_dir, "*.chunk*.tmp")):
            os.remove(old_chunk)

        current_chunk_file = os.path.join(chunk_dir, f"{os.path.basename(output_file)}.chunk{chunk_num}.tmp")
        with open(current_chunk_file, 'w', encoding='utf-8') as f:
            f.write('{"type":"FeatureCollection","features":[')
            first_in_file = True

            for i, feature in enumerate(tabblock_geojson['features']):
                if features_written_in_chunk >= chunk_size and i < expected_features - 1:
                    f.write(']}')
                    f.close()
                    chunk_num += 1
                    features_written_in_chunk = 0
                    first_in_file = True
                    current_chunk_file = os.path.join(chunk_dir, f"{os.path.basename(output_file)}.chunk{chunk_num}.tmp")
                    f = open(current_chunk_file, 'w', encoding='utf-8')
                    f.write('{"type":"FeatureCollection","features":[')

                geoid20 = feature['properties']['block_geoid']  # This is GEOID20
                
                updated_feature = {
                    "type": "Feature",
                    "id": geoid20,  # Feature-level id is block_geoid (GEOID20)
                    "properties": {
                        "STATEFP20": feature["properties"].get("STATEFP20"),
                        "COUNTYFP20": feature["properties"].get("COUNTYFP20"),
                        "TRACTCE20": feature["properties"].get("TRACTCE20"),
                        "BLOCKCE20": feature["properties"].get("BLOCKCE20"),
                        "GEOID20": feature["properties"].get("GEOID20"),
                        "GEOIDFQ20": feature["properties"].get("GEOIDFQ20"),
                        "MTFCC20": feature["properties"].get("MTFCC20"),
                        "UR20": feature["properties"].get("UR20"),
                        "UACE20": feature["properties"].get("UACE20"),
                        "FUNCSTAT20": feature["properties"].get("FUNCSTAT20"),
                        "ALAND20": feature["properties"].get("ALAND20"),
                        "AWATER20": feature["properties"].get("AWATER20"),
                        "INTPTLAT20": feature["properties"].get("INTPTLAT20"),
                        "INTPTLON20": feature["properties"].get("INTPTLON20"),
                        "HOUSING20": feature["properties"].get("HOUSING20"),
                        "POP20": feature["properties"].get("POP20"),
                        "Copper": None, "Cable": None, "Fiber": None, "GeoSat": None,
                        "NGeoSt": None, "UnlFWA": None, "LicFWA": None, "LBRFWA": None,
                        "Other": None, "TotalServed": 0, "TotalUnderserved": 0, "TotalUnserved": 0,
                        "stats": None
                    },
                    "geometry": feature["geometry"] if "geometry" in feature else {"type": "Point", "coordinates": [0, 0]}
                }

                if geoid20 in bdc_properties:
                    for key, value in bdc_properties[geoid20].items():
                        if key in updated_feature["properties"]:
                            updated_feature["properties"][key] = value

                # Initialize all technology-specific fields
                for tech in tech_types:
                    updated_feature["properties"].update({
                        f"{tech}_BrandNames": "",
                        f"{tech}_providerIDs": "",
                        f"{tech}_HoldingCompanies": "",
                        f"{tech}_providerCount": 0,
                        f"{tech}_LocationCount": 0,
                        f"{tech}_ServedCount": 0,
                        f"{tech}_UnderservedCount": 0,
                        f"{tech}_Dom_BrandName": None,
                        f"{tech}_Dom_ProviderID": None,
                        f"{tech}_Dom_Holding_Company": None,
                        f"{tech}_Dom_LocationCount": 0,
                        f"{tech}_LocationIDs": ""
                    })

                # Use precomputed values from bdc_properties
                if geoid20 in bdc_properties:
                    for tech in tech_types:
                        if f"{tech}_LocationCount" in bdc_properties[geoid20]:
                            updated_feature["properties"][f"{tech}_LocationCount"] = bdc_properties[geoid20][f"{tech}_LocationCount"]
                        if f"{tech}_ServedCount" in bdc_properties[geoid20]:
                            updated_feature["properties"][f"{tech}_ServedCount"] = bdc_properties[geoid20][f"{tech}_ServedCount"]
                        if f"{tech}_UnderservedCount" in bdc_properties[geoid20]:
                            updated_feature["properties"][f"{tech}_UnderservedCount"] = bdc_properties[geoid20][f"{tech}_UnderservedCount"]
                        if f"{tech}_LocationIDs" in bdc_properties[geoid20]:
                            updated_feature["properties"][f"{tech}_LocationIDs"] = bdc_properties[geoid20][f"{tech}_LocationIDs"]

                # Add detailed technology summaries (without recalculating counts)
                for tech in tech_types:
                    tech_data = updated_feature["properties"].get(tech)
                    if tech_data and isinstance(tech_data, dict) and tech_data:
                        brand_names = []
                        provider_ids = []
                        holding_companies = []
                        location_counts = []

                        for brand_name, provider_data in tech_data.items():
                            brand_names.append(brand_name)
                            provider_ids.append(provider_data["provider_id"])
                            holding_companies.append(provider_data["Holding_Company"])
                            loc_count = provider_data.get("Location_Count", 0)
                            location_counts.append(loc_count)

                        if location_counts:
                            dom_idx = location_counts.index(max(location_counts))
                            dom_brand = brand_names[dom_idx]
                            dom_pid = provider_ids[dom_idx]
                            dom_hc = holding_companies[dom_idx]
                            dom_loc_count = location_counts[dom_idx]
                        else:
                            dom_brand = None
                            dom_pid = None
                            dom_hc = None
                            dom_loc_count = 0

                        updated_feature["properties"].update({
                            f"{tech}_BrandNames": ",".join(brand_names),
                            f"{tech}_providerIDs": ",".join(provider_ids),
                            f"{tech}_HoldingCompanies": ",".join(holding_companies),
                            f"{tech}_providerCount": len(brand_names),
                            f"{tech}_Dom_BrandName": dom_brand,
                            f"{tech}_Dom_ProviderID": dom_pid,
                            f"{tech}_Dom_Holding_Company": dom_hc,
                            f"{tech}_Dom_LocationCount": dom_loc_count,
                        })

                # Calculate Total_LocationCount safely
                all_location_ids = set()
                for tech in tech_types:
                    loc_ids = updated_feature["properties"].get(f"{tech}_LocationIDs", "")
                    if loc_ids:
                        all_location_ids.update(loc_ids.split(","))
                updated_feature["properties"]["Total_LocationCount"] = len(all_location_ids)

                if not first_in_file:
                    f.write(',')
                else:
                    first_in_file = False
                
                json.dump(updated_feature, f, ensure_ascii=False, default=decimal_to_json_serializable)
                features_written_in_chunk += 1
                written_features += 1

                if i % 10000 == 0:
                    logging.debug(f"Processed {i} features; Memory: {psutil.virtual_memory().percent:.1f}%")

            f.write(']}')
            f.close()

            if chunk_num > 0:
                logging.info(f"Merging {chunk_num + 1} chunks into final output")
                final_temp = f"{output_file}.final.tmp"
                with open(final_temp, 'w', encoding='utf-8') as f_final:
                    f_final.write('{"type":"FeatureCollection","features":[')
                    any_features = False
                    for c in range(chunk_num + 1):
                        chunk_file = os.path.join(chunk_dir, f"{os.path.basename(output_file)}.chunk{c}.tmp")
                        with open(chunk_file, 'r', encoding='utf-8') as chunk_f:
                            chunk_data = json.load(chunk_f)
                            for feat in chunk_data.get('features', []):
                                if any_features:
                                    f_final.write(',')
                                json.dump(feat, f_final, ensure_ascii=False, default=decimal_to_json_serializable)
                                any_features = True
                    f_final.write(']}')
                os.rename(final_temp, output_file)
            else:
                os.rename(current_chunk_file, output_file)

    except Exception as e:
        logging.error(f"Error during streaming merge: {str(e)}")
        raise

    finally:
        for chunk_file in glob.glob(os.path.join(chunk_dir, "*.tmp")):
            os.remove(chunk_file)
        if os.path.exists(chunk_dir) and not os.listdir(chunk_dir):
            os.rmdir(chunk_dir)

    logging.info(f"Streamed merged GeoJSON to: {output_file}")
    return output_file