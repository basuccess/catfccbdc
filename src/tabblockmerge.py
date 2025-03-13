# tabblockmerge.py (updated)

import os
import logging
import json
import fiona
from fiona import Env
import psutil
import shutil
import glob
import re
import traceback
from copy import deepcopy
from shapely.geometry import shape, mapping
from functools import wraps
import time
from constant import TECH_ABBR_MAPPING, SERVED_DL_SPEED, SERVED_UL_SPEED, LOW_LATENCY, UNDERSERVED_DL_SPEED, UNDERSERVED_UL_SPEED
from prep import check_required_files, get_state_info

def decimal_to_json_serializable(obj):
    """Convert decimal.Decimal objects to float for JSON serialization."""
    import decimal
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    elif hasattr(obj, '__geo_interface__'):
        return obj.__geo_interface__
    return str(obj)

def sanitize_string(value):
    """Remove control characters from strings, preserving only printable characters."""
    if value is None:
        return None
    if isinstance(value, str):
        return ''.join(c if ord(c) >= 32 or c in '\n\t\r' else '' for c in value)
    return value

def sanitize_feature(feature):
    """Recursively sanitize all string values in a feature dictionary."""
    if isinstance(feature, dict):
        return {k: sanitize_feature(v) for k, v in feature.items()}
    elif isinstance(feature, list):
        return [sanitize_feature(item) for item in feature]
    elif isinstance(feature, str):
        return sanitize_string(feature)
    return feature

def retry(max_attempts=5, delay=3):
    """Retry decorator for handling transient GDAL/Fiona errors."""
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

def extract_geoid20(feature_str):
    """Extract the geoid20 (block_geoid) from a feature string using regex."""
    match = re.search(r'"block_geoid":\s*"(\d+)"', feature_str)
    if match:
        return match.group(1)
    return None

def sanitize_feature_str(feature_str):
    """Sanitize a feature string by replacing invalid control characters with a space."""
    return ''.join(c if ord(c) > 31 or c in '\t\n\r' else ' ' for c in feature_str)

def repair_malformed_number(feature_str):
    """Repair malformed numbers with trailing decimal points by appending '0'."""
    # Match numbers ending with a decimal point (e.g., "123.", "-45.") not followed by a digit
    repaired_str = re.sub(r'(\d+\.)(?![0-9])', r'\g<1>0', feature_str)
    return repaired_str

def extract_features(chunk_content):
    """Extract individual feature strings from a chunk's features array by balancing braces."""
    features_start = chunk_content.find('"features": [') + len('"features": [')
    features_end = chunk_content.rfind(']')
    if features_start == -1 or features_end == -1:
        logging.error("Could not find features array in chunk")
        return []
    features_str = chunk_content[features_start:features_end].strip()
    
    features = []
    brace_count = 0
    start = 0
    in_string = False
    escape = False
    for i, char in enumerate(features_str):
        if char == '"' and not escape:
            in_string = not in_string
        if not in_string:
            if char == '{':
                if brace_count == 0:
                    start = i
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    features.append(features_str[start:i+1])
        escape = char == '\\' and not escape
    return features

def process_tabblock_data(base_dir, state_abbr, temp_dir):
    """Process Tabblock20 data and write it to a temporary JSON file."""
    logging.info(f"Processing Tabblock20 data for state: {state_abbr}")
    _, tabblock_files = check_required_files(base_dir, state_abbr)
    fips, abbr, name = get_state_info(state_abbr)
    state_dir = f"{fips}_{abbr}_{name}"
    
    tabblock_file = os.path.join(base_dir, "USA_Census", state_dir, tabblock_files[0])
    logging.info(f"Reading tabblock file: {tabblock_file}")
    
    temp_json_file = os.path.join(temp_dir, f"tabblock_{state_abbr}.json")
    with open(temp_json_file, 'w', encoding='utf-8') as f:
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
                json.dump(sanitize_feature(feature_dict), f, ensure_ascii=False, default=decimal_to_json_serializable)
        f.write(']}')
    
    logging.debug(f"Tabblock20 GeoJSON written to temp file: {temp_json_file}")
    return temp_json_file

@retry(max_attempts=5, delay=3)
def stream_merge_bdc_stats(tabblock_json_file, bdc_properties, output_file):
    """Stream merge BDC statistics with Tabblock20 data, with enhanced error handling."""
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
        tech_types = [abbr for abbr, _ in TECH_ABBR_MAPPING.values()]

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

                geoid20 = feature['properties']['block_geoid']
                
                updated_feature = {
                    "type": "Feature",
                    "id": geoid20,
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
                
                json.dump(sanitize_feature(updated_feature), f, ensure_ascii=False, default=decimal_to_json_serializable)
                features_written_in_chunk += 1
                written_features += 1

                if i % 10000 == 0:
                    logging.debug(f"Processed {i} features; Memory: {psutil.virtual_memory().percent:.1f}%")

            f.write(']}')
            f.close()

            if chunk_num > 0:
                logging.info(f"Merging {chunk_num + 1} chunks into final output")
                total_chunks = chunk_num + 1
                final_temp = f"{output_file}.final.tmp"
                unrepaired_dir = os.path.join(output_dir, "unrepaired_features")
                os.makedirs(unrepaired_dir, exist_ok=True)
                counter = 0  # For unique naming of unknown geoid20 snippets
                
                with open(final_temp, 'w', encoding='utf-8') as f_final:
                    f_final.write('{"type":"FeatureCollection","features":[')
                    any_features = False
                    for c in range(total_chunks):
                        chunk_file = os.path.join(chunk_dir, f"{os.path.basename(output_file)}.chunk{c}.tmp")
                        try:
                            with open(chunk_file, 'r', encoding='utf-8') as chunk_f:
                                chunk_content = chunk_f.read()
                                try:
                                    chunk_data = json.loads(chunk_content)
                                except json.JSONDecodeError as e:
                                    logging.error(f"Failed to parse chunk {chunk_file}: {str(e)}")
                                    backup_chunk_file = chunk_file + '.backup'
                                    shutil.copy(chunk_file, backup_chunk_file)
                                    logging.info(f"Backed up errored chunk to {backup_chunk_file}")
                                    features = extract_features(chunk_content)
                                    for feature_str in features:
                                        try:
                                            feat = json.loads(feature_str)
                                            if any_features:
                                                f_final.write(',')
                                            json.dump(feat, f_final, ensure_ascii=False)
                                            any_features = True
                                        except json.JSONDecodeError:
                                            # First attempt: repair malformed numbers
                                            repaired_str = repair_malformed_number(feature_str)
                                            try:
                                                feat = json.loads(repaired_str)
                                                if any_features:
                                                    f_final.write(',')
                                                json.dump(feat, f_final, ensure_ascii=False)
                                                any_features = True
                                                logging.info(f"Repaired malformed number in feature from chunk {chunk_file}")
                                            except json.JSONDecodeError:
                                                # Second attempt: sanitize control characters
                                                sanitized_str = sanitize_feature_str(repaired_str)
                                                try:
                                                    feat = json.loads(sanitized_str)
                                                    if any_features:
                                                        f_final.write(',')
                                                    json.dump(feat, f_final, ensure_ascii=False)
                                                    any_features = True
                                                    logging.info(f"Repaired feature in chunk {chunk_file} after sanitization")
                                                except json.JSONDecodeError:
                                                    geoid20 = extract_geoid20(feature_str)
                                                    if geoid20:
                                                        snippet_file = os.path.join(unrepaired_dir, f"{geoid20}_unrepaired.json")
                                                    else:
                                                        snippet_file = os.path.join(unrepaired_dir, f"unknown_{int(time.time())}_{counter}_unrepaired.json")
                                                        counter += 1
                                                    with open(snippet_file, 'w', encoding='utf-8') as f:
                                                        f.write(feature_str)
                                                    logging.warning(
                                                        f"Skipped unrepaired feature from chunk {chunk_file} "
                                                        f"with geoid20={geoid20 or 'unknown'}, saved to {snippet_file}"
                                                    )
                                    continue
                                for feat in chunk_data.get('features', []):
                                    if any_features:
                                        f_final.write(',')
                                    json.dump(sanitize_feature(feat), f_final, ensure_ascii=False, default=decimal_to_json_serializable)
                                    any_features = True
                            logging.info(f"Processed chunk {c + 1} of {total_chunks} ({(c + 1) / total_chunks * 100:.1f}%)")
                        except Exception as e:
                            logging.error(f"Error processing chunk {chunk_file}: {str(e)}", exc_info=True)
                            continue
                    f_final.write(']}')
                os.rename(final_temp, output_file)
            else:
                os.rename(current_chunk_file, output_file)

    except Exception as e:
        logging.error(f"Error during streaming merge: {str(e)}", exc_info=True)
        raise

    finally:
        for chunk_file in glob.glob(os.path.join(chunk_dir, "*.tmp")):
            os.remove(chunk_file)
        if os.path.exists(chunk_dir) and not os.listdir(chunk_dir):
            os.rmdir(chunk_dir)

    logging.info(f"Streamed merged GeoJSON to: {output_file}")
    return output_file