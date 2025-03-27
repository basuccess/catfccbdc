# tabblockmerge.py (updated with chunk error correction restored)

import os
import logging
import json
import fiona
from fiona import Env
import psutil
import shutil
import glob
import re
from shapely.geometry import shape, mapping
from functools import wraps
import time
from constant import TECH_ABBR_MAPPING
from prep import check_required_files, get_state_info
from functions import retry_io, check_disk_space, track_corruption_stats, report_corruption_stats

# Initialize global corruption stats
corruption_stats = track_corruption_stats()

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
    return re.sub(r'(\d+\.)(?![0-9])', r'\g<1>0', feature_str)

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

@retry_io(max_attempts=5, delay=2)
def process_tabblock_data(base_dir, state_abbr, temp_dir):
    """Process Tabblock20 data and write it to a temporary JSON file with robust I/O."""
    logging.info(f"Processing Tabblock20 data for state: {state_abbr}")
    _, tabblock_files = check_required_files(base_dir, state_abbr)
    fips, abbr, name = get_state_info(state_abbr)
    state_dir = f"{fips}_{abbr}_{name}"
    
    tabblock_file = os.path.join(base_dir, "USA_Census", state_dir, tabblock_files[0])
    logging.info(f"Reading tabblock file: {tabblock_file}")
    
    temp_json_file = os.path.join(temp_dir, f"tabblock_{state_abbr}.json")
    check_disk_space(temp_json_file)
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
    global corruption_stats

    logging.info("Streaming merge of BDC statistics with Tabblock20 data")
    mem_percent = psutil.virtual_memory().percent
    disk_free = shutil.disk_usage(os.path.dirname(output_file)).free / (1024 ** 2)
    logging.debug(f"Starting memory usage: {mem_percent:.1f}%, Disk free: {disk_free:.2f} MB")

    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)
    check_disk_space(output_file)

    chunk_dir = os.path.join(output_dir, "chunks")
    error_dir = os.path.join(output_dir, "errored_features")
    os.makedirs(chunk_dir, exist_ok=True)
    os.makedirs(error_dir, exist_ok=True)
    for old_file in glob.glob(os.path.join(chunk_dir, "*.tmp")) + glob.glob(os.path.join(error_dir, "*.json")):
        os.remove(old_file)

    chunk_size = 50000 if mem_percent < 70 else 25000
    chunk_num = 0
    features_written_in_chunk = 0
    written_features = 0
    tech_types = [abbr for abbr, _ in TECH_ABBR_MAPPING.values()]

    def process_feature(feature, bdc_properties, tech_types):
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
                for key in [f"{tech}_{suffix}" for suffix in ["LocationCount", "ServedCount", "UnderservedCount", "LocationIDs"]]:
                    if key in bdc_properties[geoid20]:
                        updated_feature["properties"][key] = bdc_properties[geoid20][key]

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
                    updated_feature["properties"].update({
                        f"{tech}_BrandNames": ",".join(brand_names),
                        f"{tech}_providerIDs": ",".join(provider_ids),
                        f"{tech}_HoldingCompanies": ",".join(holding_companies),
                        f"{tech}_providerCount": len(brand_names),
                        f"{tech}_Dom_BrandName": brand_names[dom_idx],
                        f"{tech}_Dom_ProviderID": provider_ids[dom_idx],
                        f"{tech}_Dom_Holding_Company": holding_companies[dom_idx],
                        f"{tech}_Dom_LocationCount": location_counts[dom_idx],
                    })

        all_location_ids = set()
        for tech in tech_types:
            loc_ids = updated_feature["properties"].get(f"{tech}_LocationIDs", "")
            if loc_ids:
                all_location_ids.update(loc_ids.split(","))
        updated_feature["properties"]["Total_LocationCount"] = len(all_location_ids)
        return updated_feature

    def process_subchunk(subchunk_content, subchunk_file, error_dir, f_final, any_features_ref):
        """Process a sub-chunk feature-by-feature with repair attempts."""
        features = extract_features(subchunk_content)
        logging.info(f"Processing subchunk with {len(features)} features feature-by-feature")
        with open(subchunk_file, 'w', encoding='utf-8') as f:
            f.write('{"type":"FeatureCollection","features":[')
            first = True
            for i, feature_str in enumerate(features):
                corruption_stats["features_attempted"] += 1
                try:
                    feat = json.loads(feature_str)
                    if not first:
                        f.write(',')
                    json.dump(sanitize_feature(feat), f, ensure_ascii=False, default=decimal_to_json_serializable)
                    first = False
                    corruption_stats["features_fixed"] += 1
                except json.JSONDecodeError:
                    corruption_stats["features_failed"] += 1
                    repaired_str = repair_malformed_number(feature_str)
                    try:
                        feat = json.loads(repaired_str)
                        if not first:
                            f.write(',')
                        json.dump(sanitize_feature(feat), f, ensure_ascii=False, default=decimal_to_json_serializable)
                        first = False
                        corruption_stats["features_fixed"] += 1
                        fix_type = "number_repair"
                        success = True
                    except json.JSONDecodeError:
                        sanitized_str = sanitize_feature_str(repaired_str)
                        try:
                            feat = json.loads(sanitized_str)
                            if not first:
                                f.write(',')
                            json.dump(sanitize_feature(feat), f, ensure_ascii=False, default=decimal_to_json_serializable)
                            first = False
                            corruption_stats["features_fixed"] += 1
                            fix_type = "sanitize"
                            success = True
                        except json.JSONDecodeError:
                            fix_type = "none"
                            success = False
                    geoid20 = extract_geoid20(feature_str) or f"unknown_{int(time.time())}_{i}"
                    snippet_file = os.path.join(error_dir, f"{geoid20}_fix_{fix_type}_success_{success}.json")
                    with open(snippet_file, 'w', encoding='utf-8') as sf:
                        sf.write(feature_str)
                    logging.debug(f"Stored feature snippet: {snippet_file}")
            f.write(']}')
        with open(subchunk_file, 'r', encoding='utf-8') as sf:
            subchunk_data = json.loads(sf.read())
            for feat in subchunk_data.get('features', []):
                if any_features_ref[0]:
                    f_final.write(',')
                json.dump(sanitize_feature(feat), f_final, ensure_ascii=False, default=decimal_to_json_serializable)
                any_features_ref[0] = True
        return not first

    with fiona.open(tabblock_json_file, 'r') as src:
        expected_features = len(src)
        logging.debug(f"Expected features: {expected_features}")
        tabblock_geojson = {'type': 'FeatureCollection', 'features': list(src)}

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

            updated_feature = process_feature(feature, bdc_properties, tech_types)
            if not first_in_file:
                f.write(',')
            json.dump(sanitize_feature(updated_feature), f, ensure_ascii=False, default=decimal_to_json_serializable)
            first_in_file = False
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
            any_features = [False]

            for c in range(chunk_num + 1):
                chunk_file = os.path.join(chunk_dir, f"{os.path.basename(output_file)}.chunk{c}.tmp")
                try:
                    with open(chunk_file, 'r', encoding='utf-8') as chunk_f:
                        chunk_content = chunk_f.read()
                        try:
                            chunk_data = json.loads(chunk_content)
                            for feat in chunk_data.get('features', []):
                                if any_features[0]:
                                    f_final.write(',')
                                json.dump(sanitize_feature(feat), f_final, ensure_ascii=False, default=decimal_to_json_serializable)
                                any_features[0] = True
                        except json.JSONDecodeError as e:
                            corruption_stats["chunks_errored"] += 1
                            logging.warning(f"Chunk {chunk_file} errored: {e}. Backing up and subdividing.")
                            backup_file = f"{chunk_file}.backup"
                            shutil.copy(chunk_file, backup_file)
                            corruption_stats["chunks_backed_up"] += 1
                            logging.info(f"Backed up chunk to {backup_file}")

                            features = extract_features(chunk_content)
                            subchunk_size = max(1, len(features) // 10)
                            logging.info(f"Subdividing chunk {c} into subchunks of size {subchunk_size}")
                            for sub_idx in range(0, len(features), subchunk_size):
                                subchunk_features = features[sub_idx:sub_idx + subchunk_size]
                                subchunk_content = f'{{"type":"FeatureCollection","features":[{",".join(subchunk_features)}]}}'
                                subchunk_file = os.path.join(chunk_dir, f"{os.path.basename(chunk_file)}.subchunk{sub_idx}.tmp")
                                with open(subchunk_file, 'w', encoding='utf-8') as sf:
                                    sf.write(subchunk_content)
                                corruption_stats["subchunks_processed"] += 1

                                try:
                                    subchunk_data = json.loads(subchunk_content)
                                    for feat in subchunk_data.get('features', []):
                                        if any_features[0]:
                                            f_final.write(',')
                                        json.dump(sanitize_feature(feat), f_final, ensure_ascii=False, default=decimal_to_json_serializable)
                                        any_features[0] = True
                                except json.JSONDecodeError:
                                    corruption_stats["subchunks_errored"] += 1
                                    logging.debug(f"Subchunk {subchunk_file} failed. Switching to feature-by-feature.")
                                    process_subchunk(subchunk_content, subchunk_file, error_dir, f_final, any_features)
                except Exception as e:
                    logging.error(f"Failed to process chunk {chunk_file}: {str(e)}")
                    continue
                logging.info(f"Processed chunk {c + 1}/{chunk_num + 1}")
            f_final.write(']}')
        os.rename(final_temp, output_file)
    else:
        os.rename(current_chunk_file, output_file)

    for chunk_file in glob.glob(os.path.join(chunk_dir, "*.tmp")):
        os.remove(chunk_file)
    if os.path.exists(chunk_dir) and not os.listdir(chunk_dir):
        os.rmdir(chunk_dir)
    if os.path.exists(error_dir) and not os.listdir(error_dir):
        os.rmdir(error_dir)

    logging.info(f"Streamed merged GeoJSON to: {output_file}")
    report_corruption_stats(output_dir, corruption_stats)
    return output_file