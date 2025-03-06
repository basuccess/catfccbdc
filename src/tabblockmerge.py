# tabblockmerge.py (updated)

import os
import logging
import json
import fiona
from fiona import Env
import psutil
import shutil  # Added this import to resolve the NameError
from copy import deepcopy  # Added this import to resolve the NameError
from shapely.geometry import shape, mapping
from functools import wraps
import time
from constant import TECH_ABBR_MAPPING  # Import from constant.py

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

from prep import check_required_files, get_state_info

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

    # Ensure output directory exists
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
            logging.debug(f"Created output directory: {output_dir}")
        except OSError as e:
            logging.error(f"Failed to create output directory {output_dir}: {str(e)}")
            raise

    with fiona.open(tabblock_json_file, 'r') as src:
        tabblock_geojson = {'type': 'FeatureCollection', 'features': list(src)}
    expected_features = len(tabblock_geojson['features'])
    logging.debug(f"Expected features to process: {expected_features}")

    chunk_size = 100000
    chunk_num = 0
    features_written_in_chunk = 0
    written_features = 0
    temp_file = f"{output_file}.tmp"
    header = {"type": "FeatureCollection", "features": []}

    tech_types = [abbr for abbr, _ in TECH_ABBR_MAPPING.values()]

    f = None
    try:
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(json.dumps(header, ensure_ascii=False, indent=2)[:-3])
            f.write('\n  ')
            first = True

            for i, feature in enumerate(tabblock_geojson['features']):
                if not first:
                    f.write(',\n  ')
                else:
                    first = False
                geoid20 = feature['properties']['block_geoid']
                bdc_props = bdc_properties.get(geoid20, {})
                if i == 0 or geoid20 == "440070104002010":
                    logging.debug(f"BDC props for {geoid20} at index {i}: {json.dumps(bdc_props, indent=2)}")

                # Convert feature to a plain dict
                updated_feature = {
                    "type": feature.type,
                    "id": feature["id"],
                    "properties": deepcopy(dict(feature["properties"])),
                    "geometry": shape(feature["geometry"]).__geo_interface__
                }

                location_scores = {}
                for tech_type in tech_types:
                    tech_data = bdc_props.get(tech_type, {})
                    if tech_data and isinstance(tech_data, dict):
                        for brand_name, brand_data in tech_data.items():
                            for category in ["R", "B", "X"]:
                                for record in brand_data.get(category, []):
                                    locations = record["Locations"].split(",")
                                    max_dl = float(record["max_Adv_DL_speed"])
                                    max_ul = float(record["max_Adv_UL_speed"])
                                    low_latency = record["low_latency"] == "1"
                                    for loc_id in locations:
                                        if loc_id not in location_scores:
                                            location_scores[loc_id] = {"category": category, "scores": []}
                                        score = 2 if max_dl >= 100 and max_ul >= 20 and low_latency else 1 if max_dl >= 25 and max_ul >= 3 and low_latency else 0
                                        location_scores[loc_id]["scores"].append(score)
                    elif tech_data:
                        logging.warning(f"Invalid tech_data for {geoid20}.{tech_type}: {tech_data} (type: {type(tech_data)})")

                stats = {
                    "Total BSLs": len(location_scores),
                    "Total Residential BLSs": len({lid for lid, data in location_scores.items() if data['category'] in ["R", "X"]}),
                    "R": {"2": [], "1": [], "0": [], "Served": 0, "Underserved": 0},
                    "B": {"2": [], "1": [], "0": [], "Served": 0, "Underserved": 0},
                    "X": {"2": [], "1": [], "0": [], "Served": 0, "Underserved": 0}
                }

                for loc_id, data in location_scores.items():
                    best_score = max(data["scores"])
                    category = data["category"]
                    stats[category][str(best_score)].append(loc_id)
                    if best_score == 2:
                        stats[category]["Served"] += 1
                    elif best_score == 1:
                        stats[category]["Underserved"] += 1

                total_served = sum(stats[br_code]["Served"] for br_code in ["R", "B", "X"])
                total_underserved = sum(stats[br_code]["Underserved"] for br_code in ["R", "B", "X"])

                techs = {
                    "Copper": [[], [], [], 0, 0, 0, 0, None, None, None, 0, []],
                    "Cable": [[], [], [], 0, 0, 0, 0, None, None, None, 0, []],
                    "Fiber": [[], [], [], 0, 0, 0, 0, None, None, None, 0, []],
                    "FWA": [[], [], [], 0, 0, 0, 0, None, None, None, 0, []],
                    "SAT": [[], [], [], 0, 0, 0, 0, None, None, None, 0, []],
                    "Other": [[], [], [], 0, 0, 0, 0, None, None, None, 0, []]
                }

                for tech_type in tech_types:
                    tech_data = bdc_props.get(tech_type, {})
                    if tech_data and isinstance(tech_data, dict):
                        tech_key = "FWA" if tech_type in ["UnlFWA", "LicFWA", "LBRFWA"] else "SAT" if tech_type in ["GeoSat", "NGeoSt"] else tech_type
                        for brand_name, brand_data in tech_data.items():
                            techs[tech_key][0].append(brand_name)
                            techs[tech_key][1].append(brand_data["provider_id"])
                            techs[tech_key][2].append(brand_data["Holding_Company"])
                            techs[tech_key][3] += 1
                            loc_count = brand_data.get("Location_Count", 0)
                            techs[tech_key][4] += loc_count
                            served = sum(1 for cat in ["R", "B", "X"] for rec in brand_data.get(cat, []) for loc in rec["Locations"].split(",") if loc in stats[cat]["2"])
                            underserved = sum(1 for cat in ["R", "B", "X"] for rec in brand_data.get(cat, []) for loc in rec["Locations"].split(",") if loc in stats[cat]["1"])
                            techs[tech_key][5] += served
                            techs[tech_key][6] += underserved
                            if loc_count > techs[tech_key][10]:
                                techs[tech_key][7] = brand_name
                                techs[tech_key][8] = brand_data["provider_id"]
                                techs[tech_key][9] = brand_data["Holding_Company"]
                                techs[tech_key][10] = loc_count
                            for cat in ["R", "B", "X"]:
                                for rec in brand_data.get(cat, []):
                                    techs[tech_key][11].extend(rec["Locations"].split(","))
                    elif tech_data:
                        logging.warning(f"Invalid tech_data for {geoid20}.{tech_type} in techs: {tech_data} (type: {type(tech_data)})")

                for tech_key in techs:
                    techs[tech_key][11] = sorted(list(set(techs[tech_key][11])))

                stats_json = json.dumps(stats, ensure_ascii=False)

                if "NAME20" in updated_feature["properties"]:
                    del updated_feature["properties"]["NAME20"]
                
                # Update properties with tech-specific fields
                updated_feature['properties'].update({
                    "Copper": bdc_props.get("Copper") if bdc_props.get("Copper") else None,
                    "Cable": bdc_props.get("Cable") if bdc_props.get("Cable") else None,
                    "Fiber": bdc_props.get("Fiber") if bdc_props.get("Fiber") else None,
                    "GeoSat": bdc_props.get("GeoSat") if bdc_props.get("GeoSat") else None,
                    "NGeoSt": bdc_props.get("NGeoSt") if bdc_props.get("NGeoSt") else None,
                    "UnlFWA": bdc_props.get("UnlFWA") if bdc_props.get("UnlFWA") else None,
                    "LicFWA": bdc_props.get("LicFWA") if bdc_props.get("LicFWA") else None,
                    "LBRFWA": bdc_props.get("LBRFWA") if bdc_props.get("LBRFWA") else None,
                    "Other": bdc_props.get("Other") if bdc_props.get("Other") else None,
                    "stats": stats_json
                })

                # Move summary fields directly into properties
                for tech_key in techs:
                    prefix = tech_key
                    updated_feature['properties'][f"{prefix}_BrandNames"] = techs[tech_key][0]
                    updated_feature['properties'][f"{prefix}_providerIDs"] = techs[tech_key][1]
                    updated_feature['properties'][f"{prefix}_HoldingCompanies"] = techs[tech_key][2]
                    updated_feature['properties'][f"{prefix}_providerCount"] = techs[tech_key][3]
                    updated_feature['properties'][f"{prefix}_LocationCount"] = techs[tech_key][4]
                    updated_feature['properties'][f"{prefix}_ServedCount"] = techs[tech_key][5]
                    updated_feature['properties'][f"{prefix}_UnderservedCount"] = techs[tech_key][6]
                    updated_feature['properties'][f"{prefix}_Dom_BrandName"] = techs[tech_key][7]
                    updated_feature['properties'][f"{prefix}_Dom_ProviderID"] = techs[tech_key][8]
                    updated_feature['properties'][f"{prefix}_Dom_Holding_Company"] = techs[tech_key][9]
                    updated_feature['properties'][f"{prefix}_Dom_LocationCount"] = techs[tech_key][10]
                    updated_feature['properties'][f"{prefix}_LocationIDs"] = techs[tech_key][11]
                
                # Add top-level totals directly to properties
                updated_feature['properties']["TotalServed"] = total_served
                updated_feature['properties']["TotalUnderserved"] = total_underserved
                updated_feature['properties']["TotalUnserved"] = max(feature['properties']['HOUSING20'] - total_served - total_underserved, 0)

                if geoid20 == "440070104002010":
                    logging.debug(f"Final feature before write for {geoid20}: {json.dumps(updated_feature, indent=2)}")

                try:
                    shape(updated_feature['geometry'])
                    feature_str = json.dumps(updated_feature, ensure_ascii=False, indent=2)
                    json.loads(feature_str)
                    if not all(k in ['type', 'id', 'properties', 'geometry'] for k in updated_feature) or 'type' not in updated_feature['geometry']:
                        raise ValueError("Invalid GeoJSON structure")
                    if i % 100000 == 0:
                        logging.debug(f"Feature {i} (geoid {geoid20}) validated: {feature_str[:200]}...")
                except Exception as e:
                    logging.error(f"Invalid feature {geoid20} at index {i}: {str(e)} - Skipping")
                    logging.debug(f"Problematic feature (raw): {updated_feature}")
                    continue
                
                try:
                    f.write(feature_str)
                    f.flush()
                    written_features += 1
                    features_written_in_chunk += 1
                    if i % 100000 == 0:
                        logging.debug(f"Feature {i} (geoid {geoid20}) written: {feature_str[:200]}...")
                except IOError as e:
                    logging.error(f"Failed to write feature {geoid20} at index {i}: {str(e)}")
                    raise

                if i % 10000 == 0:
                    mem_percent = psutil.virtual_memory().percent
                    logging.debug(f"Processed {i} features; Memory: {mem_percent:.1f}%")
                
                if features_written_in_chunk >= chunk_size and i < expected_features - 1:
                    f.write('\n  ]')
                    f.flush()
                    f.close()
                    chunk_num += 1
                    temp_chunk_file = f"{output_file}.chunk{chunk_num}.tmp"
                    try:
                        os.rename(temp_file, temp_chunk_file)
                        logging.debug(f"Chunk {chunk_num} written to {temp_chunk_file} with {features_written_in_chunk} features")
                    except OSError as e:
                        logging.error(f"Failed to rename {temp_file} to {temp_chunk_file}: {str(e)}")
                        raise
                    temp_file = f"{output_file}.tmp"
                    f = open(temp_file, 'w', encoding='utf-8')
                    f.write('[\n  ')
                    features_written_in_chunk = 0
                    first = True

            f.write('\n]}')
            logging.debug(f"GeoJSON footer written—features array closed")
            f.close()

            if chunk_num > 0:
                final_temp = output_file + '.final.tmp'
                with open(final_temp, 'w', encoding='utf-8') as f_final:
                    f_final.write(json.dumps(header, ensure_ascii=False, indent=2)[:-3])
                    f_final.write('\n  ')
                    first_feature = True
                    for c in range(chunk_num + 1):
                        chunk_file = f"{output_file}.chunk{c}.tmp" if c > 0 else temp_file
                        with open(chunk_file, 'r') as f_chunk:
                            chunk_text = f_chunk.read()
                            start = chunk_text.index('[') + 1
                            end = chunk_text.rindex(']')
                            features_text = chunk_text[start:end].strip()
                            if features_text:
                                if not first_feature:
                                    f_final.write(',\n  ')
                                f_final.write(features_text)
                                first_feature = False
                        os.remove(chunk_file)
                    f_final.write('\n]}')
                if os.path.exists(output_file):
                    os.remove(output_file)
                try:
                    os.rename(final_temp, output_file)
                    logging.debug(f"Combined {chunk_num + 1} chunks into {output_file}")
                except OSError as e:
                    logging.error(f"Failed to rename {final_temp} to {output_file}: {str(e)}")
                    raise
            else:
                if os.path.exists(output_file):
                    os.remove(output_file)
                try:
                    os.rename(temp_file, output_file)
                    logging.debug(f"Renamed single chunk file to {output_file}")
                except OSError as e:
                    logging.error(f"Failed to rename {temp_file} to {output_file}: {str(e)}")
                    raise

            @retry()
            def validate_geojson(output_file):
                with Env(OGR_GEOJSON_MAX_OBJ_SIZE=0):
                    with fiona.open(output_file, 'r') as src:
                        actual_features = len(list(src))
                        logging.debug(f"Validated {actual_features} features in {output_file}")
                if actual_features != written_features:
                    logging.warning(f"GeoJSON feature count mismatch: Written {written_features}, Found {actual_features}")
                logging.info(f"GeoJSON {output_file} validated with Fiona: {actual_features} features")

            try:
                validate_geojson(output_file)
            except Exception as e:
                logging.error(f"GeoJSON validation failed: {str(e)}")
                raise

    except Exception as e:
        logging.error(f"Error during streaming merge at feature {written_features}: {str(e)}")
        if f and not f.closed:
            f.close()
        if os.path.exists(temp_file):
            logging.warning(f"Preserving temporary file {temp_file} for debugging")
        raise
    finally:
        if f and not f.closed:
            f.close()
        if os.path.exists(temp_file) and not os.path.exists(output_file):
            logging.warning(f"Temporary file {temp_file} exists but final file {output_file} missing—write failed")

    try:
        file_size = os.path.getsize(output_file) / (1024 ** 2)
        logging.debug(f"Wrote {written_features} features; File size: {file_size:.2f} MB")
    except FileNotFoundError:
        logging.error(f"Output file {output_file} not found after processing")
        raise

    if written_features != expected_features:
        logging.warning(f"Feature count mismatch: Expected {expected_features}, Wrote {written_features}")
    
    logging.info(f"Streamed merged GeoJSON to: {output_file}")