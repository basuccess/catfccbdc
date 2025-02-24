# bdcprocessing.py

import pandas as pd
import os
import re
import json
import psutil
from concurrent.futures import ProcessPoolExecutor
from constant import TECH_ABBR_MAPPING, BDC_FILE_PATTERN, SERVED_DL_SPEED, SERVED_UL_SPEED, LOW_LATENCY, UNDERSERVED_DL_SPEED, UNDERSERVED_UL_SPEED
import logging
from functions import monitor_memory
from prep import load_holder_mapping
import dask.dataframe as dd
import tempfile
import zipfile

def log_memory_usage(message):
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    logging.debug(f"{message} - Memory usage: {memory_info.rss / 1024 ** 2:.2f} MB")

def prepare_dash_dataframe(file_path, temp_dir):
    logging.info(f"Preparing Dash DataFrame for file: {file_path}")
    log_memory_usage("Before reading CSV")
    monitor_memory()
    dtype_mapping = {
        'frn': 'string',
        'provider_id': 'string',
        'brand_name': 'string',
        'location_id': 'string',
        'technology': 'int64',
        'max_advertised_download_speed': 'int64',
        'max_advertised_upload_speed': 'int64',
        'low_latency': 'int64',
        'business_residential_code': 'category',
        'state_usps': 'category',
        'block_geoid': 'string',
        'h3_res8_id': 'string'
    }

    if file_path.endswith('.zip'):
        with zipfile.ZipFile(file_path, 'r') as zf:
            csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise ValueError(f"No CSV found in ZIP file: {file_path}")
            if len(csv_files) > 1:
                raise ValueError(f"Multiple CSVs in ZIP file not supported: {file_path}")
            csv_file = csv_files[0]
            extracted_path = os.path.join(temp_dir, csv_file)
            zf.extract(csv_file, temp_dir)
            file_path = extracted_path

    total_ram = psutil.virtual_memory().available
    file_size = os.path.getsize(file_path)
    blocksize = min(file_size, total_ram // 4)
    df = dd.read_csv(file_path, dtype=dtype_mapping, blocksize=blocksize)
    df['low_latency'] = df['low_latency'].astype(bool)
    df['block_geoid'] = df['block_geoid'].apply(lambda x: x.zfill(15), meta=('block_geoid', 'string'))
    logging.info(f"Using blocksize: {blocksize / (1024 ** 2):.2f} MB for file size: {file_size / (1024 ** 2):.2f} MB")
    return df.persist()

def create_empty_feature(block_geoid):
    return {
        "type": "Feature",
        "id": str(block_geoid),
        "properties": {
            "block_geoid": str(block_geoid),
            "Copper": {},
            "Cable": {},
            "Fiber": {},
            "GeoSat": {},
            "NGeoSt": {},
            "UnlFWA": {},
            "LicFWA": {},
            "LBRFWA": {},
            "Other": {},
            "TotalServed": 0,
            "TotalUnderserved": 0,
            "TotalUnserved": 0,
            "stats": {
                "R": {"2": "", "1": "", "0": "", "Served": 0, "Underserved": 0},
                "B": {"2": "", "1": "", "0": "", "Served": 0, "Underserved": 0},
                "X": {"2": "", "1": "", "0": "", "Served": 0, "Underserved": 0}
            }
        },
        "geometry": None
    }

def process_bdc_file_chunk(df_chunk, holder_mapping, temp_file):
    summary = {}
    # All tech abbreviations for inclusion in properties
    all_tech_abbrs = [v[0] for v in TECH_ABBR_MAPPING.values()]
    
    for _, row in df_chunk.iterrows():
        block_geoid = row['block_geoid']
        brand_name = row['brand_name']
        provider_id = row['provider_id']
        holding_company = holder_mapping.get(provider_id, "Unknown")
        tech_abbr = TECH_ABBR_MAPPING[row['technology']][0]
        business_residential_code = row['business_residential_code']
        max_adv_dl_speed = str(row['max_advertised_download_speed'])
        max_adv_ul_speed = str(row['max_advertised_upload_speed'])
        low_latency = str(int(row['low_latency']))
        served_location = row['location_id']
        
        if block_geoid not in summary:
            summary[block_geoid] = create_empty_feature(block_geoid)
        
        # Include all tech abbreviations in properties
        if tech_abbr in all_tech_abbrs:
            if tech_abbr not in summary[block_geoid]["properties"]:
                summary[block_geoid]["properties"][tech_abbr] = {}
                
            if brand_name not in summary[block_geoid]["properties"][tech_abbr]:
                summary[block_geoid]["properties"][tech_abbr][brand_name] = {
                    "Holding_Company": holding_company,
                    "R": [],
                    "B": [],
                    "X": []
                }
            
            provider_data = summary[block_geoid]["properties"][tech_abbr][brand_name][business_residential_code]
            existing_record = None
            
            for record in provider_data:
                if (record["max_Adv_DL_speed"] == max_adv_dl_speed and 
                    record["max_Adv_UL_speed"] == max_adv_ul_speed and 
                    record["low_latency"] == low_latency):
                    existing_record = record
                    break
            
            if existing_record:
                locations = set(existing_record["Served_Location"].split(","))
                locations.add(served_location)
                existing_record["Served_Location"] = ",".join(sorted(locations))
            else:
                provider_data.append({
                    "max_Adv_DL_speed": max_adv_dl_speed,
                    "max_Adv_UL_speed": max_adv_ul_speed,
                    "low_latency": low_latency,
                    "Served_Location": served_location
                })
    
    with open(temp_file, 'w') as f:
        json.dump(summary, f)
    return temp_file

def process_bdc_file(file_path, holder_mapping, temp_dir):
    df = prepare_dash_dataframe(file_path, temp_dir)
    chunk_files = []
    for i, partition in enumerate(df.partitions):
        temp_file = os.path.join(temp_dir, f"chunk_{os.path.basename(file_path)}_{i}.json")
        chunk_files.append(process_bdc_file_chunk(partition.compute(), holder_mapping, temp_file))
    return chunk_files

def process_bdc_file_wrapper(args):
    file_path, holder_mapping, temp_dir = args
    return process_bdc_file(file_path, holder_mapping, temp_dir)

def merge_chunk_summaries(chunk_files):
    combined_summary = {}
    # All tech abbreviations for inclusion in properties
    all_tech_abbrs = [v[0] for v in TECH_ABBR_MAPPING.values()]
    
    for chunk_file in chunk_files:
        with open(chunk_file, 'r') as f:
            summary = json.load(f)
            for block_geoid, data in summary.items():
                if block_geoid not in combined_summary:
                    combined_summary[block_geoid] = data
                else:
                    # Only merge valid tech abbreviations
                    for tech_abbr in all_tech_abbrs:
                        if tech_abbr in data["properties"]:
                            if tech_abbr not in combined_summary[block_geoid]["properties"]:
                                combined_summary[block_geoid]["properties"][tech_abbr] = {}
                            for brand_name, provider_data in data["properties"][tech_abbr].items():
                                if brand_name not in combined_summary[block_geoid]["properties"][tech_abbr]:
                                    combined_summary[block_geoid]["properties"][tech_abbr][brand_name] = provider_data
                                else:
                                    for br_code in ["R", "B", "X"]:
                                        existing_records = combined_summary[block_geoid]["properties"][tech_abbr][brand_name][br_code]
                                        new_records = provider_data[br_code]
                                        speed_groups = {}
                                        for record in new_records:
                                            key = (record["max_Adv_DL_speed"], 
                                                   record["max_Adv_UL_speed"], 
                                                   record["low_latency"])
                                            if key not in speed_groups:
                                                speed_groups[key] = set()
                                            speed_groups[key].update(record["Served_Location"].split(","))
                                        
                                        for key, locations in speed_groups.items():
                                            dl_speed, ul_speed, latency = key
                                            matching_record = next(
                                                (r for r in existing_records if r["max_Adv_DL_speed"] == dl_speed and 
                                                 r["max_Adv_UL_speed"] == ul_speed and r["low_latency"] == latency), None)
                                            if matching_record:
                                                existing_locations = set(matching_record["Served_Location"].split(","))
                                                existing_locations.update(locations)
                                                matching_record["Served_Location"] = ",".join(sorted(existing_locations))
                                            else:
                                                existing_records.append({
                                                    "max_Adv_DL_speed": dl_speed,
                                                    "max_Adv_UL_speed": ul_speed,
                                                    "low_latency": latency,
                                                    "Served_Location": ",".join(sorted(locations))
                                                })
    return {"type": "FeatureCollection", "features": list(combined_summary.values())}

def process_bdc_files(base_dir, state_dir):
    logging.info(f"Reading BDC files for state directory: {state_dir}")
    log_memory_usage("Before listing BDC files")
    monitor_memory()
    bdc_files = [os.path.join(state_dir, f) for f in os.listdir(state_dir) if re.match(BDC_FILE_PATTERN, f)]
    log_memory_usage("After listing BDC files")
    monitor_memory()
    
    for idx, file in enumerate(bdc_files):
        logging.debug(f"BDC files to process: {idx} = '{file}'")
    
    holder_mapping = load_holder_mapping(base_dir)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        with ProcessPoolExecutor() as executor:
            args_list = [(f, holder_mapping, temp_dir) for f in bdc_files]
            chunk_files_list = list(executor.map(process_bdc_file_wrapper, args_list))
        
        all_chunk_files = [f for sublist in chunk_files_list for f in sublist]
        feature_collection = merge_chunk_summaries(all_chunk_files)
    
    log_memory_usage("After merging BDC summaries")
    return feature_collection

def calculate_service_statistics(feature_collection):
    logging.info("Calculating service statistics.")
    log_memory_usage("Before filtering technologies")
    monitor_memory()
    
    features = feature_collection["features"]
    # Subset of tech abbreviations for FCC BSL service calculations
    valid_tech_abbrs = [v[0] for v in TECH_ABBR_MAPPING.values() if v[1]]  # Only True-flagged techs
    
    for feature in features:
        block_geoid = feature["id"]
        properties = feature["properties"]
        
        location_scores = {"R": {}, "B": {}, "X": {}}
        
        # Only use valid techs (True in TECH_ABBR_MAPPING) for service stats
        for tech_abbr in valid_tech_abbrs:
            if tech_abbr in properties:
                providers = properties[tech_abbr]
                for brand_name, provider_data in providers.items():
                    for br_code in ["R", "B", "X"]:
                        for record in provider_data[br_code]:
                            served_locations = record["Served_Location"].split(",")
                            for location_id in served_locations:
                                score = 0
                                if (int(record["max_Adv_DL_speed"]) >= SERVED_DL_SPEED and 
                                    int(record["max_Adv_UL_speed"]) >= SERVED_UL_SPEED and 
                                    record["low_latency"] == "1"):
                                    score = 2
                                elif (int(record["max_Adv_DL_speed"]) >= UNDERSERVED_DL_SPEED and 
                                      int(record["max_Adv_UL_speed"]) >= UNDERSERVED_UL_SPEED and 
                                      record["low_latency"] == "1"):
                                    score = 1
                                
                                if (location_id not in location_scores[br_code] or 
                                    location_scores[br_code][location_id]['score'] < score):
                                    location_scores[br_code][location_id] = {
                                        'score': score,
                                        'max_Adv_DL_speed': int(record["max_Adv_DL_speed"]),
                                        'max_Adv_UL_speed': int(record["max_Adv_UL_speed"]),
                                        'low_latency': int(record["low_latency"]),
                                        'brand_name': brand_name
                                    }
                                elif location_scores[br_code][location_id]['score'] == score:
                                    current_speed_sum = (location_scores[br_code][location_id]['max_Adv_DL_speed'] + 
                                                        location_scores[br_code][location_id]['max_Adv_UL_speed'])
                                    new_speed_sum = int(record["max_Adv_DL_speed"]) + int(record["max_Adv_UL_speed"])
                                    if new_speed_sum > current_speed_sum:
                                        location_scores[br_code][location_id] = {
                                            'score': score,
                                            'max_Adv_DL_speed': int(record["max_Adv_DL_speed"]),
                                            'max_Adv_UL_speed': int(record["max_Adv_UL_speed"]),
                                            'low_latency': int(record["low_latency"]),
                                            'brand_name': brand_name
                                        }
        
        stats = {
            "Total BSLs": properties.get("HOUSING20", len(set().union(*[set(loc.keys()) for loc in location_scores.values()]))),
            "Total Residential BLSs": len(set(location_scores["R"].keys()).union(location_scores["X"].keys())),
            "R": {"2": [], "1": [], "0": [], "Served": 0, "Underserved": 0},
            "B": {"2": [], "1": [], "0": [], "Served": 0, "Underserved": 0},
            "X": {"2": [], "1": [], "0": [], "Served": 0, "Underserved": 0}
        }
        
        for br_code in ["R", "B", "X"]:
            for location_id, data in location_scores[br_code].items():
                score = data['score']
                stats[br_code][str(score)].append(location_id)
                if score == 2:
                    stats[br_code]["Served"] += 1
                elif score == 1:
                    stats[br_code]["Underserved"] += 1
        
        for br_code in ["R", "B", "X"]:
            for score in ["2", "1", "0"]:
                stats[br_code][score] = ",".join(stats[br_code][score])
        
        feature["properties"]["stats"] = stats
        feature["properties"]["TotalServed"] = stats["R"]["Served"] + stats["B"]["Served"] + stats["X"]["Served"]
        feature["properties"]["TotalUnderserved"] = stats["R"]["Underserved"] + stats["B"]["Underserved"] + stats["X"]["Underserved"]
        feature["properties"]["TotalUnserved"] = (stats["Total BSLs"] - 
                                                  feature["properties"]["TotalServed"] - 
                                                  feature["properties"]["TotalUnderserved"]) if (stats["Total BSLs"] - 
                                                                                                 feature["properties"]["TotalServed"] - 
                                                                                                 feature["properties"]["TotalUnderserved"] > 0) else 0
    
    log_memory_usage("After calculating service statistics")
    monitor_memory()
    logging.debug(f"Service statistics: {json.dumps(feature_collection['features'][0], indent=2)}")
    
    return feature_collection