# bdcproccessing.py

import pandas as pd
import os
import re
import json  # Import the json module
import psutil  # Import the psutil module for memory monitoring
from concurrent.futures import ProcessPoolExecutor
from constant import TECH_ABBR_MAPPING, BDC_FILE_PATTERN, SERVED_DL_SPEED, SERVED_UL_SPEED, LOW_LATENCY, UNDERSERVED_DL_SPEED, UNDERSERVED_UL_SPEED
import logging
from functions import monitor_memory  # Import the monitor_memory function
from prep import load_holder_mapping  # Import the load_holder_mapping function

def log_memory_usage(message):
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    logging.debug(f"{message} - Memory usage: {memory_info.rss / 1024 ** 2:.2f} MB")

def prepare_dash_dataframe(file_path):
    """
    Prepares a Dash DataFrame with set metadata for reading in the BDC files for a state.
    
    Parameters:
    file_path (str): The path to the BDC file.
    
    Returns:
    pd.DataFrame: A DataFrame with the specified metadata.
    """
    logging.info(f"Preparing Dash DataFrame for file: {file_path}")
    log_memory_usage("Before reading CSV")
    monitor_memory()
    dtype_mapping = {
        'frn': 'string',
        'provider_id': 'string',  # Change to string
        'brand_name': 'string',
        'location_id': 'string',
        'technology': 'int64',
        'max_advertised_download_speed': 'int64',
        'max_advertised_upload_speed': 'int64',
        'low_latency': 'int64',  # 0 - False, 1 - True
        'business_residential_code': 'category',  # B, R, X
        'state_usps': 'category',  # Enumerated string {2}
        'block_geoid': 'string',
        'h3_res8_id': 'string'
    }

    df = pd.read_csv(file_path, dtype=dtype_mapping)
    log_memory_usage("After reading CSV")
    monitor_memory()
    logging.debug(f"DataFrame head for file {file_path}:\n{df.head()}")
    
    # Convert 'low_latency' to boolean
    df['low_latency'] = df['low_latency'].astype(bool)
    
    # Ensure block_geoid is zero-padded to 15 characters
    df['block_geoid'] = df['block_geoid'].apply(lambda x: x.zfill(15))
    
    return df

def process_bdc_file(file_path, holder_mapping):
    """
    Processes a single BDC file and builds a summary record grouped by block_geoid.
    
    Parameters:
    file_path (str): The path to the BDC file.
    holder_mapping (dict): A dictionary mapping provider_id to holding company names.
    
    Returns:
    dict: A dictionary with the summary record for the BDC file.
    """
    df = prepare_dash_dataframe(file_path)
    summary = {}
    
    for _, row in df.iterrows():
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
            summary[block_geoid] = {
                "type": "Feature",
                "id": block_geoid,
                "properties": {
                    "block_geoid": block_geoid
                },
                "geometry": None
            }
        
        if tech_abbr not in summary[block_geoid]["properties"]:
            summary[block_geoid]["properties"][tech_abbr] = {}
            
        if brand_name not in summary[block_geoid]["properties"][tech_abbr]:
            summary[block_geoid]["properties"][tech_abbr][brand_name] = {
                "Holding_Company": holding_company,
                "R": [],
                "B": [],
                "X": []
            }
        
        # Create a service record key for grouping
        service_key = f"{max_adv_dl_speed}_{max_adv_ul_speed}_{low_latency}"
        
        # Check if we have an existing record with the same speeds
        provider_data = summary[block_geoid]["properties"][tech_abbr][brand_name][business_residential_code]
        existing_record = None
        
        for record in provider_data:
            if (record["max_Adv_DL_speed"] == max_adv_dl_speed and 
                record["max_Adv_UL_speed"] == max_adv_ul_speed and 
                record["low_latency"] == low_latency):
                existing_record = record
                break
        
        if existing_record:
            # Append to existing Served_Location
            locations = set(existing_record["Served_Location"].split(","))
            locations.add(served_location)
            existing_record["Served_Location"] = ",".join(sorted(locations))
        else:
            # Create new record
            provider_data.append({
                "max_Adv_DL_speed": max_adv_dl_speed,
                "max_Adv_UL_speed": max_adv_ul_speed,
                "low_latency": low_latency,
                "Served_Location": served_location
            })
    
    return summary

def read_bdc_files_for_state(base_dir, state_dir):
    """
    Reads BDC files for a state and processes each file for a specific technology.
    
    Parameters:
    state_dir (str): The directory containing the BDC files for the state.
    
    Returns:
    dict: A dictionary with the concatenated summary records for all BDC files for the state.
    """
    logging.info(f"Reading BDC files for state directory: {state_dir}")
    log_memory_usage("Before listing BDC files")
    monitor_memory()
    bdc_files = [os.path.join(state_dir, f) for f in os.listdir(state_dir) if re.match(BDC_FILE_PATTERN, f)]
    log_memory_usage("After listing BDC files")
    monitor_memory()
    
    for idx, file in enumerate(bdc_files):
        logging.debug(f"BDC files to process: {idx} = '{file}'")
    
    holder_mapping = load_holder_mapping(base_dir)  # Load the holder mapping dictionary
    
    with ProcessPoolExecutor() as executor:
        summaries = list(executor.map(process_bdc_file, bdc_files, [holder_mapping] * len(bdc_files)))
    
    # Initialize the FeatureCollection
    feature_collection = {
        "type": "FeatureCollection",
        "features": []
    }
    
    # Combine summaries
    combined_summary = {}
    for summary in summaries:
        for block_geoid, data in summary.items():
            if block_geoid not in combined_summary:
                combined_summary[block_geoid] = data
            else:
                for tech_abbr, providers in data["properties"].items():
                    if tech_abbr == "block_geoid":
                        continue
                    
                    if tech_abbr not in combined_summary[block_geoid]["properties"]:
                        combined_summary[block_geoid]["properties"][tech_abbr] = providers
                    else:
                        for brand_name, provider_data in providers.items():
                            if brand_name not in combined_summary[block_geoid]["properties"][tech_abbr]:
                                combined_summary[block_geoid]["properties"][tech_abbr][brand_name] = provider_data
                            else:
                                # Merge records for each business residential code
                                for br_code in ["R", "B", "X"]:
                                    existing_records = combined_summary[block_geoid]["properties"][tech_abbr][brand_name][br_code]
                                    new_records = provider_data[br_code]
                                    
                                    # Group records by speed characteristics
                                    speed_groups = {}
                                    for record in new_records:
                                        key = (record["max_Adv_DL_speed"], 
                                              record["max_Adv_UL_speed"], 
                                              record["low_latency"])
                                        if key not in speed_groups:
                                            speed_groups[key] = set()
                                        speed_groups[key].update(record["Served_Location"].split(","))
                                    
                                    # Update existing records or create new ones
                                    for key, locations in speed_groups.items():
                                        dl_speed, ul_speed, latency = key
                                        matching_record = next(
                                            (r for r in existing_records
                                             if r["max_Adv_DL_speed"] == dl_speed
                                             and r["max_Adv_UL_speed"] == ul_speed
                                             and r["low_latency"] == latency),
                                            None
                                        )
                                        
                                        if matching_record:
                                            # Update existing record
                                            existing_locations = set(matching_record["Served_Location"].split(","))
                                            existing_locations.update(locations)
                                            matching_record["Served_Location"] = ",".join(sorted(existing_locations))
                                        else:
                                            # Create new record
                                            existing_records.append({
                                                "max_Adv_DL_speed": dl_speed,
                                                "max_Adv_UL_speed": ul_speed,
                                                "low_latency": latency,
                                                "Served_Location": ",".join(sorted(locations))
                                            })
    
    # Ensure every tech_abbr in TECH_ABBR_MAPPING is included in the properties
    for feature in combined_summary.values():
        for tech_code, (tech_abbr, _) in TECH_ABBR_MAPPING.items():
            if tech_abbr not in feature["properties"]:
                feature["properties"][tech_abbr] = {}
        
        # Order the properties based on TECH_ABBR_MAPPING
        ordered_properties = {k: feature["properties"][k] for k in [v[0] for v in TECH_ABBR_MAPPING.values()]}
        feature["properties"] = ordered_properties
    
    # Convert the combined summary to a list of features
    feature_collection["features"] = list(combined_summary.values())
    
    return feature_collection

def calculate_service_statistics(feature_collection):
    """
    Calculates service statistics for each block_geoid.
    
    Parameters:
    feature_collection (dict): The FeatureCollection containing BDC data.
    
    Returns:
    dict: A dictionary with service statistics for each block_geoid.
    """
    logging.info("Calculating service statistics.")
    log_memory_usage("Before filtering technologies")
    monitor_memory()
    
    features = feature_collection["features"]
    
    for feature in features:
        block_geoid = feature["id"]
        properties = feature["properties"]
        
        location_scores = {}
        
        for tech_abbr, providers in properties.items():
            if tech_abbr == "block_geoid":
                continue
            
            # Find the tech_stat value for the given tech_abbr
            tech_stat = next((value[1] for key, value in TECH_ABBR_MAPPING.items() if value[0] == tech_abbr), None)
            
            if tech_stat is None:
                logging.warning(f"Technology abbreviation {tech_abbr} not found in TECH_ABBR_MAPPING.")
                continue
            
            for brand_name, provider_data in providers.items():
                for br_code in ["R", "B", "X"]:
                    for record in provider_data[br_code]:
                        served_locations = record["Served_Location"].split(",")
                        for location_id in served_locations:
                            if tech_stat:
                                score = 0
                                if int(record["max_Adv_DL_speed"]) >= SERVED_DL_SPEED and int(record["max_Adv_UL_speed"]) >= SERVED_UL_SPEED and record["low_latency"] == "1":
                                    score = 2
                                elif int(record["max_Adv_DL_speed"]) >= UNDERSERVED_DL_SPEED and int(record["max_Adv_UL_speed"]) >= UNDERSERVED_UL_SPEED and record["low_latency"] == "1":
                                    score = 1
                                
                                if location_id not in location_scores or location_scores[location_id]['score'] < score:
                                    location_scores[location_id] = {
                                        'score': score,
                                        'business_residential_code': br_code,
                                        'max_Adv_DL_speed': int(record["max_Adv_DL_speed"]),
                                        'max_Adv_UL_speed': int(record["max_Adv_UL_speed"]),
                                        'low_latency': int(record["low_latency"]),
                                        'brand_name': brand_name
                                    }
                                else:
                                    # Choose the record with the highest speed sum and low latency
                                    current_record = location_scores[location_id]
                                    current_speed_sum = current_record['max_Adv_DL_speed'] + current_record['max_Adv_UL_speed']
                                    new_speed_sum = int(record["max_Adv_DL_speed"]) + int(record["max_Adv_UL_speed"])
                                    
                                    if (int(record["low_latency"]) == 1 and 
                                        (new_speed_sum > current_speed_sum or 
                                         current_record['low_latency'] == 0)):
                                        location_scores[location_id] = {
                                            'score': score,
                                            'business_residential_code': br_code,
                                            'max_Adv_DL_speed': int(record["max_Adv_DL_speed"]),
                                            'max_Adv_UL_speed': int(record["max_Adv_UL_speed"]),
                                            'low_latency': int(record["low_latency"]),
                                            'brand_name': brand_name
                                        }
        
        stats = {
            "Total BSLs": len(location_scores),
            "Total Residential BLSs": sum(1 for data in location_scores.values() if data['business_residential_code'] in ['R', 'X']),
            "R": {"2": [], "1": [], "0": [], "Served": 0, "Underserved": 0},
            "B": {"2": [], "1": [], "0": [], "Served": 0, "Underserved": 0},
            "X": {"2": [], "1": [], "0": [], "Served": 0, "Underserved": 0}
        }
        
        for location_id, data in location_scores.items():
            code = data['business_residential_code']
            score = data['score']
            stats[code][str(score)].append(location_id)
            if score == 2:
                stats[code]["Served"] += 1
            elif score == 1:
                stats[code]["Underserved"] += 1
        
        # Convert lists to comma-separated strings
        for code in ['R', 'B', 'X']:
            for score in ['2', '1', '0']:
                stats[code][score] = ','.join(stats[code][score])
        
        # Append stats to the feature properties
        feature["properties"]["service_statistics"] = stats
    
    log_memory_usage("After calculating service statistics")
    monitor_memory()
    logging.debug(f"Service statistics: {json.dumps(feature_collection, indent=2)[:1000]}")
    
    return feature_collection