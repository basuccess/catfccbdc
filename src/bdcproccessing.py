# bdcproccessing.py

import pandas as pd
import os
import re
import json  # Import the json module
from concurrent.futures import ProcessPoolExecutor
from constant import TECH_ABBR_MAPPING, BDC_FILE_PATTERN, SERVED_DL_SPEED, SERVED_UL_SPEED, LOW_LATENCY, UNDERSERVED_DL_SPEED, UNDERSERVED_UL_SPEED
import logging

def prepare_dash_dataframe(file_path):
    """
    Prepares a Dash DataFrame with set metadata for reading in the BDC files for a state.
    
    Parameters:
    file_path (str): The path to the BDC file.
    
    Returns:
    pd.DataFrame: A DataFrame with the specified metadata.
    """
    logging.info(f"Preparing Dash DataFrame for file: {file_path}")
    dtype_mapping = {
        'frn': 'string',
        'provider_id': 'int64',
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
    logging.debug(f"DataFrame head for file {file_path}:\n{df.head()}")
    
    # Convert 'low_latency' to boolean
    df['low_latency'] = df['low_latency'].astype(bool)
    
    # Ensure block_geoid is zero-padded to 15 characters
    df['block_geoid'] = df['block_geoid'].apply(lambda x: x.zfill(15))
    
    return df

def read_bdc_files_for_state(state_dir):
    """
    Reads BDC files for a state and processes each file for a specific technology.
    
    Parameters:
    state_dir (str): The directory containing the BDC files for the state.
    
    Returns:
    pd.DataFrame: A concatenated DataFrame with data from all BDC files for the state.
    """
    logging.info(f"Reading BDC files for state directory: {state_dir}")
    bdc_files = [os.path.join(state_dir, f) for f in os.listdir(state_dir) if re.match(BDC_FILE_PATTERN, f)]
    
    for idx, file in enumerate(bdc_files):
        logging.debug(f"BDC files to process: {idx} = '{file}'")
    
    with ProcessPoolExecutor() as executor:
        dataframes = list(executor.map(prepare_dash_dataframe, bdc_files))
    
    concatenated_df = pd.concat(dataframes, ignore_index=True)
    logging.debug(f"Concatenated DataFrame head for state directory {state_dir}:\n{concatenated_df.head()}")
    return concatenated_df

def calculate_service_statistics(df):
    """
    Calculates service statistics for each block_geoid.
    
    Parameters:
    df (pd.DataFrame): The DataFrame containing BDC data.
    
    Returns:
    dict: A dictionary with service statistics for each block_geoid.
    """
    logging.info("Calculating service statistics.")
    flagger_technologies = {key for key, value in TECH_ABBR_MAPPING.items() if value[1]}
    logging.debug(f"Flagger technologies: {flagger_technologies}")
    
    df = df[df['technology'].isin(flagger_technologies)]
    logging.debug(f"Filtered DataFrame head:\n{df.head()}")
    
    def get_service_stats(group):
        location_scores = {}
        
        for _, row in group.iterrows():
            location_id = row['location_id']
            tech_code = row['technology']
            tech_abbr = TECH_ABBR_MAPPING[tech_code][0]
            tech_stat = TECH_ABBR_MAPPING[tech_code][1]
            
            if tech_stat:
                score = 0
                if row['max_advertised_download_speed'] >= SERVED_DL_SPEED and row['max_advertised_upload_speed'] >= SERVED_UL_SPEED and row['low_latency']:
                    score = 2
                elif row['max_advertised_download_speed'] >= UNDERSERVED_DL_SPEED and row['max_advertised_upload_speed'] >= UNDERSERVED_UL_SPEED and row['low_latency']:
                    score = 1
                
                if location_id not in location_scores or location_scores[location_id]['score'] < score:
                    location_scores[location_id] = {
                        'score': score,
                        'business_residential_code': row['business_residential_code']
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
        
        return stats
    
    service_stats = df.groupby('block_geoid').apply(get_service_stats).to_dict()
    logging.debug(f"Service statistics: {json.dumps(service_stats, indent=2)[:1000]}")
    
    return service_stats