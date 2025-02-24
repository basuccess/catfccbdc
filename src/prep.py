# prep.py

import os
import re
import pandas as pd
import logging
from constant import STATES_AND_TERRITORIES, BDC_US_PROVIDER_FILE_PATTERN, TECH_ABBR_MAPPING, BDC_FILE_PATTERN, TABBLOCK20_FILE_PATTERN

def get_state_info(state_abbr):
    for fips, abbr, name in STATES_AND_TERRITORIES:
        if abbr == state_abbr:
            return fips.zfill(2), abbr, name.replace(' ', '_')  # Ensure FIPS code is zero-padded and spaces are replaced by underscores
    raise ValueError(f"State abbreviation {state_abbr} not found in STATES_AND_TERRITORIES")

def prepare_lookup_tables():
    logging.info("Preparing lookup tables.")
    state_dict = {abbr: (fips, name) for fips, abbr, name in STATES_AND_TERRITORIES}
    logging.debug(f"State lookup table: {state_dict}")
    return state_dict

def load_holder_mapping(base_dir):
    resources_dir = os.path.join(base_dir, 'USA_FCC-bdc', 'resources')
    logging.debug(f"Resources directory: {resources_dir}")
    if not os.path.exists(resources_dir):
        raise FileNotFoundError(f"Resources directory not found: {resources_dir}")
    
    files = [f for f in os.listdir(resources_dir) if re.match(BDC_US_PROVIDER_FILE_PATTERN, f)]
    logging.debug(f"Provider files found: {files}")
    if not files:
        raise FileNotFoundError(f"No files matching pattern {BDC_US_PROVIDER_FILE_PATTERN} found in: {resources_dir}")
    
    files.sort(key=lambda f: re.search(BDC_US_PROVIDER_FILE_PATTERN, f).group(1), reverse=True)
    most_recent_file = files[0]
    holder_mapping_file = os.path.join(resources_dir, most_recent_file)
    logging.debug(f"Most recent provider file: {holder_mapping_file}")
    
    holder_mapping_df = pd.read_csv(holder_mapping_file)
    logging.debug(f"Holder mapping DataFrame head:\n{holder_mapping_df.head()}")
    holder_mapping_df['provider_id'] = holder_mapping_df['provider_id'].astype(str)

    holder_mapping = dict(zip(holder_mapping_df['provider_id'], holder_mapping_df['holding_company']))
    return holder_mapping

def check_required_directories(base_dir, state_abbr=None):
    logging.info("Checking required directories.")
    required_dirs = [
        os.path.join(base_dir, 'USA_FCC-bdc'),
        os.path.join(base_dir, 'USA_Census'),
        os.path.join(base_dir, 'USA_FCC-bdc', 'resources')
    ]

    for directory in required_dirs:
        logging.debug(f"Checking directory: {directory}")
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Required directory does not exist: {directory}")

    if state_abbr:
        logging.debug(f"Checking required directories for state: {state_abbr}")

        fips, abbr, name = get_state_info(state_abbr)
        state_dir = f"{fips}_{abbr}_{name}"
        
        bdc_dir = os.path.join(base_dir, 'USA_FCC-bdc', state_dir)
        tabblock_dir = os.path.join(base_dir, 'USA_Census', state_dir)

        if not os.path.exists(bdc_dir):
            logging.error(f"Required directory not found: {bdc_dir}")
            raise FileNotFoundError(f"Required directory not found: {bdc_dir}")
        else:
            logging.debug(f"BDC directory: {bdc_dir}")
        
        if not os.path.exists(tabblock_dir):
            logging.error(f"Required directory not found: {tabblock_dir}")
            raise FileNotFoundError(f"Required directory not found: {tabblock_dir}")
        else:
            logging.debug(f"Tabblock directory: {tabblock_dir}")
            
        return bdc_dir, tabblock_dir

    else:

        return None, None
    
def check_required_files(base_dir, state_abbr):
        fips, abbr, name = get_state_info(state_abbr)
        state_dir = f"{fips}_{abbr}_{name}"
        
        bdc_dir = os.path.join(base_dir, 'USA_FCC-bdc', state_dir)
        tabblock_dir = os.path.join(base_dir, 'USA_Census', state_dir)

        bdc_files = [f for f in os.listdir(bdc_dir) if re.match(BDC_FILE_PATTERN, f)]
        logging.debug(f"BDC files: {bdc_files}")

        tabblock_files = [f for f in os.listdir(tabblock_dir) if re.match(TABBLOCK20_FILE_PATTERN, f)]
        logging.debug(f"Tabblock files: {tabblock_files}")

        if not bdc_files:
            logging.error(f"No BDC files found in: {bdc_dir}")
            raise FileNotFoundError(f"No BDC files found in: {bdc_dir}")
        
        if not tabblock_files:
            logging.error(f"No Tabblock20 files found in: {tabblock_dir}")
            raise FileNotFoundError(f"No Tabblock20 files found in: {tabblock_dir}")

        return bdc_files, tabblock_files
