# main.py

import geopandas as gpd
import json
import logging
import os
import sys

from constant import TECH_ABBR_MAPPING, BDC_US_PROVIDER_FILE_PATTERN, \
    BDC_TECH_CODES_FILE_PATTERN, BDC_FILE_PATTERN, TABBLOCK20_FILE_PATTERN
from functions import setup_logging, parse_arguments, expand_state_ranges
from prep import check_required_directories, get_state_info
from bdcproccessing import read_bdc_files_for_state, calculate_service_statistics

def main():
    # Parse arguments first
    args = parse_arguments()
    logging.debug(f"Parsed arguments: {args}")
    
    # Set up logging before any logging calls
    setup_logging(args.log_file, args.base_dir, args.log_level, args.log_parts)
    logging.info("Logging is set up.")

    # Check base_dir and required files exists
    base_dir = args.base_dir
    logging.debug(f"Base directory: {base_dir}")
    check_required_directories(base_dir)
    logging.info("Required directories checked.")

    # Now expand state ranges after logging is configured
    try:
        states_to_process = expand_state_ranges(args.state)
        logging.info(f'States to be processed: {states_to_process}')
    except ValueError as e:
        logging.error("Error processing state ranges")
        logging.debug(f"Exception: {e}")
        return

    output_dir = args.output_dir

    # Process BDC files for each state
    for state_abbr in states_to_process:
        # Determine the input directories for the current state
        state_input_bdc_dir, state_input_tabblock_dir = check_required_directories(base_dir, state_abbr)
        logging.debug(f"bdc State input directory: {state_input_bdc_dir}")
        logging.debug(f"tabblock State input directory: {state_input_tabblock_dir}")
    
        if output_dir is None:
            state_output_dir, _ = check_required_directories(base_dir, state_abbr) 
        else:
            state_output_dir = output_dir

        logging.debug(f"State output directory: {state_output_dir}")

        logging.info(f"Processing BDC files for state: {state_abbr}")
        bdc_summary = read_bdc_files_for_state(base_dir, state_input_bdc_dir)
        # Log a sample of the summary dictionary instead of treating it as a DataFrame
        logging.debug(f"BDC summary sample for state {state_abbr}:\n{json.dumps(list(bdc_summary.items())[:5], indent=2)}")

        service_stats = calculate_service_statistics(bdc_summary)
        logging.debug(f"Service statistics sample for state {state_abbr}:\n{json.dumps(service_stats, indent=2)[:1000]}")
        
        # Save the service statistics to a JSON file
        output_file = os.path.join(state_output_dir, f"{state_abbr}_service_statistics.json")
        with open(output_file, 'w') as f:
            json.dump(service_stats, f, indent=2)
        logging.info(f"Service statistics saved to: {output_file}")

if __name__ == '__main__':
    main()