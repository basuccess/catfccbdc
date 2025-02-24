# main.py

import geopandas as gpd
import json
import logging
import os
import sys
import gc
import tempfile

from constant import TECH_ABBR_MAPPING, BDC_US_PROVIDER_FILE_PATTERN, \
    BDC_TECH_CODES_FILE_PATTERN, BDC_FILE_PATTERN, TABBLOCK20_FILE_PATTERN
from functions import setup_logging, parse_arguments, expand_state_ranges
from prep import check_required_directories, get_state_info
from bdcprocessing import process_bdc_files, calculate_service_statistics
from tabblockmerge import process_tabblock_data, stream_merge_bdc_stats, save_to_gpkg

def decimal_to_json_serializable(obj):
    """Convert Decimal to int or float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)  # Default to float; int used in specific fields upstream
    return obj

def main():
    args = parse_arguments()
    logging.debug(f"Parsed arguments: {args}")
    
    setup_logging(args.log_file, args.base_dir, args.log_level, args.log_parts)
    logging.info("Logging is set up.")

    base_dir = args.base_dir
    logging.debug(f"Base directory: {base_dir}")
    check_required_directories(base_dir)
    logging.info("Required directories checked.")

    try:
        states_to_process = expand_state_ranges(args.state)
        logging.info(f'States to be processed: {states_to_process}')
    except ValueError as e:
        logging.error("Error processing state ranges")
        logging.debug(f"Exception: {e}")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        for state_abbr in states_to_process:
            state_input_bdc_dir, state_input_tabblock_dir = check_required_directories(base_dir, state_abbr)
            # Default to state_input_bdc_dir unless args.output_dir is valid
            state_output_dir = state_input_bdc_dir
            if args.output_dir and os.path.isdir(os.path.dirname(args.output_dir)):
                state_output_dir = os.path.join(args.output_dir, f"{get_state_info(state_abbr)[0]}_{state_abbr}_{get_state_info(state_abbr)[2]}")
            os.makedirs(state_output_dir, exist_ok=True)

            logging.info(f"Processing BDC files for state: {state_abbr}")
            bdc_feature_collection = process_bdc_files(base_dir, state_input_bdc_dir)
            logging.debug(f"BDC feature collection sample for state {state_abbr}:\n{json.dumps(bdc_feature_collection['features'][:1], indent=2, default=decimal_to_json_serializable)}")

            try:
                service_stats = calculate_service_statistics(bdc_feature_collection)
            except Exception as e:
                logging.error(f"Failed to calculate service statistics for state {state_abbr}: {str(e)}")
                service_stats = {"type": "FeatureCollection", "features": []}
            if service_stats['features']:
                logging.debug(f"Service statistics sample for state {state_abbr}:\n{json.dumps(service_stats['features'][0], indent=2, default=decimal_to_json_serializable)}")
            
            del bdc_feature_collection
            gc.collect()

            logging.info(f"Processing Tabblock20 data for state: {state_abbr}")
            tabblock_json_file = process_tabblock_data(base_dir, state_abbr, temp_dir)

            # Merge BDC stats into Tabblock20
            fips, abbr, name = get_state_info(state_abbr)
            geojson_output_file = os.path.join(state_output_dir, f"{fips.zfill(2)}_{abbr}_BB.geojson")
            stream_merge_bdc_stats(tabblock_json_file, {f['id']: f['properties'] for f in service_stats['features']}, geojson_output_file)
            logging.info(f"Merged GeoJSON saved to: {geojson_output_file}")

            # Convert to GeoPackage
            gdf = gpd.read_file(geojson_output_file)
            gdf.set_crs(epsg=4269, inplace=True)
            gpkg_output_file = os.path.join(state_output_dir, f"{fips.zfill(2)}_{abbr}_BB.gpkg")
            gdf.to_file(gpkg_output_file, driver="GPKG")
            logging.info(f"Merged GeoPackage saved to: {gpkg_output_file}")

            del service_stats, gdf
            gc.collect()
            logging.info(f"Memory cleared after processing state: {state_abbr}")

if __name__ == '__main__':
    main()