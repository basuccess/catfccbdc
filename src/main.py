# main.py

import geopandas as gpd
import json
import logging
import os
import sys
import gc
import tempfile
from constant import TECH_ABBR_MAPPING
from functions import setup_logging, parse_arguments, expand_state_ranges
from prep import check_required_directories, get_state_info
from bdcprocessing import process_bdc_files, calculate_service_statistics
from tabblockmerge import process_tabblock_data, stream_merge_bdc_stats

def decimal_to_json_serializable(obj):
    if isinstance(obj, Decimal):
        return float(obj)
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
            # Use state_input_bdc_dir as default, override with args.output_dir if provided and valid
            state_output_dir = state_input_bdc_dir
            if args.output_dir and os.path.isdir(os.path.dirname(args.output_dir)):
                state_output_dir = os.path.join(args.output_dir, f"{get_state_info(state_abbr)[0]}_{state_abbr}_{get_state_info(state_abbr)[2]}")
            os.makedirs(state_output_dir, exist_ok=True)

            logging.info(f"Processing BDC files for state: {state_abbr}")
            bdc_feature_collection = process_bdc_files(base_dir, state_input_bdc_dir)
            logging.debug(f"BDC feature collection sample for state {state_abbr}:\n{json.dumps(bdc_feature_collection['features'][:1], indent=2, default=decimal_to_json_serializable)}")
            if not bdc_feature_collection['features']:
                logging.warning(f"No BDC features processed for state {state_abbr}")

            try:
                service_stats = calculate_service_statistics(bdc_feature_collection)
            except Exception as e:
                logging.error(f"Failed to calculate service statistics for state {state_abbr}: {str(e)}")
                service_stats = {"type": "FeatureCollection", "features": []}
            if service_stats['features']:
                logging.debug(f"Service statistics sample for state {state_abbr}:\n{json.dumps(service_stats['features'][0], indent=2, default=decimal_to_json_serializable)}")
            
            del bdc_feature_collection
            gc.collect()

            # Process Tabblock20 and merge to temporary GeoJSON (EPSG:4269)
            logging.info(f"Processing Tabblock20 data for state: {state_abbr}")
            tabblock_json_file = process_tabblock_data(base_dir, state_abbr, temp_dir)
            fips, abbr, name = get_state_info(state_abbr)
            geojson_4269_file = os.path.join(state_output_dir, f"{fips.zfill(2)}_{abbr}_BB_4269.geojson")
            stream_merge_bdc_stats(tabblock_json_file, {f['id']: f['properties'] for f in service_stats['features']}, geojson_4269_file)
            logging.info(f"Temporary GeoJSON (EPSG:4269) saved to: {geojson_4269_file}")
            
            # Delete service_stats here—done with BDC data
            del service_stats
            gc.collect()
            logging.debug(f"Memory cleared after merging BDC data for state: {state_abbr}")

            # Read, reproject, and write final outputs
            try:
                # Read the 4269 GeoJSON, ensuring nested objects are preserved
                with open(geojson_4269_file, 'r', encoding='utf-8') as f:
                    geojson_data = json.load(f)
                gdf = gpd.GeoDataFrame.from_features(geojson_data['features'], crs="EPSG:4269")
                logging.debug(f"Loaded GeoJSON {geojson_4269_file} into GeoDataFrame with {len(gdf)} features")

                # Verify CRS
                if gdf.crs is None or gdf.crs.to_epsg() != 4269:
                    gdf.set_crs(epsg=4269, inplace=True, allow_override=True)
                    logging.debug("Set CRS to EPSG:4269 with override")
                else:
                    logging.debug("CRS already set to EPSG:4269")

                # Reproject to 4326
                gdf_4326 = gdf.to_crs(epsg=4326)
                logging.debug(f"Reprojected GeoDataFrame to EPSG:4326 for state: {state_abbr}")

                # Write GeoJSON without indentation, preserving nested objects
                geojson_output_file = os.path.join(state_output_dir, f"{fips.zfill(2)}_{abbr}_BB.geojson")
                with open(geojson_output_file, 'w', encoding='utf-8') as f:
                    # Parse the JSON string and extract features, then write as compact JSON
                    geojson_dict = json.loads(gdf_4326.to_json(indent=None, na="null"))
                    f.write(json.dumps(geojson_dict, ensure_ascii=False, indent=None))
                logging.info(f"Final GeoJSON (EPSG:4326) saved to: {geojson_output_file}")
                
                del gdf
                gc.collect()
                logging.debug(f"Memory cleared after writing GeoJSON for state: {state_abbr}")

                # Write final GPKG with overwrite
                gpkg_output_file = os.path.join(state_output_dir, f"{fips.zfill(2)}_{abbr}_BB.gpkg")
                layer_name = f"{fips.zfill(2)}_{abbr}_BB"
                
                # If the GPKG file exists, delete it to avoid layer conflicts
                if os.path.exists(gpkg_output_file):
                    try:
                        os.remove(gpkg_output_file)
                        logging.debug(f"Deleted existing GeoPackage file: {gpkg_output_file}")
                    except OSError as e:
                        logging.error(f"Failed to delete existing GeoPackage {gpkg_output_file}: {str(e)}")
                        raise
                
                # Write new GPKG with explicit overwrite option
                gdf_4326.to_file(
                    gpkg_output_file,
                    driver="GPKG",
                    layer=layer_name,
                    layer_options={"OVERWRITE": "YES"}
                )
                logging.info(f"Final GeoPackage (EPSG:4326) saved to: {gpkg_output_file}")

            except Exception as e:
                logging.error(f"Failed to process GeoJSON/GPKG for {state_abbr}: {str(e)}")
                raise

            # Final cleanup
            del gdf_4326
            gc.collect()
            logging.info(f"Memory cleared after processing state: {state_abbr}")

if __name__ == '__main__':
    main()