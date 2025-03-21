import fiona
import argparse
import os
import logging
from constant import STATES_AND_TERRITORIES
from functions import expand_state_ranges, setup_logging

def get_state_info(state_abbr):
    """Retrieve FIPS code, abbreviation, and name for a given state abbreviation."""
    for fips, abbr, name in STATES_AND_TERRITORIES:
        if abbr == state_abbr:
            return fips, abbr, name
    return None, None, None

def human_readable_size(size_bytes):
    """Convert file size in bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

def process_file(file_path):
    """Process a file (shapefile, GeoPackage, or GeoJSON) and return size and feature types."""
    try:
        with fiona.open(file_path, 'r') as src:
            feature_count = len(list(src))
            file_size = os.path.getsize(file_path)
            human_size = human_readable_size(file_size)
            
            # Count feature types based on geometry
            feature_types = {}
            for feature in src:
                geom_type = feature['geometry']['type']
                feature_types[geom_type] = feature_types.get(geom_type, 0) + 1
            
            feature_summary = ", ".join(f"{k}: {v}" for k, v in feature_types.items())
            filename = os.path.basename(file_path)
            logging.info(f"Processed {file_path} - Size: {human_size}, Features: {feature_count}")
            return filename, human_size, feature_summary
    except Exception as e:
        logging.error(f"Failed to process {file_path}: {e}")
        raise

def main():
    # Customize argument parser to include --tabblock, --geojson, and --gpkg
    parser = argparse.ArgumentParser(description="Count features in shapefiles, GeoPackages, and/or GeoJSON files.")
    parser.add_argument('-d', '--base-dir', type=str, default=os.getcwd(), help='Base directory for data files')
    parser.add_argument('-s', '--state', type=str, nargs='*', default=[state[1] for state in STATES_AND_TERRITORIES], help='State abbreviation(s) to process (e.g., AZ..TX)')
    parser.add_argument('-t', '--tabblock', action='store_true', help='Process USA_Census tl_{fips}_tabblock20.shp files')
    parser.add_argument('-g', '--geojson', action='store_true', help='Process USA_FCC-bdc {fips}_{state_abbr}_BB.geojson files')
    parser.add_argument('-gp', '--gpkg', action='store_true', help='Process USA_FCC-bdc {fips}_{state_abbr}_BB.gpkg files')
    parser.add_argument('--log-file', type=str, nargs='?', const='feature_count_log.log', help='Log file path (enables logging at specified level)')
    parser.add_argument('--log-level', type=str, default='INFO', help='Logging level when --log-file is specified (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--log-parts', type=str, nargs='*', default=[], help='Modules to apply DEBUG logging level to')
    
    args = parser.parse_args()
    
    # Set default logging level to CRITICAL if --log-file is not provided
    if args.log_file is None:
        logging_level = 'CRITICAL'
    else:
        logging_level = args.log_level
    
    # Setup logging with the determined level
    setup_logging(args.log_file, args.base_dir, logging_level, args.log_parts)
    
    logging.info("Starting feature counting script")
    
    # Expand state ranges
    state_abbrs = expand_state_ranges(args.state)
    if not state_abbrs:
        logging.error("No valid states provided to process.")
        print("Error: No valid states provided.")
        return
    
    logging.debug(f"States to process: {', '.join(state_abbrs)}")
    
    base_dir = args.base_dir
    use_tabblock = args.tabblock
    use_geojson = args.geojson
    use_gpkg = args.gpkg
    
    # If no flags are specified, process all three
    if not (use_tabblock or use_geojson or use_gpkg):
        use_tabblock = use_geojson = use_gpkg = True
    
    for state_abbr in state_abbrs:
        logging.info(f"Processing state: {state_abbr}")
        
        fips, abbr, state_name = get_state_info(state_abbr)
        if not fips:
            logging.warning(f"State {state_abbr} not recognized. Skipping.")
            print(f"State {state_abbr} not recognized.")
            continue
        
        # Construct file paths based on state directory
        state_dir = f"{fips}_{state_abbr}_{state_name}"
        files_to_process = []
        
        if use_tabblock:
            tabblock_path = os.path.join(base_dir, 'USA_Census', state_dir, f'tl_{fips}_tabblock20.shp')
            tabblock_relative = os.path.join('USA_Census', state_dir, f'tl_{fips}_tabblock20.shp')
            files_to_process.append((tabblock_path, tabblock_relative))
        
        if use_gpkg:
            gpkg_path = os.path.join(base_dir, 'USA_FCC-bdc', state_dir, f"{fips}_{state_abbr}_BB.gpkg")
            gpkg_relative = os.path.join('USA_FCC-bdc', state_dir, f"{fips}_{state_abbr}_BB.gpkg")
            files_to_process.append((gpkg_path, gpkg_relative))
        
        if use_geojson:
            geojson_path = os.path.join(base_dir, 'USA_FCC-bdc', state_dir, f"{fips}_{state_abbr}_BB.geojson")
            geojson_relative = os.path.join('USA_FCC-bdc', state_dir, f"{fips}_{state_abbr}_BB.geojson")
            files_to_process.append((geojson_path, geojson_relative))
        
        # Process files and collect results
        filenames = []
        sizes = []
        feature_types = []
        for file_path, relative_path in files_to_process:
            if not os.path.exists(file_path):
                logging.warning(f"File not found: {file_path}. Skipping.")
                filenames.append(os.path.basename(relative_path) + " - Missing")
                sizes.append("N/A")
                feature_types.append("N/A")
            else:
                filename, size, feats = process_file(file_path)
                filenames.append(filename)
                sizes.append(size)
                feature_types.append(feats)
        
        # Format and print the combined result
        print(f"File: {', '.join(filenames)}; Size: {', '.join(sizes)}; Feature Types: {', '.join(feature_types)}")

    logging.info("Feature counting completed")

if __name__ == "__main__":
    main()