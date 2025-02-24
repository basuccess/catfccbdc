# functions.py

import os
import sys
import logging
import argparse
import psutil
import gc
from constant import STATES_AND_TERRITORIES

def setup_logging(log_file, base_dir, log_level, log_parts):
    log_format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    parts_log_level = getattr(logging, log_level.upper(), logging.INFO)

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file is not None:
        log_file_path = os.path.join(base_dir, log_file)
        log_dir = os.path.dirname(log_file_path)
        try:
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            handlers.append(logging.FileHandler(log_file_path, mode='w'))
        except FileNotFoundError as e:
            logging.error(f"Failed to create log file directory: {e}")
            sys.exit(1)

    # Remove any existing handlers to avoid duplicates
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    

    # Check if log_parts is empty
    if not log_parts:
        # Execute alternative code if log_parts is empty
        logging.basicConfig(level=parts_log_level, format=log_format, handlers=handlers, force=True)
        logging.info("No specific log parts provided, using default or set logging level for all modules.")
    else:
        # Set specific log levels for specified modules
        logging.basicConfig(level=logging.INFO, format=log_format, handlers=handlers, force=True)
        for part in log_parts:
            logger = logging.getLogger(part)
            logger.setLevel(parts_log_level)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Build broadband service geopackage files for US states and territories.')
    parser.add_argument('-d', '--base-dir', type=str, default=os.getcwd(), help='Base directory for data files')
    parser.add_argument('-s', '--state', type=str, nargs='*', default=[state[1] for state in STATES_AND_TERRITORIES], help='State abbreviation(s) to process')
    parser.add_argument('--log-file', type=str, nargs='?', const='catfccbdc_log.log', help='Log file path')
    parser.add_argument('--log-level', type=str, default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--log-parts', type=str, nargs='*', default=[], help='Modules to apply DEBUG logging level to (e.g., main, prep, functions, bdcfunction, tabblockmerge)')
    parser.add_argument('-o', '--output-dir', type=str, help='Output directory for data files')
    parser.add_argument('-u', '--usage', action='store_true', help='Print usage information and exit')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 2.0', help='Print version and exit')
    
    args = parser.parse_args()

    # Check if --usage is present
    if args.usage:
        parser.print_usage()
        sys.exit(0)

    # Check if --state is present without any options
    if '--state' in sys.argv and not args.state:
        print("Error: --state argument requires at least one state abbreviation.")
        parser.print_usage()
        sys.exit(1)

    return args

def expand_state_ranges(states):
    state_abbrs = [state[1] for state in STATES_AND_TERRITORIES]
    expanded_states = []
    
    for state in states:
        if '..' in state:
            start, end = state.split('..')
            if start not in state_abbrs or end not in state_abbrs:
                raise ValueError(f"Invalid state range: {state}")
            start_index = state_abbrs.index(start)
            end_index = state_abbrs.index(end)
            if start_index > end_index:
                raise ValueError(f"Invalid state range: {state}")
            expanded_states.extend(state_abbrs[start_index:end_index + 1])
        else:
            if state not in state_abbrs:
                logging.error(f"Invalid state abbreviation: {state}")
                continue  # Skip invalid state
            expanded_states.append(state)
    
    return expanded_states

def monitor_memory(threshold=80):
    """Monitor memory usage and log a warning if it exceeds the threshold."""
    memory_usage = psutil.virtual_memory().percent
    if memory_usage > threshold:
        logging.warning(f"Memory usage is high: {memory_usage}%")
        gc.collect()  # Force garbage collection

def download_files():
    logging.info("Downloading files.")
    # Placeholder for file download logic
    pass

def process_files():
    logging.info("Processing files.")
    # Placeholder for file processing logic
    pass

def create_geopackage():
    logging.info("Creating GeoPackage.")
    # Placeholder for GeoPackage creation logic
    pass

def cleanup_files():
    logging.info("Cleaning up files.")
    # Placeholder for file cleanup logic
    pass