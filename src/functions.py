# functions.py
# Functions for building broadband service geopackage files for US states and territories.

import os
import sys
import logging
import argparse
import psutil
import gc
from constant import STATES_AND_TERRITORIES
import shutil
import time
from functools import wraps
from datetime import datetime  # Added for timestamp

def retry_io(max_attempts=5, delay=2):
    """Retry decorator for I/O operations."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except (IOError, OSError) as e:
                    attempts += 1
                    if attempts == max_attempts:
                        logging.error(f"Failed {func.__name__} after {max_attempts} attempts: {e}")
                        raise
                    logging.warning(f"Retry {attempts}/{max_attempts} for {func.__name__}: {e}")
                    time.sleep(delay)
        return wrapper
    return decorator

def check_disk_space(path, min_mb=100):
    """Ensure sufficient disk space before writing."""
    free_mb = shutil.disk_usage(os.path.dirname(path)).free / (1024 ** 2)
    if free_mb < min_mb:
        raise OSError(f"Insufficient disk space: {free_mb:.2f} MB available, {min_mb} MB required")

def track_corruption_stats():
    """Global stats tracker for corruption handling."""
    return {
        "chunks_errored": 0,
        "chunks_backed_up": 0,
        "subchunks_processed": 0,
        "subchunks_errored": 0,
        "features_attempted": 0,
        "features_fixed": 0,
        "features_failed": 0
    }

corruption_stats = track_corruption_stats()

def report_corruption_stats(output_dir, stats):
    """Log a summary report of corruption handling and append to corruption_history.log in output_dir."""
    # Log to existing logging handlers (console and/or main log file)
    logging.info(f"=== Corruption Handling Summary for {output_dir} ===")
    for key, value in stats.items():
        logging.info(f"{key.replace('_', ' ').title()}: {value}")
    
    # Append to corruption_history.log in the state-specific output_dir
    corruption_log_file = os.path.join(output_dir, "corruption_history.log")
    try:
        with open(corruption_log_file, 'a', encoding='utf-8') as f:  # 'a' for append mode
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"{timestamp} - Corruption Handling Summary for {output_dir}\n")
            for key, value in stats.items():
                f.write(f"{key.replace('_', ' ').title()}: {value}\n")
            f.write("\n")  # Add a blank line between entries for readability
    except IOError as e:
        logging.error(f"Failed to append to {corruption_log_file}: {e}")

def setup_logging(log_file, base_dir, log_level, log_parts):
    log_format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s'
    parts_log_level = getattr(logging, log_level.upper(), logging.INFO)

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file is not None:
        log_file_path = os.path.join(base_dir, log_file)
        log_dir = os.path.dirname(log_file_path)
        try:
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            file_handler = logging.FileHandler(log_file_path, mode='w')
            file_handler.setLevel(parts_log_level)
            handlers.append(file_handler)
        except FileNotFoundError as e:
            logging.error(f"Failed to create log file directory: {e}")
            sys.exit(1)

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(level=parts_log_level, format=log_format, handlers=handlers, force=True)

    if not log_parts:
        logging.info("No specific log parts provided, using default or set logging level for all modules.")
    else:
        for part in log_parts:
            logger = logging.getLogger(part.strip())
            logger.setLevel(parts_log_level)
        logging.info(f"Set logging level {log_level} for parts: {', '.join(log_parts)}")

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

    if args.usage:
        parser.print_usage()
        sys.exit(0)

    if '--state' in sys.argv and not args.state:
        print("Error: --state argument requires at least one state abbreviation.")
        parser.print_usage()
        sys.exit(1)

    return args

def expand_state_ranges(state_inputs):
    """Expand state ranges (e.g., 'AL..AZ') into a list of state abbreviations."""
    state_abbrs = [state[1] for state in STATES_AND_TERRITORIES]
    expanded_states = []
    
    for state_input in state_inputs:
        if '..' in state_input:
            start, end = state_input.split('..')
            if start in state_abbrs and end in state_abbrs:
                start_idx = state_abbrs.index(start)
                end_idx = state_abbrs.index(end) + 1
                expanded_states.extend(state_abbrs[start_idx:end_idx])
            else:
                logging.warning(f"Invalid state range: {state_input}. Skipping.")
                print(f"Invalid state range: {state_input}. Skipping.")
        else:
            if state_input in state_abbrs:
                expanded_states.append(state_input)
            else:
                logging.warning(f"Invalid state: {state_input}. Skipping.")
                print(f"Invalid state: {state_input}. Skipping.")
    
    return expanded_states

def monitor_memory(threshold=80):
    memory_usage = psutil.virtual_memory().percent
    if memory_usage > threshold:
        logging.warning(f"Memory usage is high: {memory_usage}%")
        gc.collect()

def download_files():
    logging.info("Downloading files.")
    pass

def process_files():
    logging.info("Processing files.")
    pass

def create_geopackage():
    logging.info("Creating GeoPackage.")
    pass

def cleanup_files():
    logging.info("Cleaning up files.")
    pass