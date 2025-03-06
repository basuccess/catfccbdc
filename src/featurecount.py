import fiona
import argparse
import os
from constant import STATES_AND_TERRITORIES

def get_state_info(state_abbr):
    for fips, abbr, name in STATES_AND_TERRITORIES:
        if abbr == state_abbr:
            return fips, abbr, name
    return None, None, None

def main():
    parser = argparse.ArgumentParser(description="Count features in a shapefile.")
    parser.add_argument('--base-dir', required=True, help='Base directory containing the shapefiles')
    parser.add_argument('--state', required=True, help='State abbreviation (e.g., TX for Texas)')
    args = parser.parse_args()

    base_dir = args.base_dir
    state_abbr = args.state

    fips, abbr, state_name = get_state_info(state_abbr)
    if not fips:
        print(f"State {state_abbr} not recognized.")
        return

    shapefile_path = os.path.join(base_dir, 'USA_Census', f"{fips}_{abbr}_{state_name}", f'tl_{fips}_tabblock20.shp')

    with fiona.open(shapefile_path, 'r') as src:
        print(f"Shapefile feature count: {len(list(src))}")

if __name__ == "__main__":
    main()