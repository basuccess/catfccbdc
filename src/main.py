# main.py

import geopandas as gpd
import json
import logging
import os
import sys
import gc
import tempfile
import ijson
from decimal import Decimal
from constant import TECH_ABBR_MAPPING
from functions import setup_logging, parse_arguments, expand_state_ranges, retry_io, check_disk_space, track_corruption_stats, report_corruption_stats
from prep import check_required_directories, get_state_info, check_required_files
from bdcprocessing import process_bdc_files, calculate_service_statistics
from tabblockmerge import process_tabblock_data, stream_merge_bdc_stats

# Initialize global corruption stats
corruption_stats = track_corruption_stats()

def decimal_to_json_serializable(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

def repair_malformed_number(value):
    """Repair a malformed number string by appending '0' after a trailing decimal point."""
    if isinstance(value, str) and value.endswith('.'):
        return value + '0'
    return value

def parse_geojson_with_ijson(file_path):
    """Parse GeoJSON file using ijson, handling nested coordinates and properties with error recovery."""
    @retry_io(max_attempts=5, delay=2)
    def safe_open(file_path, mode):
        check_disk_space(file_path)
        return open(file_path, mode)

    with safe_open(file_path, 'rb') as f:
        parser = ijson.parse(f)
        features = []
        current_feature = None
        current_geometry = None
        geometry_stack = []
        coord_stack = []
        properties_stack = []
        feature_count = 0

        while True:
            try:
                for prefix, event, value in parser:
                    if prefix == 'features.item' and event == 'start_map':
                        current_feature = {"type": "Feature", "properties": {}}
                        properties_stack = [current_feature['properties']]
                        geometry_stack = []
                        coord_stack = []
                    elif prefix == 'features.item' and event == 'end_map':
                        if current_feature:
                            if current_geometry and "type" in current_geometry:
                                if "coordinates" not in current_geometry or not isinstance(current_geometry["coordinates"], list):
                                    logging.debug(f"Feature with id {current_feature.get('id', 'unknown')} has invalid geometry: {current_geometry}")
                                    current_geometry = {"type": "GeometryCollection", "geometries": []}
                                current_feature['geometry'] = current_geometry
                            else:
                                logging.debug(f"Feature with id {current_feature.get('id', 'unknown')} has no valid geometry")
                                current_feature['geometry'] = {"type": "GeometryCollection", "geometries": []}
                            features.append(current_feature)
                            feature_count += 1
                            if feature_count % 10000 == 0:
                                logging.debug(f"Parsed {feature_count} features so far")
                        current_feature = None
                        current_geometry = None
                        properties_stack = []
                        geometry_stack = []
                        coord_stack = []
                    elif prefix.startswith('features.item.geometry'):
                        if current_feature is None:
                            continue
                        keys = prefix.split('.')[2:]
                        if event == 'start_map':
                            new_dict = {}
                            if not geometry_stack:
                                current_geometry = new_dict
                                geometry_stack.append(new_dict)
                            else:
                                geometry_stack[-1][keys[-1]] = new_dict
                                geometry_stack.append(new_dict)
                        elif event == 'end_map':
                            if geometry_stack:
                                geometry_stack.pop()
                        elif event == 'start_array':
                            if keys[-1] == 'coordinates':
                                geometry_stack[-1]['coordinates'] = []
                                coord_stack = [geometry_stack[-1]['coordinates']]
                            elif coord_stack:
                                new_array = []
                                coord_stack[-1].append(new_array)
                                coord_stack.append(new_array)
                        elif event == 'end_array':
                            if coord_stack:
                                coord_stack.pop()
                        elif event == 'number':
                            if coord_stack:
                                coord_stack[-1].append(float(value))
                        elif event == 'string':
                            if keys[-1] == 'type':
                                geometry_stack[-1]['type'] = value
                            elif coord_stack:
                                repaired_value = repair_malformed_number(value)
                                coord_stack[-1].append(repaired_value)
                        elif event == 'null':
                            if keys[-1] == 'coordinates':
                                geometry_stack[-1]['coordinates'] = []
                    elif prefix.startswith('features.item.properties'):
                        if current_feature is None:
                            continue
                        keys = prefix.split('.')[2:]
                        if event == 'start_map':
                            if len(keys) > 1:
                                if keys[-1] == 'item':
                                    if not properties_stack or not isinstance(properties_stack[-1], list):
                                        logging.error(f"Expected list at {prefix}")
                                        continue
                                    new_dict = {}
                                    properties_stack[-1].append(new_dict)
                                    properties_stack.append(new_dict)
                                else:
                                    if not properties_stack or not isinstance(properties_stack[-1], dict):
                                        logging.error(f"Expected dict at {prefix}")
                                        continue
                                    new_dict = {}
                                    properties_stack[-1][keys[-1]] = new_dict
                                    properties_stack.append(new_dict)
                        elif event == 'start_array':
                            if keys[-1] == 'item':
                                logging.error(f"Unexpected start_array for 'item' at {prefix}")
                                continue
                            else:
                                if not properties_stack or not isinstance(properties_stack[-1], dict):
                                    logging.error(f"Expected dict at {prefix}")
                                    continue
                                new_array = []
                                properties_stack[-1][keys[-1]] = new_array
                                properties_stack.append(new_array)
                        elif event in ('string', 'number', 'boolean', 'null'):
                            if keys[-1] == 'item':
                                if not properties_stack or not isinstance(properties_stack[-1], list):
                                    logging.error(f"Expected list at {prefix}")
                                    continue
                                properties_stack[-1].append(value)
                            else:
                                if not properties_stack or not isinstance(properties_stack[-1], dict):
                                    logging.error(f"Expected dict at {prefix}")
                                    continue
                                properties_stack[-1][keys[-1]] = value
                        elif event == 'end_map' or event == 'end_array':
                            if properties_stack:
                                properties_stack.pop()
                    elif prefix == 'features.item.id' and event in ('string', 'number'):
                        current_feature['id'] = value
                break
            except ijson.common.IncompleteJSONError as e:
                feature_id = current_feature.get('id', 'unknown') if current_feature else 'unknown'
                estimated_chunk = feature_count // 50000
                logging.error(
                    f"IncompleteJSONError while parsing {file_path} at feature id/geoid20={feature_id}, "
                    f"estimated chunk {estimated_chunk}: {str(e)}"
                )
                if current_feature:
                    logging.warning(f"Skipping feature id/geoid20={feature_id} due to unrepairable error")
                    features.append({"type": "Feature", "id": feature_id, "properties": {"block_geoid": feature_id}, "geometry": {"type": "Point", "coordinates": [0, 0]}})
                    feature_count += 1
                current_feature = None
                current_geometry = None
                properties_stack = []
                geometry_stack = []
                coord_stack = []
                parser = ijson.parse(f)
                continue

        if not features:
            raise ValueError(f"No valid features parsed from {file_path}")
        logging.debug(f"Parsed {len(features)} features from {file_path}")
        return {"type": "FeatureCollection", "features": features}

@retry_io(max_attempts=5, delay=2)
def safe_file_write(filename, data, mode='w', encoding='utf-8'):
    """Safely write data to a file with disk space check."""
    check_disk_space(filename)
    with open(filename, mode, encoding=encoding) as f:
        f.write(data)

def main():
    args = parse_arguments()
    setup_logging(args.log_file, args.base_dir, args.log_level, args.log_parts)
    logging.info("Logging is set up.")

    base_dir = args.base_dir
    check_required_directories(base_dir)

    states_to_process = expand_state_ranges(args.state)
    logging.info(f'States to be processed: {states_to_process}')

    with tempfile.TemporaryDirectory() as temp_dir:
        for state_abbr in states_to_process:
            try:
                state_input_bdc_dir, state_input_tabblock_dir = check_required_directories(base_dir, state_abbr)
                bdc_files, tabblock_files = check_required_files(base_dir, state_abbr)
                logging.info(f"Found {len(bdc_files)} BDC files and {len(tabblock_files)} Tabblock files for state: {state_abbr}")
            except FileNotFoundError as e:
                logging.warning(f"Skipping state {state_abbr} due to missing files: {str(e)}")
                continue

            state_output_dir = state_input_bdc_dir
            if args.output_dir and os.path.isdir(os.path.dirname(args.output_dir)):
                state_output_dir = os.path.join(args.output_dir, f"{get_state_info(state_abbr)[0]}_{state_abbr}_{get_state_info(state_abbr)[2]}")
            os.makedirs(state_output_dir, exist_ok=True)

            bdc_feature_collection = process_bdc_files(base_dir, state_input_bdc_dir)
            service_stats = calculate_service_statistics(bdc_feature_collection)
            
            del bdc_feature_collection
            gc.collect()

            tabblock_json_file = process_tabblock_data(base_dir, state_abbr, temp_dir)
            fips, abbr, name = get_state_info(state_abbr)
            geojson_4269_file = os.path.join(state_output_dir, f"{fips.zfill(2)}_{abbr}_BB_4269.geojson")
            stream_merge_bdc_stats(tabblock_json_file, {f['id']: f['properties'] for f in service_stats['features']}, geojson_4269_file)
            
            del service_stats
            gc.collect()

            geojson_data = parse_geojson_with_ijson(geojson_4269_file)
            valid_features = geojson_data['features']
            gdf = gpd.GeoDataFrame.from_features(valid_features, crs="EPSG:4269")

            if 'GEOID20' in gdf.columns:
                gdf.set_index('GEOID20', inplace=True, drop=False)
            gdf.set_crs(epsg=4269, inplace=True, allow_override=True)
            gdf_4326 = gdf.to_crs(epsg=4326)

            # Ensure JSON fields are strings with double quotes
            for tech in [abbr for abbr, _ in TECH_ABBR_MAPPING.values()]:
                if tech in gdf_4326.columns:
                    gdf_4326[tech] = gdf_4326[tech].apply(
                        lambda x: json.dumps(x, ensure_ascii=False, default=decimal_to_json_serializable) if x is not None else None
                    )

            if 'GEOID20' in gdf_4326.columns:
                gdf_4326['id'] = gdf_4326['GEOID20']

            geojson_output_file = os.path.join(state_output_dir, f"{fips.zfill(2)}_{abbr}_BB.geojson")
            if os.path.exists(geojson_output_file):
                os.remove(geojson_output_file)
            geojson_str = gdf_4326.to_json(indent=None, na="null")
            safe_file_write(geojson_output_file, geojson_str)
            logging.info(f"Final GeoJSON (EPSG:4326) saved to: {geojson_output_file}")

            gpkg_output_file = os.path.join(state_output_dir, f"{fips.zfill(2)}_{abbr}_BB.gpkg")
            layer_name = f"{fips.zfill(2)}_{abbr}_BB"
            if os.path.exists(gpkg_output_file):
                os.remove(gpkg_output_file)
            
            if gdf_4326.index.name == 'GEOID20' and 'GEOID20' in gdf_4326.columns:
                gdf_4326_for_gpkg = gdf_4326.reset_index(drop=True)
            else:
                gdf_4326_for_gpkg = gdf_4326
            gdf_4326_for_gpkg.to_file(
                gpkg_output_file,
                driver="GPKG",
                layer=layer_name,
                layer_options={"OVERWRITE": "YES"}
            )
            logging.info(f"Final GeoPackage (EPSG:4326) saved to: {gpkg_output_file}")

            del gdf, gdf_4326, gdf_4326_for_gpkg
            gc.collect()

        report_corruption_stats(state_output_dir, corruption_stats)

if __name__ == '__main__':
    main()