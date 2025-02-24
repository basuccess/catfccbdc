# tabblockmerge.py

import os
import logging
import geopandas as gpd
import json
import fiona
import tempfile
from shapely.geometry import shape, mapping
from collections import OrderedDict

from prep import check_required_files, get_state_info

def process_tabblock_data(base_dir, state_abbr, temp_dir):
    logging.info(f"Processing Tabblock20 data for state: {state_abbr}")
    _, tabblock_files = check_required_files(base_dir, state_abbr)
    fips, abbr, name = get_state_info(state_abbr)
    state_dir = f"{fips}_{abbr}_{name}"
    
    tabblock_file = os.path.join(base_dir, 'USA_Census', state_dir, tabblock_files[0])
    logging.info(f"Reading tabblock file: {tabblock_file}")
    
    # Stream features using Fiona
    temp_json_file = os.path.join(temp_dir, f"tabblock_{state_abbr}.json")
    with open(temp_json_file, 'w') as f:
        f.write('{"type": "FeatureCollection", "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4269"}}, "features": [')
        first = True
        with fiona.open(tabblock_file, 'r') as src:
            for feature in src:
                # Convert to GeoJSON-like structure
                geoid20 = feature["properties"]["GEOID20"]
                properties = OrderedDict({
                    "block_geoid": geoid20,
                    "Copper": {},
                    "Cable": {},
                    "Fiber": {},
                    "GeoSat": {},
                    "NGeoSt": {},
                    "UnlFWA": {},
                    "LicFWA": {},
                    "LBRFWA": {},
                    "Other": {},
                    "TotalServed": 0,
                    "TotalUnderserved": 0,
                    "TotalUnserved": 0,
                    "stats": {
                        "R": {"2": "", "1": "", "0": "", "Served": 0, "Underserved": 0},
                        "B": {"2": "", "1": "", "0": "", "Served": 0, "Underserved": 0},
                        "X": {"2": "", "1": "", "0": "", "Served": 0, "Underserved": 0}
                    }
                })
                # Remove NAME20 if it exists
                if "NAME20" in feature["properties"]:
                    del feature["properties"]["NAME20"]
                properties.update({k: v for k, v in feature["properties"].items() if k != "NAME20"})
                feature_dict = {
                    "type": "Feature",
                    "id": geoid20,
                    "properties": properties,
                    "geometry": mapping(shape(feature["geometry"]))  # Convert geometry to GeoJSON-like dict
                }
                # Transform geometry to EPSG:4269 (assumes source is compatible, e.g., NAD83)
                # Note: Fiona doesn’t transform CRS; we’ll handle in GPKG later
                if not first:
                    f.write(',')
                else:
                    first = False
                json.dump(feature_dict, f)
        f.write(']}')
    
    logging.debug(f"Tabblock20 GeoJSON written to temp file: {temp_json_file}")
    return temp_json_file

def stream_merge_bdc_stats(tabblock_json_file, bdc_properties, output_file):
    logging.info("Streaming merge of BDC statistics with Tabblock20 data")
    
    with open(tabblock_json_file, 'r') as tf:
        tabblock_geojson = json.load(tf)
    
    with open(output_file, 'w') as f:
        f.write('{"type": "FeatureCollection", "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4269"}}, "features": [')
        first = True
        
        for feature in tabblock_geojson['features']:
            geoid20 = feature['properties']['block_geoid']
            bdc_props = bdc_properties.get(geoid20, {})
            
            if not first:
                f.write(',')
            else:
                first = False
                
            feature["id"] = geoid20
            # Ensure "NAME20" and others are excluded
            if "NAME20" in feature["properties"]:
                del feature["properties"]["NAME20"]

            feature['properties'].update({
                "Copper": bdc_props.get("Copper", {}),
                "Cable": bdc_props.get("Cable", {}),
                "Fiber": bdc_props.get("Fiber", {}),
                "GeoSat": bdc_props.get("GeoSat", {}),
                "NGeoSt": bdc_props.get("NGeoSt", {}),
                "UnlFWA": bdc_props.get("UnlFWA", {}),
                "LicFWA": bdc_props.get("LicFWA", {}),
                "LBRFWA": bdc_props.get("LBRFWA", {}),
                "Other": bdc_props.get("Other", {}),
                "stats": bdc_props.get("stats", {
                    "Total BSLs": feature['properties']['HOUSING20'],
                    "Total Residential BLSs": 0,
                    "R": {"2": "", "1": "", "0": "", "Served": 0, "Underserved": 0},
                    "B": {"2": "", "1": "", "0": "", "Served": 0, "Underserved": 0},
                    "X": {"2": "", "1": "", "0": "", "Served": 0, "Underserved": 0}
                }),
                "TotalServed": bdc_props.get("TotalServed", 0),
                "TotalUnderserved": bdc_props.get("TotalUnderserved", 0)
            })
            total_served = feature['properties']['TotalServed']
            total_underserved = feature['properties']['TotalUnderserved']
            housing20 = feature['properties']['HOUSING20']
            feature['properties']['TotalUnserved'] = max(housing20 - total_served - total_underserved, 0)
            feature['properties']['stats']['Total BSLs'] = housing20
            
            json.dump(feature, f)
        
        f.write(']}')
        f.flush()  # Ensure all data is written to the file
        os.fsync(f.fileno())  # Ensure the file is fully written to disk
    logging.info(f"Streamed merged GeoJSON to: {output_file}")

def save_to_gpkg(geojson_file, gpkg_file):
    logging.info(f"Saving GeoJSON to GPKG: {gpkg_file}")
    gdf = gpd.read_file(geojson_file)
    gdf.to_file(gpkg_file, driver="GPKG")
    logging.info(f"Saved GeoJSON to GPKG: {gpkg_file}")