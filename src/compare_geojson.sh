#!/bin/bash

file1="48_TX_BB.geojson"
file2="48_TX_BB_4269.geojson"

# Extract properties and geometry
jq '.features[0].properties' "$file1" > file1_properties.json
jq '.features[0].properties' "$file2" > file2_properties.json
jq '.features[0].geometry' "$file1" > file1_geometry.json
jq '.features[0].geometry' "$file2" > file2_geometry.json

# Compare properties
echo "Comparing properties..."
diff file1_properties.json file2_properties.json > properties_diff.txt
if [ -s properties_diff.txt ]; then
  echo "Properties differ. See properties_diff.txt for details."
else
  echo "Properties are identical."
fi

# Compare geometry type
type1=$(jq -r '.type' file1_geometry.json)
type2=$(jq -r '.type' file2_geometry.json)
if [ "$type1" != "$type2" ]; then
  echo "Geometry types differ: $type1 vs $type2"
else
  echo "Geometry types match: $type1"
fi

# Compare number of coordinates
coords1=$(jq '.coordinates[0] | length' file1_geometry.json)
coords2=$(jq '.coordinates[0] | length' file2_geometry.json)
if [ "$coords1" -ne "$coords2" ]; then
  echo "Number of coordinates differ: $coords1 vs $coords2"
else
  echo "Number of coordinates match: $coords1"
fi

# Compare coordinates values (if count matches)
if [ "$coords1" -eq "$coords2" ]; then
  diff file1_geometry.json file2_geometry.json > geometry_diff.txt
  if [ -s geometry_diff.txt ]; then
    echo "Coordinates differ. See geometry_diff.txt for details."
  else
    echo "Coordinates are identical."
  fi
fi
