#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <directory_path>"
  exit 1
fi

directory=$1


for file in "$directory"/*.csv; do
  if [ -f "$file" ]; then
    echo "Processing $file"


    total_lines=$(wc -l < "$file")


    start_line=$((total_lines - 20))


    head -n "$start_line" "$file" > "$file.tmp"


    last_value=$(tail -n 21 "$file" | head -n 1 | awk -F, '{print $3}')


    new_value=$((last_value + 1000))


    for ((i=0; i<20; i++)); do
      echo "ENDD,9,$new_value" >> "$file.tmp"
    done


    mv "$file.tmp" "$file"
  else
    echo "No .csv files found in $directory"
  fi
done

echo "Processing complete."