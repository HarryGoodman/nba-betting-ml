#!/bin/bash

# Define directories for input and output data
INPUT_DIR="data/team"
OUTPUT_DIR="data/elo"

# Create the output directory if it doesn't exist
mkdir -p $OUTPUT_DIR

# Get the current year
current_year=$(date +"%Y")

# Calculate the start year for 20 seasons ago
start_year=$((current_year - 20))

# Loop through each season and call the Python script
for ((year=$start_year; year<current_year; year++)); do
    # Calculate the next year for the season format
    next_year=$((year + 1))

    # Format the season (e.g., '2004-05')
    season="${year}-${next_year: -2}"

    # Format the input and output file paths
    input_filepath="${INPUT_DIR}/season_team_${year}${next_year: -2}.parquet"
    output_filepath="${OUTPUT_DIR}/season_elo_${year}${next_year: -2}.parquet"

    # Call the Python script to compute Elo ratings
    echo "Computing Elo ratings for season $season..."
    python utils/save_season_elo.py --input "$input_filepath" --output "$output_filepath"

    # Check if the Python script ran successfully
    if [ $? -ne 0 ]; then
        echo "Failed to compute Elo ratings for season $season. Exiting."
        exit 1
    fi
done

echo "Elo ratings computed for all seasons."
