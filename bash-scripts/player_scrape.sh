#!/bin/bash

# Define the directory where you want to save the data
SAVE_DIR="data/player"

# Create the directory if it doesn't exist
mkdir -p $SAVE_DIR

# Get the current year
current_year=$(date +"%Y")

# Calculate the start year for 20 seasons ago
start_year=$((current_year - 20))

# Loop through each season and call the Python script
for ((year=$start_year; year<current_year; year++)); do
    # Calculate the next year for the season format
    next_year=$((year + 1))

    # Format the season (e.g., '2023-24')
    season="${year}-${next_year: -2}"

    # Format the file name (e.g., 'season_player_202324.parquet')
    filepath="${SAVE_DIR}/season_player_${year}${next_year: -2}.parquet"

    # Call the Python script
    echo "Scraping player data for season $season..."
    python utils/save_season_data_player.py --season "$season" --save-type local --filepath "$filepath"

    # Check if the Python script ran successfully
    if [ $? -ne 0 ]; then
        echo "Failed to scrape player data for season $season. Exiting."
        exit 1
    fi
done

echo "Player data scraping completed for all seasons."
