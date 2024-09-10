#!/bin/bash

# Define base directories for input and output data
ELO_DIR="data/elo"
TEAM_DIR="data/team"
PLAYER_DIR="data/player"
OUTPUT_DIR="data/train"

# Create the output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Get the current year
current_year=$(date +"%Y")

# Define the number of years to go back
num_years=10

# Calculate the start year
start_year=$((current_year - num_years))

# Loop through each season and call the Python script
for ((year=$start_year; year<current_year; year++)); do
    # Calculate the next year for the season format
    next_year=$((year + 1))

    # Format the season year string (e.g., '202223' for the season '2022-23')
    season_year="${year}${next_year: -2}"

    # Define file paths for the ELO, team, and player parquet files
    elo_path="${ELO_DIR}/season_elo_${season_year}.parquet"
    team_path="${TEAM_DIR}/season_team_${season_year}.parquet"
    player_path="${PLAYER_DIR}/season_player_${season_year}.parquet"
    
    # Define the output file path
    output_path="${OUTPUT_DIR}/${season_year}.parquet"

    # Parameters
    lag=5
    top_n=7

    # Call the Python script for the current season
    echo "Generating training data for season ${year}-${next_year: -2}..."
    python utils/data_generator.py \
        --elo "$elo_path" \
        --team-path "$team_path" \
        --player-path "$player_path" \
        --lag "$lag" \
        --top-n "$top_n" \
        --output-path "$output_path"

    # Check if the Python script ran successfully
    if [ $? -ne 0 ]; then
        echo "Failed to generate training data for season ${year}-${next_year: -2}. Exiting."
        exit 1
    fi
done

echo "Training data generation completed for all seasons."
