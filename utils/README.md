# Utils Folder

This folder contains a set of utility scripts designed to work with NBA data and perform various tasks, such as loading, processing, and saving game data, player statistics, and Elo ratings. The scripts utilize `nba_api` for fetching NBA game data and player statistics, and provide options for saving the data locally or to AWS S3 in Parquet format.

## Table of Contents
- [Scripts Overview](#scripts-overview)
- [Setup](#setup)
- [Usage](#usage)
  - [General Structure](#general-structure)
  - [Script Usage](#script-usage)

---

## Scripts Overview

### 1. **`parquet_reader.py`**
   - **Purpose**: This script loads and prints the contents of a Parquet file.
   - **Main Features**:
     - Load a Parquet file into a pandas DataFrame.
     - Print the content of the Parquet file and its columns.
   - **Usage**: Primarily used for debugging or inspecting Parquet files.

### 2. **`save_season_data_team.py`**
   - **Purpose**: Fetches NBA team game logs for a given season and saves the data as a Parquet file.
   - **Main Features**:
     - Fetches team-level game logs using the `nba_api`.
     - Saves the fetched data either locally or to AWS S3.
   - **Arguments**:
     - `--season`: Specifies the NBA season to fetch (e.g., '2023-24').
     - `--save-type`: Determines where to save the Parquet file (`local` or `s3`).
     - `--filepath`: (For local save) Path to save the Parquet file locally.
     - `--bucket-name`: (For S3 save) Name of the S3 bucket.
     - `--s3-key`: (For S3 save) Path in the S3 bucket to save the Parquet file.

### 3. **`save_season_data_player.py`**
   - **Purpose**: Fetches player statistics for a specific season and saves the data as a Parquet file.
   - **Main Features**:
     - Fetches all players’ game statistics for a given season using `nba_api`.
     - Saves the player data locally or to AWS S3.
   - **Arguments**:
     - `--season`: Specifies the NBA season to fetch player data (e.g., '2023-24').
     - `--save-type`: Determines where to save the Parquet file (`local` or `s3`).
     - `--filepath`: (For local save) Path to save the Parquet file locally.
     - `--bucket-name`: (For S3 save) Name of the S3 bucket.
     - `--s3-key`: (For S3 save) Path in the S3 bucket to save the Parquet file.

### 4. **`save_season_elo.py`**
   - **Purpose**: Computes Elo ratings for each NBA team across a given season and saves the Elo ratings as a Parquet file.
   - **Main Features**:
     - Processes the game results to compute Elo ratings for all teams.
     - Saves the computed Elo ratings locally or to AWS S3.
   - **Arguments**:
     - `--input`: Path to the Parquet file containing the NBA game results.
     - `--output`: Path to save the Elo ratings Parquet file.

---

## Setup

### Prerequisites
- Python 3.x
- Required Python packages are listed in the `requirements.txt` file. Install them using:
  ```bash
  pip install -r requirements.txt
  ```


### Environment Variables (if using AWS S3)
If you intend to save the Parquet files to AWS S3, make sure you have the following environment variables set:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_DEFAULT_REGION`

---

## Usage

### General Structure

Each script can be run from the command line and accepts various arguments to control the input and output behavior (e.g., fetching specific seasons, saving locally or to S3).

### Script Usage

#### **1. `parquet_reader.py`**
   Prints the contents of a Parquet file.
   ```bash
   python utils/parquet_reader.py --filepath path/to/file.parquet
   ```

#### **2. `save_season_data_team.py`**
Fetches and saves team game logs for a specific NBA season.
```bash
# Save locally
python utils/save_season_data_team.py --season '2023-24' --save-type local --filepath path/to/save/season_team_202324.parquet

# Save to S3
python utils/save_season_data_team.py --season '2023-24' --save-type s3 --bucket-name your-bucket --s3-key season_team_202324.parquet
```

#### **3. `save_season_data_player.py`**
Fetches and saves player statistics for a specific NBA season.
```bash
# Save locally
python utils/save_season_data_player.py --season '2023-24' --save-type local --filepath path/to/save/season_player_202324.parquet

# Save to S3
python utils/save_season_data_player.py --season '2023-24' --save-type s3 --bucket-name your-bucket --s3-key season_player_202324.parquet
```


#### **4. `save_season_elo.py`**
Computes and saves Elo ratings for NBA teams based on the game results for a specific season.
```bash
# Local input and local output:
python utils/save_season_elo.py --season '2023-24' --input-type local --input-filepath path/to/game_results.parquet --output-type local --output-filepath path/to/save/elo_ratings.parquet

# S3 input and S3 output:
python utils/save_season_elo.py --season '2023-24' --input-type s3 --input-filepath nba/season_team_202324.parquet --output-type s3 --bucket-name mybucket --s3-key nba/season_elo_202324.parquet

```