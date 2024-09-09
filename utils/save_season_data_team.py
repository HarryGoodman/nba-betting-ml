import pandas as pd
from nba_api.stats.endpoints import leaguegamelog
import pyarrow.parquet as pq
import pyarrow as pa
import argparse
import boto3
import io
import os

def fetch_nba_season_results(season):
    """
    Fetches all NBA game results for a given season.
    
    Args:
    - season: A string representing the season (e.g., '2023-24')
    
    Returns:
    - DataFrame containing the game logs for the season
    """
    try:
        print(f"Fetching NBA results for season {season}...")
        game_logs = leaguegamelog.LeagueGameLog(season=season).get_data_frames()[0]
        game_logs['GAME_DATE'] = pd.to_datetime(game_logs['GAME_DATE'])
        return game_logs
    except Exception as e:
        raise RuntimeError(f"Failed to fetch NBA results: {e}")

def save_as_parquet_local(df, season, filepath):
    """
    Saves the given DataFrame to a local Parquet file.
    
    Args:
    - df: The DataFrame containing game results.
    - season: The season for which the data is being saved.
    - filepath: The file path where the Parquet file will be saved.
    """
    try:
        df['SEASON'] = season
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filepath)
        print(f"Data successfully saved locally at {filepath}")
    except Exception as e:
        raise RuntimeError(f"Failed to save data locally: {e}")

def save_as_parquet_s3(df, season, bucket_name, s3_key):
    """
    Saves the given DataFrame as a Parquet file to an S3 bucket.
    
    Args:
    - df: The DataFrame containing game results.
    - season: The season for which the data is being saved.
    - bucket_name: The S3 bucket where the file will be saved.
    - s3_key: The S3 key (path) for the file.
    """
    try:
        df['SEASON'] = season
        table = pa.Table.from_pandas(df)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)

        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())
        print(f"Data successfully uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        raise RuntimeError(f"Failed to upload data to S3: {e}")

def run(season, save_type, filepath=None, bucket_name=None, s3_key=None):
    """
    Main function to fetch NBA results and save them based on the user's choice.

    Args:
    - season: The NBA season (e.g., '2023-24').
    - save_type: The method to save the data ('local' or 's3').
    - filepath: The local file path for saving Parquet (if local).
    - bucket_name: The S3 bucket name (if saving to S3).
    - s3_key: The S3 key (path) for saving the Parquet file in S3.
    """
    # Fetch NBA results
    nba_data = fetch_nba_season_results(season)
    
    print(nba_data.head())

    # Save based on the type
    if save_type == 'local':
        if not filepath:
            raise ValueError("Filepath must be provided for local save.")
        save_as_parquet_local(nba_data, season, filepath)
    elif save_type == 's3':
        if not bucket_name or not s3_key:
            raise ValueError("Bucket name and S3 key must be provided for S3 save.")
        save_as_parquet_s3(nba_data, season, bucket_name, s3_key)
    else:
        raise ValueError("Invalid save type. Choose 'local' or 's3'.")

def parse_arguments():
    """
    Parses command line arguments for the NBA data extraction script.
    
    Returns:
    - Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Fetch NBA season data and save as Parquet.")
    
    parser.add_argument("--season", required=True, type=str, help="NBA season to fetch (e.g., '2023-24').")
    parser.add_argument("--save-type", required=True, choices=['local', 's3'], help="Where to save the Parquet file: 'local' or 's3'.")
    
    # Local save option
    parser.add_argument("--filepath", type=str, help="Local file path for saving Parquet file (required if save-type is 'local').")
    
    # S3 save option
    parser.add_argument("--bucket-name", type=str, help="S3 bucket name (required if save-type is 's3').")
    parser.add_argument("--s3-key", type=str, help="S3 key (path) for saving the Parquet file in S3 (required if save-type is 's3').")
    
    return parser.parse_args()

def main():
    # Parse command-line arguments
    args = parse_arguments()

    # Run the script with the parsed arguments
    run(
        season=args.season,
        save_type=args.save_type,
        filepath=args.filepath,
        bucket_name=args.bucket_name,
        s3_key=args.s3_key
    )

if __name__ == "__main__":
    main()
