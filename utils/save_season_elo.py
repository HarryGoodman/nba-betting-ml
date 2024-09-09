import pandas as pd
import boto3
import io
import argparse
from datetime import timedelta
import pyarrow.parquet as pq
import pyarrow as pa

# Constants for Elo computation
K_FACTOR = 20
M = 400
STARTING_ELO = 1000

# Elo functions
def expected_outcome(elo_a, elo_b, m=M):
    """Calculate the expected outcome."""
    return 1 / (1 + 10**((elo_b - elo_a) / m))

def update_elo(elo_a, elo_b, actual_score, k=K_FACTOR, m=M):
    """Update the Elo rating."""
    return elo_a + k * (actual_score - expected_outcome(elo_a, elo_b, m))

def elo_formula(game, elo, k=K_FACTOR, m=M):
    """Apply Elo formula to a game."""
    team = game['TEAM_ABBREVIATION']
    opponent = game['MATCHUP'].split(' ')[-1]  # Extract the opponent team from the matchup
    date = game['GAME_DATE']

    # Determine if the current team won or lost
    if game['WL'] == 'W':
        winner = team
        loser = opponent
    else:
        winner = opponent
        loser = team

    # Get current Elo ratings
    winner_elo = elo[winner][-1][1]
    loser_elo = elo[loser][-1][1]

    # Update Elo ratings
    new_winner_elo = update_elo(winner_elo, loser_elo, 1, k, m)
    new_loser_elo = update_elo(loser_elo, winner_elo, 0, k, m)

    # Append new Elo ratings with the game date
    elo[winner].append([date, new_winner_elo])
    elo[loser].append([date, new_loser_elo])

    return elo

def initialize_elo(teams, start_date):
    """Initialize the Elo ratings for all teams."""
    return {team: [[start_date - timedelta(days=1), STARTING_ELO]] for team in teams}

def compute_season_elo(games_df):
    """Compute Elo ratings for an entire season."""
    # Extract all unique teams
    teams = games_df['TEAM_ABBREVIATION'].unique()

    # Initialize Elo for all teams
    start_date = games_df['GAME_DATE'].min()
    elo = initialize_elo(teams, start_date)

    # Apply Elo formula to all games
    for _, game in games_df.iterrows():
        elo = elo_formula(game, elo)

    # Convert Elo ratings to a DataFrame for easier use
    elo_df = pd.DataFrame()
    for team, ratings in elo.items():
        df_team = pd.DataFrame(ratings, columns=['Date', 'Elo'])
        df_team['Team'] = team
        elo_df = pd.concat([elo_df, df_team])

    return elo_df

def load_game_data_local(filepath):
    """Load game data from a local Parquet file."""
    return pd.read_parquet(filepath)

def load_game_data_s3(bucket_name, s3_key):
    """Load game data from an S3 bucket."""
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
    buffer = io.BytesIO(obj['Body'].read())
    return pd.read_parquet(buffer)

def save_as_parquet_local(df, season, filepath):
    """
    Saves the given DataFrame to a local Parquet file.
    
    Args:
    - df: The DataFrame containing Elo ratings.
    - season: The season for which the Elo ratings are being saved.
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
    - df: The DataFrame containing Elo ratings.
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

def run(season, input_type, input_filepath, output_type, output_filepath=None, bucket_name=None, s3_key=None):
    """
    Main function to compute Elo ratings and save them based on the user's choice.

    Args:
    - season: The NBA season (e.g., '2023-24').
    - input_type: The input data location type ('local' or 's3').
    - input_filepath: The input file path (local or S3 key).
    - output_type: The method to save the data ('local' or 's3').
    - output_filepath: The local file path for saving Parquet (if local).
    - bucket_name: The S3 bucket name (if using S3 for saving/loading).
    - s3_key: The S3 key (path) for saving/loading the Parquet file in S3.
    """
    # Load NBA game data
    if input_type == 'local':
        nba_data = load_game_data_local(input_filepath)
    elif input_type == 's3':
        if not bucket_name or not s3_key:
            raise ValueError("Bucket name and S3 key must be provided for S3 input.")
        nba_data = load_game_data_s3(bucket_name, input_filepath)
    else:
        raise ValueError("Invalid input type. Choose 'local' or 's3'.")

    # Compute Elo ratings
    elo_df = compute_season_elo(nba_data)

    # Save based on the type
    if output_type == 'local':
        if not output_filepath:
            raise ValueError("Filepath must be provided for local save.")
        save_as_parquet_local(elo_df, season, output_filepath)
    elif output_type == 's3':
        if not bucket_name or not s3_key:
            raise ValueError("Bucket name and S3 key must be provided for S3 save.")
        save_as_parquet_s3(elo_df, season, bucket_name, s3_key)
    else:
        raise ValueError("Invalid save type. Choose 'local' or 's3'.")

def parse_arguments():
    """
    Parses command line arguments for the Elo computation script.
    
    Returns:
    - Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Compute Elo ratings for an NBA season and save as Parquet.")
    
    parser.add_argument("--season", required=True, type=str, help="NBA season to compute Elo ratings for (e.g., '2023-24').")
    parser.add_argument("--input-type", required=True, choices=['local', 's3'], help="Where to load the Parquet file from: 'local' or 's3'.")
    parser.add_argument("--input-filepath", required=True, type=str, help="Input file path (local file path or S3 key).")
    
    parser.add_argument("--output-type", required=True, choices=['local', 's3'], help="Where to save the Parquet file: 'local' or 's3'.")
    parser.add_argument("--output-filepath", type=str, help="Output file path for saving Parquet file (required if output type is 'local').")
    
    parser.add_argument("--bucket-name", type=str, help="S3 bucket name (required if using S3 for saving/loading).")
    parser.add_argument("--s3-key", type=str, help="S3 key (path) for saving/loading the Parquet file in S3 (required if input/output type is 's3').")
    
    return parser.parse_args()

def main():
    # Parse command-line arguments
    args = parse_arguments()

    # Run the script with the parsed arguments
    run(
        season=args.season,
        input_type=args.input_type,
        input_filepath=args.input_filepath,
        output_type=args.output_type,
        output_filepath=args.output_filepath,
        bucket_name=args.bucket_name,
        s3_key=args.s3_key
    )

if __name__ == "__main__":
    main()
