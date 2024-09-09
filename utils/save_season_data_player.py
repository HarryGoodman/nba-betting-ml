import pandas as pd
from nba_api.stats.endpoints import commonteamroster, playergamelog, commonteamyears
import pyarrow.parquet as pq
import pyarrow as pa
import argparse
import boto3
import io
import time

def fetch_team_ids():
    """
    Fetch all NBA team IDs programmatically.
    
    Returns:
    - A list of all team IDs.
    """
    try:
        # Fetch all team data
        teams_data = commonteamyears.CommonTeamYears().get_data_frames()[0]
        
        # Extract the team IDs
        team_ids = teams_data['TEAM_ID'].tolist()
        
        print(f"Found {len(team_ids)} teams.")
        return team_ids
    
    except Exception as e:
        raise RuntimeError(f"Failed to fetch team IDs: {e}")

def fetch_all_players_from_rosters(season):
    """
    Fetch all player IDs by extracting them from team rosters for a given season.
    
    Args:
    - season: A string representing the season (e.g., '2023-24').
    
    Returns:
    - A set of unique player IDs for the given season.
    """
    try:
        print(f"Fetching all players from team rosters for season {season}...")
        # Dynamically fetch the team IDs
        team_ids = fetch_team_ids()

        player_ids = set()  # Use a set to store unique player IDs
        
        for team_id in team_ids:
            team_roster = commonteamroster.CommonTeamRoster(team_id=team_id, season=season).get_data_frames()[0]
            # Extract the player IDs from the roster
            player_ids.update(team_roster['PLAYER_ID'].tolist())
            print(f"Fetched {len(team_roster['PLAYER_ID'])} players from team ID {team_id}")
            time.sleep(0.6)  # Sleep between requests to avoid rate-limiting

        print(f"Found {len(player_ids)} players for the season {season}.")
        return player_ids

    except Exception as e:
        raise RuntimeError(f"Failed to fetch players from team rosters: {e}")

def fetch_player_stats_for_season(player_ids, season):
    """
    Fetches the game stats for all players in the given season.
    
    Args:
    - player_ids: List of player IDs.
    - season: A string representing the season (e.g., '2023-24').
    
    Returns:
    - DataFrame containing all player stats for the season.
    """
    try:
        all_players_stats = []
        print(f"Fetching player stats for season {season}...")
        
        for player_id in player_ids:
            try:
                player_data = playergamelog.PlayerGameLog(player_id=player_id, season=season).get_data_frames()[0]
                all_players_stats.append(player_data)
                print(f"Fetched data for player ID {player_id}")
                # Be mindful of API rate limits
                time.sleep(0.6)  # Sleep between requests to avoid rate-limiting
            except Exception as e:
                print(f"Failed to fetch data for player ID {player_id}: {e}")
        
        # Concatenate all player data into a single DataFrame
        all_players_stats_df = pd.concat(all_players_stats, ignore_index=True)
        
        # Preprocess and convert the GAME_DATE column
        all_players_stats_df['GAME_DATE'] = all_players_stats_df['GAME_DATE'].str.strip().str.upper()
        all_players_stats_df['GAME_DATE'] = pd.to_datetime(all_players_stats_df['GAME_DATE'], format='%b %d, %Y')
        
        return all_players_stats_df
    
    except Exception as e:
        raise RuntimeError(f"Failed to fetch player stats for the season: {e}")

def save_as_parquet_local(df, season, filepath):
    """
    Saves the given DataFrame to a local Parquet file.
    
    Args:
    - df: The DataFrame containing player stats.
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
    - df: The DataFrame containing player stats.
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
    Main function to fetch player stats and save them based on the user's choice.

    Args:
    - season: The NBA season (e.g., '2023-24').
    - save_type: The method to save the data ('local' or 's3').
    - filepath: The local file path for saving Parquet (if local).
    - bucket_name: The S3 bucket name (if saving to S3).
    - s3_key: The S3 key (path) for saving the Parquet file in S3.
    """
    # Fetch all player IDs from team rosters
    player_ids = fetch_all_players_from_rosters(season)
    
    # Fetch player stats for the season
    player_stats = fetch_player_stats_for_season(player_ids, season)

    # Save based on the type
    if save_type == 'local':
        if not filepath:
            raise ValueError("Filepath must be provided for local save.")
        save_as_parquet_local(player_stats, season, filepath)
    elif save_type == 's3':
        if not bucket_name or not s3_key:
            raise ValueError("Bucket name and S3 key must be provided for S3 save.")
        save_as_parquet_s3(player_stats, season, bucket_name, s3_key)
    else:
        raise ValueError("Invalid save type. Choose 'local' or 's3'.")

def parse_arguments():
    """
    Parses command line arguments for the NBA player stats extraction script.
    
    Returns:
    - Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Fetch NBA player stats and save as Parquet.")
    
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
