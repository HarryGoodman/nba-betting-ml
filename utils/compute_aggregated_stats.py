from nba_api.stats.static import players, teams
import pandas as pd
import numpy as np
import argparse

# Constants for columns to be used in computations
TEAM_STATS_COLUMNS = [
    'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 
    'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 
    'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS'
]

PLAYER_STATS_COLUMNS = [
    'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 
    'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 
    'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS'
]

# Function to fetch player data and create a mapping of Player_ID to Player_Name
def fetch_player_mapping():
    """Fetch player data from nba_api and create a dictionary mapping Player_ID to Player_Name."""
    player_list = players.get_players()
    return {player['id']: player['full_name'] for player in player_list}

# Function to fetch team data and create a mapping of abbreviation to TEAM_ID
def fetch_team_abbreviation_to_id():
    """Fetch NBA team data and create a dictionary mapping team abbreviations to team IDs."""
    team_list = teams.get_teams()
    return {team['abbreviation']: team['id'] for team in team_list}

def load_parquet_data(filepath):
    """Load parquet data into a pandas DataFrame."""
    return pd.read_parquet(filepath)

def get_current_elo(elo_df, team, game_date):
    """Fetch the most recent Elo for the team before the given date."""
    team_elo_df = elo_df[(elo_df['Team'] == team) & (elo_df['Date'] < game_date)]
    if team_elo_df.empty:
        raise ValueError(f"No Elo rating found for team {team} before {game_date}")
    return team_elo_df['Elo'].iloc[-1]

def get_team_stats_average(team_df, team, game_date, lag):
    """Compute the average team stats over the last 'n' games before the given date."""
    team_stats = team_df[(team_df['TEAM_ABBREVIATION'] == team) & (team_df['GAME_DATE'] < game_date)]
    if team_stats.empty:
        raise ValueError(f"No team stats found for team {team} before {game_date}")
    last_n_games = team_stats.sort_values(by='GAME_DATE', ascending=False)
    avg_team_stats = last_n_games[TEAM_STATS_COLUMNS].mean()
    return avg_team_stats.values, TEAM_STATS_COLUMNS

def get_player_stats_top_n_average(player_df, team, game_date, lag, n_players, player_mapping, team_abbreviation_to_id):
    """
    Compute the player stats for the top 'n_players' by minutes played over the last 'n' games,
    confirming that the players are from the specified team.
    """
    player_stats = player_df[(player_df['MATCHUP'].str.contains(team)) & (player_df['GAME_DATE'] < game_date)]
    
    if player_stats.empty:
        raise ValueError(f"No player stats found for team {team} before {game_date}")
    
    last_n_games = player_stats.sort_values(by='GAME_DATE', ascending=False)
    top_players = last_n_games.groupby('Player_ID')['MIN'].sum().nlargest(n_players).index.tolist()

    top_player_stats = last_n_games[last_n_games['Player_ID'].isin(top_players[:len(last_n_games)])]

    player_names = {player_id: player_mapping.get(player_id, f"Player_{player_id}") for player_id in top_players[:len(top_player_stats)]}

    avg_player_stats = top_player_stats.groupby('Player_ID')[PLAYER_STATS_COLUMNS[1:]].mean().values.flatten()

    padding_needed = n_players - len(top_player_stats['Player_ID'].unique())
    if padding_needed > 0:
        avg_player_stats = np.concatenate([avg_player_stats, np.zeros((padding_needed, len(PLAYER_STATS_COLUMNS[1:])))], axis=None)

    player_columns = []
    for player_idx in range(1, n_players + 1):
        for stat_col in PLAYER_STATS_COLUMNS[1:]:
            player_columns.append(f"Player_{player_idx}_{stat_col}")
    
    return avg_player_stats, player_columns, top_players[:len(top_player_stats)], player_names


def compute_stats(elo_df, team_df, player_df, team, game_date, lag, n_players, player_mapping, team_abbreviation_to_id):
    """
    Compute the numpy array of Elo, team stats, and player stats.
    """
    current_elo = get_current_elo(elo_df, team, game_date)
    avg_team_stats, team_columns = get_team_stats_average(team_df, team, game_date, lag)
    avg_player_stats, player_columns, top_players, player_names = get_player_stats_top_n_average(
        player_df, team, game_date, lag, n_players, player_mapping, team_abbreviation_to_id
    )
    
    combined_array = np.concatenate([[current_elo], avg_team_stats, avg_player_stats])
    combined_columns = ['Elo'] + list(team_columns) + player_columns
    
    return combined_array, combined_columns, top_players, player_names

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Compute team and player stats along with Elo for a given date, team, and lag.")
    
    parser.add_argument('--elo-path', required=True, type=str, help='Path to the Elo data parquet file.')
    parser.add_argument('--team-path', required=True, type=str, help='Path to the team stats parquet file.')
    parser.add_argument('--player-path', required=True, type=str, help='Path to the player stats parquet file.')
    parser.add_argument('--team', required=True, type=str, help='Team abbreviation (e.g., GSW, LAL, etc.).')
    parser.add_argument('--date', required=True, type=str, help='Game date in YYYY-MM-DD format.')
    parser.add_argument('--lag', required=True, type=int, help='Number of previous games (lag) to consider for average stats.')
    parser.add_argument('--top-n', type=int, default=7, help='Number of top players by minutes played to include (default: 7).')

    return parser.parse_args()

def main():
    args = parse_arguments()

    elo_df = load_parquet_data(args.elo_path)
    team_df = load_parquet_data(args.team_path)
    player_df = load_parquet_data(args.player_path)
    
    # Fetch player mapping (Player_ID to Player_Name)
    player_mapping = fetch_player_mapping()

    # Fetch team abbreviation to TEAM_ID mapping
    team_abbreviation_to_id = fetch_team_abbreviation_to_id()

    stats_array, column_names, top_players, player_names = compute_stats(
        elo_df=elo_df,
        team_df=team_df,
        player_df=player_df,
        team=args.team,
        game_date=args.date,
        lag=args.lag,
        n_players=args.top_n,
        player_mapping=player_mapping,
        team_abbreviation_to_id=team_abbreviation_to_id
    )
    
    print(f"Computed Stats for Team {args.team} on {args.date} with Lag {args.lag}:")
    print(stats_array)
    print("Column Names:")
    print(column_names)
    
    print(f"Top {args.top_n} Players based on minutes played (Player IDs and Names):")
    for player_id in top_players:
        print(f"Player_ID: {player_id}, Player_Name: {player_names.get(player_id)}")

if __name__ == "__main__":
    main()
