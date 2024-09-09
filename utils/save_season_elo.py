import pandas as pd
from datetime import timedelta
import argparse

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

def load_game_data(filepath):
    """Load game data from a Parquet file."""
    return pd.read_parquet(filepath)

def save_elo_to_parquet(elo_df, output_path):
    """Save Elo ratings DataFrame to a Parquet file."""
    elo_df.to_parquet(output_path)

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Compute Elo ratings for an NBA season.")
    parser.add_argument("--input", required=True, type=str, help="Path to the game data Parquet file.")
    parser.add_argument("--output", required=True, type=str, help="Path to save the Elo ratings as a Parquet file.")
    return parser.parse_args()

def main():
    # Parse arguments
    args = parse_arguments()

    # Load game data
    games_df = load_game_data(args.input)

    # Compute Elo ratings for the season
    elo_df = compute_season_elo(games_df)

    # Save Elo ratings to Parquet file
    save_elo_to_parquet(elo_df, args.output)
    print(f"Elo ratings saved to {args.output}")

if __name__ == "__main__":
    main()
