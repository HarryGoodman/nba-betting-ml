import os
import sys
import pandas as pd
import numpy as np
import argparse
from tqdm import tqdm

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.compute_aggregated_stats import (
    compute_stats,
    fetch_player_mapping,
    fetch_team_abbreviation_to_id,
)
from utils.compute_game_result import get_team_win_loss_record


def generate_X(
    elo_df,
    team_df,
    player_df,
    team1,
    team2,
    game_date,
    lag,
    n_players,
    player_mapping,
    team_abbreviation_to_id,
):
    """
    Generate the feature matrix X for a given game, collecting stats for both teams.
    Uses compute_stats function from compute_aggregated_stats.py.
    """
    try:
        # Get stats for Team 1
        X_team1, columns_team1, _, _ = compute_stats(
            elo_df,
            team_df,
            player_df,
            team1,
            game_date,
            lag,
            n_players,
            player_mapping,
            team_abbreviation_to_id,
        )

        # Get stats for Team 2 (opponent)
        X_team2, columns_team2, _, _ = compute_stats(
            elo_df,
            team_df,
            player_df,
            team2,
            game_date,
            lag,
            n_players,
            player_mapping,
            team_abbreviation_to_id,
        )

        # Combine team stats into a single feature vector
        X_combined = np.concatenate([X_team1, X_team2])

        # Combine the column names, prefixing for each team
        columns_combined = [f"Team_1_{col}" for col in columns_team1] + [
            f"Team_2_{col}" for col in columns_team2
        ]

        return X_combined, columns_combined
    except ValueError as e:
        print(f"Skipping game for {team1} vs {team2} on {game_date}: {e}")
        return None, None


def generate_y(result_df, team, game_date):
    """
    Generate the target variable y for a given game (1 for win, 0 for loss).
    Uses the get_team_win_loss_record function.
    """
    win_loss_record = get_team_win_loss_record(result_df, team, game_date)

    # Ensure both game_date and GAME_DATE are datetime objects
    game_date = pd.to_datetime(game_date)
    win_loss_record["GAME_DATE"] = pd.to_datetime(win_loss_record["GAME_DATE"])

    # Find the game matching the exact date and return the win/loss result
    game_result = win_loss_record[win_loss_record["GAME_DATE"] == game_date]

    if game_result.empty:
        raise ValueError(f"No game found for team {team} on {game_date}")

    return game_result["WIN"].iloc[0]  # Return 1 for win, 0 for loss


# Function to load all data files from the given directories or file paths
def load_data(file_or_dir):
    """Load parquet files from a file or directory."""
    if os.path.isdir(file_or_dir):
        # Load all parquet files in the directory
        files = [
            os.path.join(file_or_dir, f)
            for f in os.listdir(file_or_dir)
            if f.endswith(".parquet")
        ]
        dfs = [pd.read_parquet(f) for f in files]
        return pd.concat(dfs, ignore_index=True)
    else:
        # Load a single parquet file
        return pd.read_parquet(file_or_dir)


def generate_training_data(
    elo_path, team_path, player_path, output_path, lag, n_players
):
    """
    Generate training data (X and y) for machine learning models.
    Loads data from provided file paths, computes X and y using abstracted functions.
    """

    # Load all the data
    print("Loading data...")
    elo_df = load_data(elo_path)
    team_df = load_data(team_path)
    player_df = load_data(player_path)

    # Fetch player mapping (Player_ID to Player_Name)
    player_mapping = fetch_player_mapping()
    team_abbreviation_to_id = fetch_team_abbreviation_to_id()

    # Print some example values from the MATCHUP column to debug the format
    print("Sample MATCHUP values:")
    print(team_df["MATCHUP"].head())

    # Loop through each game in the dataset to generate training data
    X_list, y_list, game_ids = [], [], []
    for _, game_row in tqdm(team_df.iterrows()):
        game_date = game_row["GAME_DATE"]
        team1 = game_row["TEAM_ABBREVIATION"]
        matchup = game_row["MATCHUP"]

        # Extract the opponent abbreviation from the MATCHUP column, handling both "vs." and "@" formats
        if "vs." in matchup:
            team2 = matchup.split(" vs. ")[1].strip()  # For home games with 'vs.'
        elif "@" in matchup:
            team2 = matchup.split(" @ ")[1].strip()  # For away games with '@'
        else:
            print(f"Could not parse matchup format for game {matchup}")
            continue

        game_id = game_row["GAME_ID"]  # Capture the GAME_ID

        try:
            # Generate features (X) for both teams
            X, columns = generate_X(
                elo_df,
                team_df,
                player_df,
                team1,
                team2,
                game_date,
                lag,
                n_players,
                player_mapping,
                team_abbreviation_to_id,
            )
            if X is None:
                continue  # Skip if no data is available for X

            # Generate target (y) for the game (win/loss for team 1)
            y = generate_y(team_df, team1, game_date)
            if y is None:
                continue  # Skip if no target is available

            # Append to lists
            X_list.append(X)
            y_list.append(y)
            game_ids.append(game_id)  # Add GAME_ID to the list

        except Exception as e:
            print(f"Error processing game {team1} vs {team2} on {game_date}: {e}")
            continue

    # Convert lists to numpy arrays if there are any valid games
    if X_list and y_list:
        X_array = np.vstack(X_list)
        y_array = np.array(y_list)
        game_ids_array = np.array(game_ids)  # Convert GAME_ID list to array

        # Convert to DataFrame for easier manipulation
        X_df = pd.DataFrame(X_array, columns=columns)
        y_df = pd.DataFrame(y_array, columns=["Win"])
        game_id_df = pd.DataFrame(game_ids_array, columns=["GAME_ID"])

        # Combine X, y, and GAME_ID into a single DataFrame
        training_data = pd.concat([game_id_df, X_df, y_df], axis=1)

        # Save as parquet file
        training_data.to_parquet(output_path, index=False)
        print(f"Training data saved to {output_path} as Parquet format.")
    else:
        print("No valid games were processed.")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate training data for machine learning models using NBA data."
    )

    parser.add_argument(
        "--elo-path",
        required=True,
        type=str,
        help="Path to the Elo data directory or file.",
    )
    parser.add_argument(
        "--team-path",
        required=True,
        type=str,
        help="Path to the team stats directory or file.",
    )
    parser.add_argument(
        "--player-path",
        required=True,
        type=str,
        help="Path to the player stats directory or file.",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        type=str,
        help="Output file path to save the training data (numpy arrays).",
    )
    parser.add_argument(
        "--lag",
        required=True,
        type=int,
        help="Number of previous games (lag) to consider for average stats.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=7,
        help="Number of top players by minutes played to include (default: 7).",
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    # Generate the training data for all games
    generate_training_data(
        elo_path=args.elo_path,
        team_path=args.team_path,
        player_path=args.player_path,
        output_path=args.output_path,
        lag=args.lag,
        n_players=args.top_n,
    )


if __name__ == "__main__":
    main()
