from nba_api.stats.static import teams
import pandas as pd
import argparse


# Function to fetch team data and create a mapping of abbreviation to TEAM_ID
def fetch_team_abbreviation_to_id():
    """Fetch NBA team data and create a dictionary mapping team abbreviations to team IDs."""
    team_list = teams.get_teams()
    return {team["abbreviation"]: team["id"] for team in team_list}


def load_parquet_data(filepath):
    """Load parquet data into a pandas DataFrame."""
    return pd.read_parquet(filepath)


def get_team_win_loss_record(team_df, team, game_date=None):
    """Fetch the win/loss record for a team up to and including a certain game date, or all games if no date is specified."""
    # Filter the dataframe for the team
    team_games = team_df[team_df["TEAM_ABBREVIATION"] == team].copy()

    if game_date:
        # Ensure the game_date is a datetime object
        game_date = pd.to_datetime(game_date)
        team_games.loc[:, "GAME_DATE"] = pd.to_datetime(team_games["GAME_DATE"])

        # If game date is provided, filter for games on or before the specified date
        team_games = team_games[team_games["GAME_DATE"] <= game_date]

    if team_games.empty:
        raise ValueError(f"No games found for team {team} before or on {game_date or 'the entire season'}")

    # Get win/loss status for each game and create a boolean column: 1 for Win, 0 for Loss
    win_loss = team_games[["GAME_DATE", "MATCHUP", "WL"]].sort_values(by="GAME_DATE")
    win_loss["WIN"] = win_loss["WL"].apply(lambda x: 1 if x == "W" else 0)

    return win_loss


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch win/loss results for a given team up to a specific date or for the whole season."
    )

    parser.add_argument(
        "--team-path",
        required=True,
        type=str,
        help="Path to the team stats parquet file.",
    )
    parser.add_argument(
        "--team",
        required=True,
        type=str,
        help="Team abbreviation (e.g., GSW, LAL, etc.).",
    )
    parser.add_argument(
        "--date",
        required=False,
        type=str,
        help="Game date in YYYY-MM-DD format. If not provided, all games will be returned.",
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    # Load team game data
    team_df = load_parquet_data(args.team_path)

    # Fetch team abbreviation to TEAM_ID mapping
    team_abbreviation_to_id = fetch_team_abbreviation_to_id()

    # Fetch win/loss record for the team, filtered by date if provided
    win_loss_record = get_team_win_loss_record(
        team_df=team_df, team=args.team, game_date=args.date
    )

    if args.date:
        print(f"Win/Loss record for {args.team} up to and including {args.date}:")
    else:
        print(f"All Win/Loss records for {args.team}:")

    print(win_loss_record)


if __name__ == "__main__":
    main()
