import pandas as pd
import argparse


def load_parquet(filepath):
    """
    Load a Parquet file and return a DataFrame.

    Args:
    - filepath: Path to the Parquet file.

    Returns:
    - DataFrame containing the Parquet file data.
    """
    try:
        df = pd.read_parquet(filepath)
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to load the Parquet file: {e}")


def print_parquet(filepath):
    """
    Load and print the contents of a Parquet file.

    Args:
    - filepath: Path to the Parquet file.
    """
    df = load_parquet(filepath)

    # Print the DataFrame to the console
    print(df)
    print(df.columns)


def parse_arguments():
    """
    Parses command line arguments for the script.

    Returns:
    - Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Open and print a Parquet file.")
    parser.add_argument(
        "--filepath",
        required=True,
        type=str,
        help="Path to the Parquet file to open and print.",
    )
    return parser.parse_args()


def main():
    # Parse command-line arguments
    args = parse_arguments()

    # Load and print the Parquet file
    print_parquet(args.filepath)


if __name__ == "__main__":
    main()
