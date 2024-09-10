import os
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import argparse


# Function to load and stack the parquet files based on a list of full paths
def load_and_stack_data(train_file_path):
    # Read the train.txt file to get the full parquet file paths
    with open(train_file_path, "r") as f:
        file_paths = f.read().splitlines()

    # List to store the dataframes
    data_frames = []

    # Loop through each file path in the train.txt
    for file_path in file_paths:
        if os.path.exists(file_path):
            print(f"Loading data from {file_path}...")
            df = pd.read_parquet(file_path)
            data_frames.append(df)
        else:
            print(f"File {file_path} does not exist. Skipping...")

    # Stack all dataframes together
    if data_frames:
        data = pd.concat(data_frames, ignore_index=True)
        print(f"Successfully stacked {len(data_frames)} years of data.")
        return data
    else:
        raise ValueError("No valid data files were found!")


# Function to preprocess data and prepare features and target
def prepare_features_and_target(df):
    # Assuming 'Win' is the target column and all other columns are features
    # Exclude non-numeric columns like GAME_ID
    df = df.select_dtypes(include=["number"])  # Keep only numeric columns
    X = df.drop(columns=["Win"])  # Features
    y = df["Win"]  # Target
    return X, y


# Main function to load data, train XGBoost model, and save the model
def main(train_file_path, model_output_path):
    # Load and stack the data
    data = load_and_stack_data(train_file_path)

    # Prepare features (X) and target (y)
    X, y = prepare_features_and_target(data)

    # Split data into train and test sets (80/20 split)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Convert to DMatrix for XGBoost
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)

    # Define XGBoost parameters
    params = {
        "objective": "binary:logistic",
        "max_depth": 6,  # Try values between 3 and 10
        "eta": 0.1,  # Lowering eta to prevent overfitting, try 0.01 to 0.3
        "min_child_weight": 1,  # Try values between 1 and 6
        "gamma": 0.1,  # Try values between 0 and 0.5
        "subsample": 0.8,  # Try values between 0.5 and 1.0
        "colsample_bytree": 0.8,  # Try values between 0.5 and 1.0
        "eval_metric": "logloss",
        "verbosity": 1,
    }

    # Train the XGBoost model
    print("Training the XGBoost model...")
    bst = xgb.train(params, dtrain, num_boost_round=100, evals=[(dtest, "test")])

    # Make predictions on the test set
    y_pred = bst.predict(dtest)
    y_pred_binary = [1 if pred > 0.5 else 0 for pred in y_pred]

    # Evaluate the model
    accuracy = accuracy_score(y_test, y_pred_binary)
    print(f"Test Accuracy: {accuracy:.4f}")

    # Save the trained model
    bst.save_model(model_output_path)
    print(f"Model saved to {model_output_path}")


if __name__ == "__main__":
    # Argument parser
    parser = argparse.ArgumentParser(
        description="Train an XGBoost model on stacked data."
    )
    parser.add_argument(
        "--train-file",
        required=True,
        type=str,
        help="Path to train.txt specifying the parquet files for training.",
    )
    parser.add_argument(
        "--model-output",
        required=True,
        type=str,
        help="Path to save the trained model.",
    )
    args = parser.parse_args()

    # Call the main function
    main(train_file_path=args.train_file, model_output_path=args.model_output)
