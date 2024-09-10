import os
import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score
import argparse


def load_test_data(test_file_path):
    if os.path.exists(test_file_path):
        print(f"Loading test data from {test_file_path}...")
        df = pd.read_parquet(test_file_path)
        return df
    else:
        raise FileNotFoundError(f"Test file {test_file_path} does not exist.")

def prepare_features_and_target(df):
    df = df.select_dtypes(include=['number'])  
    X = df.drop(columns=['Win'])  
    y = df['Win'] 
    return X, y

def evaluate_model(test_file_path, model_path):
    test_data = load_test_data(test_file_path)

    X_test, y_test = prepare_features_and_target(test_data)

    dtest = xgb.DMatrix(X_test, label=y_test)

    print(f"Loading model from {model_path}...")
    bst = xgb.Booster()
    bst.load_model(model_path)

    y_pred = bst.predict(dtest)
    y_pred_binary = [1 if pred > 0.5 else 0 for pred in y_pred]

    accuracy = accuracy_score(y_test, y_pred_binary)
    print(f"Test Accuracy: {accuracy:.4f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate XGBoost model accuracy on a test set.")
    parser.add_argument('--test-file', required=True, type=str, help="Path to the test parquet file.")
    parser.add_argument('--model-path', required=True, type=str, help="Path to the saved XGBoost model.")
    args = parser.parse_args()

    evaluate_model(test_file_path=args.test_file, model_path=args.model_path)
