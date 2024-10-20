import pandas as pd
from sklearn.model_selection import KFold, cross_validate
import joblib
import json
import yaml
import os


def evaluate_model():
    with open("params.yaml", "r") as fd:
        params = yaml.safe_load(fd)
    model_path = "models/fitted_model.pkl"
    with open(model_path, "rb") as fd:
        loaded_model = joblib.load(fd)
    data = pd.read_csv("data/initial_data.csv")
    target = data[params["target_col"]]
    cv_strategy = KFold(n_splits=params["n_splits"])
    cv_res = cross_validate(
        loaded_model,
        data,
        target,
        cv=cv_strategy,
        n_jobs=params["n_jobs"],
        scoring=params["metrics"],
    )
    for key, value in cv_res.items():
        cv_res[key] = round(value.mean(), 3)
    os.makedirs("cv_results", exist_ok=True)
    evaluate_path = "cv_results/cv_res.json"
    with open(evaluate_path, "w") as json_file:
        json.dump(cv_res, json_file, indent=4)


if __name__ == "__main__":
    evaluate_model()
