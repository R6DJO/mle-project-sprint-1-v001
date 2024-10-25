# scripts/fit.py

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from catboost import CatBoostRegressor
import yaml
import os
import joblib


def fit_model():
    with open("params.yaml", "r") as fd:
        params = yaml.safe_load(fd)

    data = pd.read_csv("data/initial_data.csv", delimiter=",")

    target = data[params["target_col"]]
    # data = data.drop([params["index_col"]] + [params["target_col"]], axis=1)
    data = data.drop([params["target_col"]], axis=1)

    cat_features = data.select_dtypes(include="int64")
    potential_binary_features = cat_features.nunique() == 2
    binary_cat_features = cat_features[
        potential_binary_features[potential_binary_features].index
    ]
    other_cat_features = cat_features[
        potential_binary_features[~potential_binary_features].index
    ]
    num_features = data.select_dtypes(["float64"])

    # print(binary_cat_features)
    # print(other_cat_features)
    # print(num_features)

    preprocessor = ColumnTransformer(
        [
            ("binary", "passthrough", binary_cat_features.columns.tolist()),
            ("cat", StandardScaler(), other_cat_features.columns.tolist()),
            ("num", StandardScaler(), num_features.columns.tolist()),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    model = CatBoostRegressor(loss_function=params["loss_function"], verbose=0)

    pipeline = Pipeline([("preprocessor", preprocessor), ("model", model)])
    pipeline.fit(data, target)

    os.makedirs("models", exist_ok=True)
    model_path = "models/fitted_model.pkl"
    joblib.dump(pipeline, model_path)


if __name__ == "__main__":
    fit_model()
