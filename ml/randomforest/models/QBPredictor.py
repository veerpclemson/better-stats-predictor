import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
import numpy as np

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

file = "../../../backend/data_pulling/final_files/QBs.csv"

df = pd.read_csv(file)

target = "passing_yards"
leak_cols = ['pass_attempt', 'complete_pass']

train = df[df["season"] <= 2024]
X_train = train.drop(columns=[target, "player_id", "game_id", "season", "week"] + leak_cols)
y_train = train[target]

test = df[(df["season"] < 2025) | ((df["season"] == 2025) & (df["week"] <= 8))]
X_test = test.drop(columns=[target, "player_id", "game_id", "season", "week"] + leak_cols)

# ✅ One-hot encode first
X_train = pd.get_dummies(X_train, drop_first=True)
X_test = pd.get_dummies(X_test, drop_first=True)

# ✅ Then align columns
X_train, X_test = X_train.align(X_test, join="left", axis=1, fill_value=0)

model = RandomForestRegressor(
    n_estimators=300,
    max_depth=12,
    min_samples_split=4,
    min_samples_leaf=2,
    random_state=42,
    n_jobs=-1
)
model.fit(X_train, y_train)


y_pred = model.predict(X_test)

y_test = test[target]

mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_absolute_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print(f"MAE: {mae:.2f}")
print(f"RMSE: {rmse:.2f}")
print(f"R²: {r2:.3f}")

results = test[["player_id", "game_id", "season", "week"]].copy()
results["actual"] = y_test
results["predicted"] = y_pred

#print(results.head())

importances = model.feature_importances_


feature_importance = pd.Series(importances, index=X_train.columns).sort_values(ascending=False)

print(feature_importance.head(20))