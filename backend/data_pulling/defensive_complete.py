import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

roster_files = [
    "defense_tendencies_2021_2025.csv",
    "ppg.csv",
    "QBs.csv",
    "RBs.csv"
]
dfs = []

for file in roster_files:
    df = pd.read_csv(file)
    dfs.append(df)

defense, ppg, QBs, RBs = dfs
window_size1 = 3
window_size2 = 5


final = ppg[['season', 'week', 'defteam', 'game_id', 'points_allowed_rolling_3', 'points_allowed_rolling_5']]

defense = defense.drop('total_pass_plays', axis=1)

defense = defense.sort_values(['defteam', 'season', 'week'])

rolling_cols = ['blitz_rate', 'pressure_rate', 'man_coverage_pct', 'zone_coverage_pct']



for col in rolling_cols:
    defense[f'{col}_rolling_{window_size1}'] = defense[col].shift(1).rolling(window = window_size1).mean()
    defense[f'{col}_rolling_{window_size2}'] = defense[col].shift(1).rolling(window = window_size2).mean()

defense['blitz_rate_rolling_5'] = defense['blitz_rate_rolling_5'].fillna(defense['blitz_rate_rolling_3'])
defense['pressure_rate_rolling_5'] = defense['pressure_rate_rolling_5'].fillna(defense['pressure_rate_rolling_3'])
defense['man_coverage_pct_rolling_5'] = defense['man_coverage_pct_rolling_5'].fillna(defense['man_coverage_pct_rolling_3'])
defense['zone_coverage_pct_rolling_5'] = defense['zone_coverage_pct_rolling_5'].fillna(defense['zone_coverage_pct_rolling_3'])
defense = defense.drop(['blitz_rate', 'pressure_rate', 'man_coverage_pct', 'zone_coverage_pct'], axis = 1)


QBs = QBs[['season', 'week', 'game_id', 'defteam', 'passing_yards']]
QBs = QBs.sort_values(['defteam', 'season', 'week'])
QBs = QBs.groupby(['season', 'week','game_id', 'defteam'], as_index=False)['passing_yards'].sum()
QBs = QBs.sort_values(['defteam', 'season', 'week'])

rolling_cols = ['passing_yards']

for col in rolling_cols:
    QBs[f'{col}_rolling_{window_size1}'] = QBs[col].shift(1).rolling(window = window_size1).mean()
    QBs[f'{col}_rolling_{window_size2}'] = QBs[col].shift(1).rolling(window = window_size2).mean()
QBs['passing_yards_rolling_5'] = QBs['passing_yards_rolling_5'].fillna(QBs['passing_yards_rolling_3'])
QBs = QBs.drop('passing_yards', axis=1)
final = final.sort_values(['defteam', 'season', 'week'])



RBs = RBs[['season', 'week', 'game_id', 'defteam', 'rushing_yards']]
RBs = RBs.sort_values(['defteam', 'season', 'week'])
RBs = RBs.groupby(['season', 'week', 'game_id', 'defteam'], as_index=False)['rushing_yards'].sum()
RBs = RBs.sort_values(['defteam', 'season', 'week'])

rolling_cols = ['rushing_yards']

for col in rolling_cols:
    RBs[f'{col}_rolling_{window_size1}'] = RBs[col].shift(1).rolling(window = window_size1).mean()
    RBs[f'{col}_rolling_{window_size2}'] = RBs[col].shift(1).rolling(window = window_size2).mean()
RBs['rushing_yards_rolling_5'] = RBs['rushing_yards_rolling_5'].fillna(RBs['rushing_yards_rolling_3'])
RBs = RBs.drop('rushing_yards', axis=1)

final = final.merge(defense, on=['season', 'week', 'game_id', 'defteam'], how='left')
final = final.merge(QBs, on=['season', 'week','game_id', 'defteam'], how='left')
final = final.merge(RBs, on=['season', 'week','game_id', 'defteam'], how='left')

final.to_csv("../data_pulling/all_defense.csv", index=False)
print(final.head(10))

