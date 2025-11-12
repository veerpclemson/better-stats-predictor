import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

roster_files = [
    "QBs.csv",
    "RBs.csv",
    "WRsAndTEs.csv",
    "all_defense.csv"
]

dfs = []

for file in roster_files:
    df = pd.read_csv(file)
    dfs.append(df)

QBs, RBs, WRsAndTEs, defense = dfs
defense = defense.rename(columns={'passing_yards_rolling_3': 'def_allowed_passing_rolling_3'})
defense = defense.rename(columns={'passing_yards_rolling_5': 'def_allowed_passing_rolling_5'})
defense = defense.rename(columns={'rushing_yards_rolling_3': 'def_allowed_rushing_rolling_3'})
defense = defense.rename(columns={'rushing_yards_rolling_5': 'def_allowed_rushing_rolling_5'})


RBs = RBs.drop(['points_allowed_rolling_3', 'points_allowed_rolling_5'], axis=1)

RBs = RBs.merge(defense, on=['season', 'week', 'game_id', 'defteam'], how='left')

QBs = QBs.drop(['points_allowed_rolling_3', 'points_allowed_rolling_5'], axis=1)
QBs = QBs.merge(defense, on=['season', 'week', 'game_id', 'defteam'], how='left')

WRsAndTEs = WRsAndTEs.drop(['points_allowed_rolling_3', 'points_allowed_rolling_5'], axis=1)
WRsAndTEs = WRsAndTEs.merge(defense, on=['season', 'week', 'game_id', 'defteam'], how='left')

RBs.to_csv("../data_pulling/final_files/RBs.csv", index=False)
QBs.to_csv("../data_pulling/final_files/QBs.csv", index=False)
WRsAndTEs.to_csv("../data_pulling/final_files/WRsAndTEs.csv", index=False)