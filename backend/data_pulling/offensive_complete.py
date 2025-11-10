import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

roster_files = [
    "pbp_2021_2025.csv",
     "games_scores2021_2025.csv",
     "roster_2021_2025.csv"
]
dfs = []

for file in roster_files:
    df = pd.read_csv(file)
    dfs.append(df)

pbp, games_scores, players = dfs
window_size1 = 3
window_size2 = 5
"""
=====================================================
    Quarterbacks
=====================================================
"""


passer_df = pbp[pbp['play_type'] == 'pass'][[
    'season','week','game_id','posteam','defteam','passer_player_id',
    'pass_attempt','complete_pass','receiving_yards',
]].copy()

passer_df = passer_df.rename(columns={'passer_player_id': 'player_id'})
passer_df = passer_df.rename(columns={'receiving_yards': 'passing_yards'})
passer_df.fillna(0, inplace=True)

passer_df = passer_df.dropna(subset=['player_id'])
agg_cols = [
    'pass_attempt','complete_pass','passing_yards'
]

QBs = (
    passer_df
    .groupby(['season','week','game_id','posteam','defteam','player_id'], as_index=False)[agg_cols]
    .sum()
)

QBs = QBs[QBs['passing_yards'] != 0]

rolling_cols = ['pass_attempt', 'complete_pass', 'passing_yards']


QBs = QBs.sort_values(['player_id', 'season', 'week'])

for col in rolling_cols:
    QBs[f'{col}_rolling_{window_size1}'] = QBs[col].shift(1).rolling(window = window_size1).mean()
    QBs[f'{col}_rolling_{window_size2}'] = QBs[col].shift(1).rolling(window = window_size2).mean()

QBs['pass_attempt_rolling_5'] = QBs['pass_attempt_rolling_5'].fillna(QBs['pass_attempt_rolling_3'])

QBs['complete_pass_rolling_5'] = QBs['complete_pass_rolling_5'].fillna(QBs['complete_pass_rolling_3'])

QBs['passing_yards_rolling_5'] = QBs['passing_yards_rolling_5'].fillna(QBs['passing_yards_rolling_3'])

"""
=====================================================
    Running Backs
=====================================================
"""

rusher_df = pbp[pbp['play_type'] == 'run'][[
    'season','week','game_id','posteam','defteam','rusher_player_id',
    'rushing_yards'
]].copy()


rusher_df = rusher_df.rename(columns={'rusher_player_id': 'player_id'})
rusher_df.fillna(0, inplace=True)
rusher_df['rush_attempts'] = 1

rusher_df = rusher_df.dropna(subset=['player_id'])
agg_cols = [
    'rushing_yards',
    'rush_attempts'
]

RBs = (
    rusher_df
    .groupby(['season','week','game_id','posteam','defteam','player_id'], as_index=False)[agg_cols]
    .sum()
)
RBs = RBs[RBs['rushing_yards'] != 0]

RBs = RBs.sort_values(['player_id', 'season', 'week'])
rolling_cols = ['rushing_yards', 'rush_attempts']
for col in rolling_cols:
    RBs[f'{col}_rolling_{window_size1}'] = RBs[col].shift(1).rolling(window = window_size1).mean()
    RBs[f'{col}_rolling_{window_size2}'] = RBs[col].shift(1).rolling(window = window_size2).mean()


RBs['rushing_yards_rolling_5'] = RBs['rushing_yards_rolling_5'].fillna(RBs['rushing_yards_rolling_3'])

RBs['rush_attempts_rolling_5'] = RBs['rush_attempts_rolling_5'].fillna(RBs['rush_attempts_rolling_3'])

"""
=====================================================
    Wide Receivers and Tight Ends
=====================================================
"""
receiver_df = pbp[pbp['play_type'] == 'pass'][[
    'season','week','game_id','posteam','defteam','receiver_player_id',
    'reception','receiving_yards'
]].copy()

receiver_df = receiver_df.rename(columns={'receiver_player_id': 'player_id'})
receiver_df.fillna(0, inplace=True)

receiver_df = receiver_df.dropna(subset=['player_id'])
agg_cols = [
    'reception','receiving_yards'
]

WRsAndTEs = (
    receiver_df
    .groupby(['season','week','game_id','posteam','defteam','player_id'], as_index=False)[agg_cols]
    .sum()
)
WRsAndTEs = WRsAndTEs[WRsAndTEs['receiving_yards'] != 0]
WRsAndTEs = WRsAndTEs.sort_values(['player_id', 'season', 'week'])
rolling_cols = ['reception', 'receiving_yards']
for col in rolling_cols:
    WRsAndTEs[f'{col}_rolling_{window_size1}'] = WRsAndTEs[col].shift(1).rolling(window = window_size1).mean()
    WRsAndTEs[f'{col}_rolling_{window_size2}'] = WRsAndTEs[col].shift(1).rolling(window = window_size2).mean()
    
WRsAndTEs['receiving_yards_rolling_3'] = WRsAndTEs['receiving_yards_rolling_3'].fillna(WRsAndTEs['receiving_yards'])
WRsAndTEs['receiving_yards_rolling_5'] = WRsAndTEs['receiving_yards_rolling_5'].fillna(WRsAndTEs['receiving_yards_rolling_3'])

WRsAndTEs['reception_rolling_5'] = WRsAndTEs['reception_rolling_5'].fillna(WRsAndTEs['reception_rolling_3'])





"""
=====================================================
    PPG scored and allowed
=====================================================
"""

offense = pd.DataFrame({
    'season': games_scores['season'],
    'week': games_scores['week'],
    'game_id': games_scores['game_id'],
    'posteam': games_scores['home_team'],
    'defteam': games_scores['away_team'],
    'points_scored': games_scores['home_score'],
    'points_allowed': games_scores['away_score']
})

# Defensive perspective
defense = pd.DataFrame({
    'season': games_scores['season'],
    'week': games_scores['week'],
    'game_id': games_scores['game_id'],
    'posteam': games_scores['away_team'],
    'defteam': games_scores['home_team'],
    'points_scored': games_scores['away_score'],
    'points_allowed': games_scores['home_score']
})

# Combine into one dataframe
long_games = pd.concat([offense, defense], ignore_index=True)
long_games = long_games.sort_values(['posteam', 'season', 'week'])
rolling_cols = ['points_scored', 'points_allowed']
for col in rolling_cols:
    long_games[f'{col}_rolling_{window_size1}'] = long_games[col].shift(1).rolling(window = window_size1).mean()
    long_games[f'{col}_rolling_{window_size2}'] = long_games[col].shift(1).rolling(window = window_size2).mean()
    
long_games['points_scored_rolling_5'] = long_games['points_scored_rolling_5'].fillna(long_games['points_scored_rolling_3'])
long_games['points_allowed_rolling_5'] = long_games['points_allowed_rolling_5'].fillna(long_games['points_allowed_rolling_3'])
long_games.to_csv("../data_pulling/ppg.csv", index=False)

cols = ['points_scored_rolling_3', 'points_scored_rolling_5', 'points_allowed_rolling_3', 'points_allowed_rolling_5']

for col in cols:
    QBs = QBs.merge(long_games[['game_id', 'posteam', col]], on=['game_id', 'posteam'], how='left')
    RBs = RBs.merge(long_games[['game_id', 'posteam', col]], on=['game_id', 'posteam'], how='left')
    WRsAndTEs = WRsAndTEs.merge(long_games[['game_id', 'posteam', col]], on=['game_id', 'posteam'], how='left')

players = players.rename(columns={'gsis_id': 'player_id'})

players = players[['player_id', 'full_name']]

QBs = QBs.merge(
    players,
    how='left',
    on=['player_id']
)
RBs = RBs.merge(
    players,
    how='left',
    on=['player_id']
)
WRsAndTEs = WRsAndTEs.merge(
    players,
    how='left',
    on=['player_id']
)
QBs = QBs.drop_duplicates()
RBs = RBs.drop_duplicates()
WRsAndTEs = WRsAndTEs.drop_duplicates()


QBs.to_csv("../data_pulling/QBs.csv", index=False)
WRsAndTEs.to_csv("../data_pulling/WRsAndTEs.csv", index=False)
RBs.to_csv("../data_pulling/RBs.csv", index=False)
#print(WRsAndTEs.head())
