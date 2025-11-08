import pandas as pd
from sqlalchemy import create_engine
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
    print(f"reading file:{file}")
    df = pd.read_csv(file)
    dfs.append(df)

pbp, games_scores, offense = dfs

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

rusher_df = rusher_df.dropna(subset=['player_id'])
agg_cols = [
    'rushing_yards'
]

RBs = (
    rusher_df
    .groupby(['season','week','game_id','posteam','defteam','player_id'], as_index=False)[agg_cols]
    .sum()
)
RBs = RBs[RBs['rushing_yards'] != 0]



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
print(QBs.head())
