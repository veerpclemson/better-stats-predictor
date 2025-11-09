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
    "ppg.csv"
]
dfs = []

for file in roster_files:
    df = pd.read_csv(file)
    dfs.append(df)

defense, ppg = dfs