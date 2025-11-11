import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from dotenv import load_dotenv
import os
import subprocess

subprocess.run(["Rscript", "fetch.R"])
subprocess.run(["python", "offensive_complete.py"])
subprocess.run(["python", "defensive_complete.py"])
subprocess.run(["python", "combine.py"])

print("Ran all files")