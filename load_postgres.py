#!/usr/bin/python3

import os
import json
import sqlparse

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

import connection

conf = connection.config('postgresql')
conn, engine = connection.psql_conn(conf)
cursor = conn.cursor()

engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['pwd']}@{conf['host']}/{conf['db']}")

print(f"[INFO] loading casualty_adjustment...")
pd.read_csv('/mnt/c/linux/Personal_Project_Accident/dataset_accident/casualty_adjustment.csv').to_sql('casualty_adjustment', con=engine, if_exists='replace', index=False)
print(f"[INFO] loading casualty_accident...")
pd.read_csv('/mnt/c/linux/Personal_Project_Accident/dataset_accident/casualty_accident.csv').to_sql('casualty_accident', con=engine, if_exists='replace', index=False)
print(f"[INFO] loading casualty_statistics...")
pd.read_csv('/mnt/c/linux/Personal_Project_Accident/dataset_accident/casualty_statistics.csv').to_sql('casualty_statistics', con=engine, if_exists='replace', index=False)
print(f"[INFO] loading casualty_vehicle...")
pd.read_csv('/mnt/c/linux/Personal_Project_Accident/dataset_accident/casualty_vehicle.csv').to_sql('casualty_vehicle', con=engine, if_exists='replace', index=False)
print(f"[INFO] loading casualty_vehicle_e_scooter...")
pd.read_csv('/mnt/c/linux/Personal_Project_Accident/dataset_accident/casualty_vehicle_e_scooter.csv').to_sql('casualty_vehicle_e_scooter', con=engine, if_exists='replace', index=False)
print(f"[INFO] loading collision_adjustment...")
pd.read_csv('/mnt/c/linux/Personal_Project_Accident/dataset_accident/collision_adjustment.csv').to_sql('collision_adjustment', con=engine, if_exists='replace', index=False)
print(f"[INFO] Sucess loading data to postgres...") 