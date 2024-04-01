import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from pandas.io import sql
import re
from fuzzywuzzy import fuzz, process



pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

engine = create_engine("postgresql://postgres:postgres@localhost:5432/cricket")
df = pd.read_sql("select * from masterbattingdf;", engine)
df.drop(["index"], axis=1, inplace=True)
df = df.rename(columns={'runs_x': 'runs', 'fours_x': 'fours', 'sixes_x': 'sixes', 'innings_x': 'innings'})


df2 = pd.read_sql("select * from playerdatacurated;", engine)
df2.drop(["index"], axis=1, inplace=True)

sch = pd.read_sql("select * from schedule24;", engine)
sch.drop(["index"], axis=1, inplace=True)
sch['MatchNumber'] = sch['MatchNumber'].astype(float)

big = df.merge(df2, how='left', left_on='batter', right_on='name')


def e(x):
    if float(x["runs"]) >= 50 and float(x["runs"]) < 75:
        return 25
    elif float(x["runs"]) >= 75 and float(x["runs"]) < 100:
        return 35
    elif float(x["runs"]) >= 100:
        return 50
    elif float(x["runs"]) == 0 and x["PlayerType"] != "Bowler" and x['wkt'] != 'not out':
        return -10
    else:
        return 0

def f(x):
    if (float(x["balls"]) >= 8 or float(x["runs"]) >= 15) and float(x["strikerate"]) < 50 :
        return -25
    elif (float(x["balls"]) >= 8 or float(x["runs"]) >= 15) and float(x["strikerate"]) >= 50 and float(x["strikerate"]) < 70:
        return -20
    elif (float(x["balls"]) >= 8 or float(x["runs"]) >= 15) and float(x["strikerate"]) >= 70 and float(x["strikerate"]) < 90:
        return -15
    elif (float(x["balls"]) >= 8 or float(x["runs"]) >= 15) and float(x["strikerate"]) >= 90 and float(x["strikerate"]) < 100:
        return 0
    elif (float(x["balls"]) >= 8 or float(x["runs"]) >= 15) and float(x["strikerate"]) >= 100 and float(x["strikerate"]) < 130:
        return 15
    elif (float(x["balls"]) >= 8 or float(x["runs"]) >= 15) and float(x["strikerate"]) >= 130 and float(x["strikerate"]) < 150:
        return 20
    elif (float(x["balls"]) >= 8 or float(x["runs"]) >= 15) and float(x["strikerate"]) > 150:
        return 30
    else:
        return 0

big.drop(["name", "bowling"], axis=1, inplace=True)

big['runs'] = big['runs'].astype(float)
big['balls'] = big['balls'].astype(float)
big['minutes'] = big['minutes'].replace('-', float(1))
big['minutes'] = big['minutes'].astype(float)
big['fours'] = big['fours'].astype(float)
big['sixes'] = big['sixes'].astype(float)
big['strikerate'] = big['strikerate'].replace('-', float(0))
big['strikerate'] = big['strikerate'].astype(float)


big['rp'] = big.apply(e, axis=1)
big['sp'] = big.apply(f, axis=1)
big['btp'] = big['runs'] + 2*big['fours'] + 3*big['sixes'] + big['rp'] + big['sp']
big = big.merge(df2, how='left', left_on='bowler_y', right_on='name')

big.drop(["batting_y", "Price_y", "age_y", "name"], axis=1, inplace=True)

a = pd.merge(big, sch, left_on="matchnumber", right_on='MatchNumber', how='left')
a.drop(["MatchDay", "Date", "Day", "Time", "Home", "Away"], axis=1, inplace=True)

a.to_sql("battinginfocurated", engine)