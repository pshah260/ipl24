import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from pandas.io import sql
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

engine = create_engine("postgresql://postgres:postgres@localhost:5432/cricket")

batdf = pd.read_sql("select * from battinginfocurated;", engine)
batdf.drop(["index"], axis=1, inplace=True)

bowdf = pd.read_sql("select * from bowlinginfocurated;", engine)
bowdf.drop(["index"], axis=1, inplace=True)

a = pd.merge(batdf, bowdf, left_on=["batter", "matchnumber"], right_on=["bowler", "matchnumber"], how='left')
a["btp"] = a["btp"].fillna(0)
a["botp"] = a["botp"].fillna(0)
a["totalpoints"] = a["btp"] + a["botp"]

a.to_sql("masterdata", engine)