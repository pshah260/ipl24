
import time

from selenium import webdriver
import json
import pandas as pd
from sqlalchemy import create_engine
from pandas.io import sql
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC, wait
from fuzzywuzzy import fuzz, process

from bs4 import BeautifulSoup
import re

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


engine = create_engine("postgresql://postgres:postgres@localhost:5432/cricket")


options = webdriver.ChromeOptions()
browser = webdriver.Chrome(options=options)

wait = WebDriverWait(browser, 30)

start_game = 1354
browser.get("https://www.espncricinfo.com/series/indian-premier-league-2024-1410320/match-schedule-fixtures-and-results")

html = browser.page_source

soup = BeautifulSoup(html, 'html.parser')

div = soup.find('div', class_='ds-mb-4')
links = div.findAll("a")

matchlinks = []


def g(x):
    runout = r'run out'
    notout = r'not out'
    bname = r' b (.*)'
    if re.search(runout, x['wkt']):
        return ""
    elif re.search(notout, x['wkt']):
        return ""
    else:
        return re.search(bname, x['wkt']).group(1)


def find_best_match(name, names_to_match, threshold=50):
    best_match = process.extractOne(name, names_to_match, scorer=fuzz.token_sort_ratio, score_cutoff=threshold)
    return best_match[0] if best_match is not None else None


for link in links:
    href = link.get('href')
    if href != "/series/indian-premier-league-2024-1410320":
        matchlinks.append(href)

start_match_number = 1
end_match_number = 13

if start_match_number != 1:
    masterbattingdf = pd.read_sql("select * from masterbattingdata24;", engine)
    masterbattingdf.reset_index(drop=True, inplace=True)
else:
    masterbattingdf = pd.DataFrame()

if start_match_number != 1:
    masterbowlingdf = pd.read_sql("select * from masterbowlingdata24;", engine)
    masterbowlingdf.reset_index(drop=True, inplace=True)
else:
    masterbowlingdf = pd.DataFrame()

for match in range(start_match_number-1, end_match_number):
    browser.get("https://www.espncricinfo.com" + matchlinks[match])
    html = browser.page_source
    soup = BeautifulSoup(html, 'html.parser')

    tables = soup.findAll("table")
    t = tables[0]

    data = []
    for row in t.find_all('tr'):
        row_data = []
        for cell in row.find_all('td'):
            row_data.append(cell.text)
        data.append(row_data)



    df = pd.DataFrame(data)
    df = df.dropna()

    columns = ["batter", "wkt", "runs", "balls", "minutes", "fours", "sixes", "strikerate"]
    df.columns = columns

    df.batter = df.batter.str.replace(r'\(c\)', '')
    df.batter = df.batter.str.replace(r'\u2020', '')
    df['batter'] = df['batter'].str.strip()
    df.wkt = df.wkt.str.replace(r'\u2020', '')
    df.wkt = df.wkt.str.replace(r'\(', '')
    df.wkt = df.wkt.str.replace(r'\)', '')
    df["innings"] = 1
    df = df.reset_index(drop=True)
    df['battingorder'] = df.index + 1
    df["bowler"] = df.apply(g, axis=1)
    battinginfo_1 = df

    #batting second inning

    t = tables[2]

    data = []
    for row in t.find_all('tr'):
        row_data = []
        for cell in row.find_all('td'):
            row_data.append(cell.text)
        data.append(row_data)



    df = pd.DataFrame(data)
    df = df.dropna()

    columns = ["batter", "wkt", "runs", "balls", "minutes", "fours", "sixes", "strikerate"]
    df.columns = columns

    df.batter = df.batter.str.replace(r'\(c\)', '')
    df.batter = df.batter.str.replace(r'\u2020', '')
    df['batter'] = df['batter'].str.strip()
    df.wkt = df.wkt.str.replace(r'\u2020', '')
    df.wkt = df.wkt.str.replace(r'\(', '')
    df.wkt = df.wkt.str.replace(r'\)', '')
    df["innings"] = 2
    df = df.reset_index(drop=True)
    df['battingorder'] = df.index + 1
    df["bowler"] = df.apply(g, axis=1)
    battinginfo_2 = df


    # bowling first innings

    t = tables[1]

    data = []
    for row in t.find_all('tr'):
        row_data = []
        for cell in row.find_all('td'):
            row_data.append(cell.text)
        data.append(row_data)



    df = pd.DataFrame(data)
    df = df.dropna()

    columns = ["bowler", "overs", "maiden", "runs", "wickets", "economy", "dots", "fours", "sixes", "wides", "noball"]
    df.columns = columns

    df.bowler = df.bowler.str.replace(r'\(c\)', '')
    df.bowler = df.bowler.str.replace(r'\u2020', '')
    df['bowler'] = df['bowler'].str.strip()
    df["innings"] = 1
    df = df.reset_index(drop=True)
    bowlingdf_1 = df

    # bowling second inngs


    t = tables[3]

    data = []
    for row in t.find_all('tr'):
        row_data = []
        for cell in row.find_all('td'):
            row_data.append(cell.text)
        data.append(row_data)



    df = pd.DataFrame(data)
    df = df.dropna()

    columns = ["bowler", "overs", "maiden", "runs", "wickets", "economy", "dots", "fours", "sixes", "wides", "noball"]
    df.columns = columns

    df.bowler = df.bowler.str.replace(r'\(c\)', '')
    df.bowler = df.bowler.str.replace(r'\u2020', '')
    df['bowler'] = df['bowler'].str.strip()

    df["innings"] = 2
    df = df.reset_index(drop=True)
    bowlingdf_2 = df

    battinginfo_1['Matched_Name'] = battinginfo_1['bowler'].apply(lambda x: find_best_match(x, bowlingdf_1['bowler']))
    batting_merge_df_1 = battinginfo_1.merge(bowlingdf_1, how='left', left_on='Matched_Name', right_on='bowler')

    battinginfo_2['Matched_Name'] = battinginfo_2['bowler'].apply(lambda x: find_best_match(x, bowlingdf_2['bowler']))
    batting_merge_df_2 = battinginfo_2.merge(bowlingdf_2, how='left', left_on='Matched_Name', right_on='bowler')

    battingdf = pd.concat([batting_merge_df_1, batting_merge_df_2], ignore_index=True)
    battingdf["matchnumber"] = match + 1
    bowlingdf = pd.concat([bowlingdf_1, bowlingdf_2], ignore_index=True)
    bowlingdf["matchnumber"] = match + 1



    masterbattingdf = pd.concat([masterbattingdf, battingdf], ignore_index=True)
    masterbattingdf.drop(["overs", "maiden", "wickets", "economy", "dots", "fours_y", "sixes_y", "wides", "noball", "innings_y", "bowler_x", "Matched_Name", "runs_y"], axis=1, inplace=True)


    masterbowlingdf = pd.concat([masterbowlingdf, bowlingdf], ignore_index=True)


masterbattingdf.to_sql("masterbattingdf", engine)
masterbowlingdf.to_sql("masterbowlingdf", engine)
