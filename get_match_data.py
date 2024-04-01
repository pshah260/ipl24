
import time

from selenium import webdriver
import json
import pandas as pd
from sqlalchemy import create_engine
from pandas.io import sql
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC, wait
from bs4 import BeautifulSoup
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

    battinginfo = df

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

    battingdf = pd.concat([battinginfo, df], ignore_index=True)
    battingdf["matchnumber"] = match + 1

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
    bowlingdf = df

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

    bowlingdf = pd.concat([bowlingdf, df], ignore_index=True)
    bowlingdf["matchnumber"] = match + 1

    masterbattingdf = pd.concat([masterbattingdf, battingdf], ignore_index=True)
    masterbowlingdf = pd.concat([masterbowlingdf, bowlingdf], ignore_index=True)


masterbattingdf.to_sql("masterbattingdf", engine)
masterbowlingdf.to_sql("masterbowlingdf", engine)
