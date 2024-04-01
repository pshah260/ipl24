
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


CSK = "https://www.espncricinfo.com/series/indian-premier-league-2024-1410320/chennai-super-kings-squad-1413620/series-squads"
DC = "https://www.espncricinfo.com/series/indian-premier-league-2024-1410320/delhi-capitals-squad-1413547/series-squads"
GT = "https://www.espncricinfo.com/series/indian-premier-league-2024-1410320/gujarat-titans-squad-1413572/series-squads"
KKR = "https://www.espncricinfo.com/series/indian-premier-league-2024-1410320/kolkata-knight-riders-squad-1413553/series-squads"
LSG = "https://www.espncricinfo.com/series/indian-premier-league-2024-1410320/lucknow-super-giants-squad-1413571/series-squads"
MI = "https://www.espncricinfo.com/series/indian-premier-league-2024-1410320/mumbai-indians-squad-1413556/series-squads"
PBKS = "https://www.espncricinfo.com/series/indian-premier-league-2024-1410320/punjab-kings-squad-1413565/series-squads"
RR = "https://www.espncricinfo.com/series/indian-premier-league-2024-1410320/rajasthan-royals-squad-1413568/series-squads"
RCB = "https://www.espncricinfo.com/series/indian-premier-league-2024-1410320/royal-challengers-bangalore-squad-1413548/series-squads"
SRH = "https://www.espncricinfo.com/series/indian-premier-league-2024-1410320/sunrisers-hyderabad-squad-1413569/series-squads"

squads = [CSK, DC, GT, KKR, LSG, MI, PBKS, RR, RCB, SRH]
playerdf = pd.DataFrame()

for team in squads:
    browser.get(team)

    html = browser.page_source
    soup = BeautifulSoup(html, 'html.parser')
    div = soup.find('div', class_='ds-mb-4')


    span_values = div.find_all('span')
    span_data = [span.get_text(strip=True) for span in span_values]


    filtered_data = [item for item in span_data if item != ""]
    index = filtered_data.index("BATTERS") + 1
    filtered_data = filtered_data[index:]
    filtered_data = [item for item in filtered_data if item != "ALLROUNDERS"]
    filtered_data = [item for item in filtered_data if item != "BOWLERS"]
    filtered_data = [item for item in filtered_data if item != "(c)"]
    filtered_data = [item for item in filtered_data if item != "†"]
    filtered_data = [item for item in filtered_data if item != "Withdrawn"]
    filtered_data = [item for item in filtered_data if item != "† (c)"]

    names = []
    ages = []
    batting_styles = []
    bowling_styles = []
    i = 0
    while i < len(filtered_data)-6:
        names.append(filtered_data[i])
        if filtered_data[i+1].strip() == 'Age:':
            ages.append(filtered_data[i+2])
        if filtered_data[i+3].strip() == 'Batting:':
            batting_styles.append(filtered_data[i+4])
        if filtered_data[i+5].strip() == 'Bowling:':
            bowling_styles.append(filtered_data[i+6])
            i += 7
            continue
        else:
            bowling_styles.append(None)
            i += 5
            continue

    # Create DataFrame
    df = pd.DataFrame({
        'name': names,
        'age': ages,
        'batting': batting_styles,
        'bowling': bowling_styles
    })

    playerdf = pd.concat([playerdf, df], ignore_index=True)

playerdf.to_sql("playersinsquad", engine)