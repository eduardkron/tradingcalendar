import requests as rq
from bs4 import BeautifulSoup as BS
import lxml
import pandas as pd
from datetime import datetime as dt
import schedule
import time


def test_url(url):
  """Trying to establish connection with the datasource"""
  try:
    page = rq.get(url).status_code
    if page == 200:
      return True, rq.get(url).text
    else:
      print("Connection failed")
      return False, '200 not given'
  except:
    print("Other Error")
    return False, "Error"


def parse_web_page(raw_html):
  '''Accepts Raw HTML and parses for content'''
  soup = BS(raw_html, 'lxml')
  

  table = soup.find('table', {'id' : 'calendar'})

  rows = []
  for row in table.find_all('tr'):
    row_dict = {}
    y = [ele.text.strip() for ele in row.find_all('td')]
    y = [i for i in y if i]


    country = row.find('td', {'class' : 'calendar-iso'})
    if country and country.text:
      row_dict['country'] = country.text

    indicator = row.find('a', {'class' : 'calendar-event'})
    if indicator and indicator.text:
      row_dict['indicator'] = indicator.text

    actual = row.find('span', {'id' : 'actual'})
    if actual and actual.text:
      row_dict['actual'] = actual.text

    previous = row.find('span', {'id' : 'previous'})
    if previous and previous.text:
      row_dict['previous'] = previous.text

    forecast = row.find('a', {'id' : 'forecast'})
    if forecast and forecast.text:
      row_dict['forecast'] = forecast.text

    rows.append(row_dict)
  return rows


def row_to_df(rows, write_csv=True):
  """Checking if there are 5 datapoints per row
  and if true putting in Dataframe"""
  rows = [i for i in rows if len(i) == 5]
  df = pd.DataFrame(rows)

  now = str(dt.now().date())
  filename = '{}_data.csv'.format(now)
  df['date_added'] = [now] * len(rows)

  df = df[['date_added', 'country' ,
            'indicator','actual',
            'previous','forecast']]

  ## Creates the 'daily' csv
  if write_csv:
    df.to_csv(filename, index=False)

  return df


def update_master_file(todays_data):
  '''
  Read the existing master file
  Add todays new content to the end,
  and overwrites data
  '''
  master = pd.read_csv('master.csv')
  master = pd.concat([master, todays_data])
  master.reset_index(drop=True, inplace=True)
  master.to_csv('master.csv', index=False)


def main_process():
  link = "https://tradingeconomics.com/calendar"
  status, value = test_url(link)
  if status:
    all_rows = parse_web_page(value) # Gets the data
    data = row_to_df(all_rows) # Save data to csv
    update_master_file(data)
  else:
    print(status)




main_process()

if __name__ == '__main__':
  schedule.every(5).seconds.do(main_process)
  # schedule.every().hour.do(main_process)
  #schedule.every().day.at("10:00").do(main_process)

  while True:
    schedule.run_pending()
    time.sleep(1)