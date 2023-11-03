from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os
import sqlite3
import requests
import json
from datetime import datetime
from datetime import timedelta


def chess_extract():
    # Our list of GMs
    names = ['hikaru', 'magnuscarlsen', 'fabianocaruana', 'chefshouse', 'firouzja2003', 'iachesisq', 'anishgiri', 'gukeshdommaraju', 'thevish', 'gmwso']

    # For some reason Chess.com API returns 403 if you don't have these headers specifically
    headers = {'User-Agent':'Chess.py (Python 3.10)(username: vixinxiviir; contact:cody.r.byers@gmail.com)'}

    # Checking to see if there's a database folder in cwd
    db_exist = os.path.exists('databases')
    if not db_exist:
        os.makedirs('databases')
    
    # Establishing SQL connection
    connection = sqlite3.connect('databases/chess_data.db')
    cursor = connection.cursor()

    # Creating our staging table
    cursor.execute('''
                    CREATE TABLE IF NOT EXISTS chess_stage(
                    game_category TEXT(25) NOT NULL,
                    player_name TEXT(25) NOT NULL,
                    date DATETIME NOT NULL,
                    chess_daily TEXT(512),
                    chess960_daily TEXT(512),
                    chess_rapid TEXT(512),
                    chess_blitz TEXT(512),
                    chess_bullet TEXT(512),
                    lessons TEXT(512),
                    tactics TEXT(512),
                    fide TEXT(512),
                    puzzle_rush TEXT(512),
                    transformed TEXT(512)
                   );
                    ''')
    
    # Iterating through our list of GMs
    for name in names:
        
        # Chess.com REST API call, returns a json
        api_url = 'https://api.chess.com/pub/player/' + name + '/stats'
        response = requests.get(api_url, headers=headers)
        json_data = json.dumps(response.json())
        
        # Getting it into a data frame
        nested_data = pd.read_json(json_data)

        # Adding some useful columns, just a teensy bit of cleaning to make it easier to insert into the DB
        nested_data['player_name'] = name
        nested_data = nested_data.reset_index().rename(columns={'index':'game_category'})
        for column in nested_data.columns.values:
            nested_data[column] = nested_data[column].astype(str)
        nested_data['date'] = datetime.now()

        # Keeps the transformation piece from duplicating the data
        nested_data['transformed'] = 'False'

        # We append here so we can keep a record of all the raw data if we want to see a particular extract
        nested_data.to_sql(name='chess_stage', con=connection, if_exists='append', index=False)
    print("Data extracted!")

def chess_transform():
    names = ['hikaru', 'magnuscarlsen', 'fabianocaruana', 'chefshouse', 'firouzja2003', 'iachesisq', 'anishgiri', 'gukeshdommaraju', 'thevish', 'gmwso']
    tables = ['chess_daily', 'chess_rapid', 'chess_blitz', 'chess_bullet']

    # This creates a data frame for the player dimension table--we'll need it to join on later
    names_frame = pd.DataFrame(names, columns=['player_name'], index=(range(0,len(names))))
    names_frame['last_updated'] = datetime.now()
    connection = sqlite3.connect('databases/chess_data.db')
    cursor = connection.cursor()

    # Making a table for each of our category data
    for table in tables:
        cursor.execute('CREATE TABLE IF NOT EXISTS ' + table + '''(
                        ''' + table + '''_win INTEGER,
                        ''' + table + '''_loss INTEGER,
                        ''' + table + '''_draw INTEGER,
                        ''' + table + '''_winrate NUMERIC,
                        player_name TEXT(512),
                        date DATETIME
                       );''' 
                    )
    
    # Making our player dimension table
    cursor.execute('CREATE TABLE IF NOT EXISTS players(player_name TEXT(512), last_updated DATETIME)')
    names_frame.to_sql(con=connection, name='players', if_exists='replace', index=False)

    # The workhorse loop: For each GM, and for each category, we grab the win/loss record from stage, 
    # unzip it and clean it, and insert it into the corresponding table
    for name in names:
        for table in tables:
            query = 'SELECT player_name, game_category, ' + table + ', date FROM chess_stage WHERE game_category=\'record\' AND player_name=\'' + name + '\' AND transformed <> \'True\';'
            raw_data = pd.read_sql(con=connection, sql=query)
            transformed_data = pd.DataFrame()
            for index in range(0, len(raw_data.index)):
                record = raw_data[table].iloc[index]
                try:
                    record = record.replace('\'', '"')
                    data = pd.read_json(record, typ='series')
                    data = data.to_frame()
                    data = data.transpose()
                    data = data[['win', 'loss', 'draw']]
                    data = data.rename(columns={'win': table + '_win', 
                                            'loss' : table + '_loss', 
                                            'draw' : table + '_draw'})
                    for column in data.columns.values:
                        data[column] = data[column].astype(int)
                    data[table + '_winrate'] = (data[table + '_win'])/(data[table + '_win'] + data[table + '_loss'] + data[table + '_draw'])
                    data['date'] = datetime.now()
                    data['player_name'] = name
                except:
                    data = pd.DataFrame()
                    data['player_name'] = name
                    data['date'] = datetime.now()
                    data[table + '_win'] = 0
                    data[table + '_loss'] = 0
                    data[table + '_draw'] = 0
                    data[table + '_winrate'] = 0
                transformed_data = pd.concat([transformed_data, data])
            transformed_data.to_sql(con=connection, name=table, index=False, if_exists='append')
    cursor.execute("UPDATE chess_stage SET transformed = 'True' WHERE transformed = 'False';")
    connection.commit()

# The load task takes most recent record data from the game tables, joins it to the players, and updates the final snapshot table
def chess_load():
    connection = sqlite3.connect('databases/chess_data.db')
    cursor = connection.cursor()
    table_creation = '''CREATE TABLE IF NOT EXISTS full_chess_data(
    player_name TEXT(512),
    last_update DATETIME,
    chess_daily_win INTEGER,
    chess_daily_loss INTEGER,
    chess_daily_draw INTEGER,
    chess_daily_winrate NUMERIC,
    chess_blitz_win INTEGER,
    chess_blitz_loss INTEGER,
    chess_blitz_draw INTEGER,
    chess_blitz_winrate NUMERIC,
    chess_bullet_win INTEGER,
    chess_bullet_loss INTEGER,
    chess_bullet_draw INTEGER,
    chess_bullet_winrate NUMERIC,
    chess_rapid_win INTEGER,
    chess_rapid_loss INTEGER,
    chess_rapid_draw INTEGER,
    chess_rapid_winrate NUMERIC
    )'''
    cursor.execute(table_creation)
    consolidation_query = '''SELECT DISTINCT players.player_name, players.last_updated,
                IFNULL(blitz.chess_blitz_win, 0) AS chess_blitz_win, IFNULL(blitz.chess_blitz_loss, 0) AS chess_blitz_loss, IFNULL(blitz.chess_blitz_draw, 0) as chess_blitz_draw, IFNULL(blitz.chess_blitz_winrate, 0) AS chess_blitz_winrate,
                IFNULL(daily.chess_daily_win, 0) AS chess_daily_win,  IFNULL(daily.chess_daily_loss, 0) AS chess_daily_loss,  IFNULL(daily.chess_daily_draw, 0) AS chess_daily_draw,  IFNULL(daily.chess_daily_winrate, 0) AS chess_daily_winrate,
                IFNULL(bullet.chess_bullet_win, 0) AS chess_bullet_win, IFNULL(bullet.chess_bullet_loss, 0) AS chess_bullet_loss, IFNULL(bullet.chess_bullet_draw, 0) AS chess_bullet_draw, IFNULL(bullet.chess_bullet_winrate, 0) AS chess_bullet_winrate,
                IFNULL(rapid.chess_rapid_win, 0) AS chess_rapid_win, IFNULL(rapid.chess_rapid_loss, 0) AS chess_rapid_loss, IFNULL(rapid.chess_rapid_draw, 0) AS chess_rapid_draw, IFNULL(rapid.chess_rapid_winrate, 0) AS chess_rapid_winrate
            FROM players
            LEFT JOIN chess_blitz AS blitz ON players.player_name = blitz.player_name
            LEFT JOIN chess_daily AS daily ON players.player_name = daily.player_name
            LEFT JOIN chess_bullet AS bullet ON players.player_name = bullet.player_name
            LEFT JOIN chess_rapid AS rapid ON players.player_name = rapid.player_name
            ORDER BY players.player_name; '''
    
    consolidated_data = pd.read_sql(con=connection, sql=consolidation_query)
    consolidated_data['last_updated'] = datetime.now()
    consolidated_data.to_sql(con=connection, name='full_chess_data', index=False, if_exists='replace')
    print("Data loaded!")

default_args = {
    'owner': 'airflow',
    'start_date' : days_ago(5)
}
with DAG(
    dag_id='chess_pipeline',
    default_args=default_args,
    description='ETL Pipeline for Chess.com GM play data',
    schedule_interval=timedelta(days=1),
    catchup=False
) as chess_dag:
    
    task1 = PythonOperator(
        task_id='chess_extract',
        python_callable=chess_extract,
        dag=chess_dag
    )

    task2 = PythonOperator(
        task_id = 'chess_transform',
        python_callable = chess_transform,
        dag=chess_dag
    )

    task3 = PythonOperator(
        task_id = 'chess_load',
        python_callable = chess_load,
        dag=chess_dag
    )

task1 >> task2 >> task3

