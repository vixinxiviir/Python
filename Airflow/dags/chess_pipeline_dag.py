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
    names = ['hikaru', 'magnuscarlsen', 'fabianocaruana', 'chefshouse', 'firouzja2003', 'iachesisq', 'anishgiri', 'gukeshdommaraju', 'thevish', 'gmwso']
    headers = {'User-Agent':'Chess.py (Python 3.10)(username: vixinxiviir; contact:cody.r.byers@gmail.com)'}
    db_exist = os.path.exists('databases')
    if not db_exist:
        os.makedirs('databases')
    connection = sqlite3.connect('databases/chess_data.db')
    cursor = connection.cursor()
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
    for name in names:
        api_url = 'https://api.chess.com/pub/player/' + name + '/stats'
        response = requests.get(api_url, headers=headers)
        json_data = json.dumps(response.json())
        nested_data = pd.read_json(json_data)
        nested_data['player_name'] = name
        nested_data = nested_data.reset_index().rename(columns={'index':'game_category'})
        for column in nested_data.columns.values:
            nested_data[column] = nested_data[column].astype(str)
        nested_data['date'] = datetime.now()
        nested_data['transformed'] = 'False'
        nested_data.to_sql(name='chess_stage', con=connection, if_exists='append', index=False)
    print("Data extracted!")

def chess_transform():
    names = ['hikaru', 'magnuscarlsen', 'fabianocaruana', 'chefshouse', 'firouzja2003', 'iachesisq', 'anishgiri', 'gukeshdommaraju', 'thevish', 'gmwso']
    tables = ['chess_daily', 'chess_rapid', 'chess_blitz', 'chess_bullet']
    names_frame = pd.DataFrame(names, index=(range(0,len(names))))
    connection = sqlite3.connect('databases/chess_data.db')
    cursor = connection.cursor()
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
    cursor.execute('CREATE TABLE IF NOT EXISTS players(player_name TEXT(512))')
    names_frame.to_sql(con=connection, name='players', if_exists='replace', index=False)
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
    
task1 >> task2