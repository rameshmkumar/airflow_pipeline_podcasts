from airflow.decorators import dag, task
import pendulum 
import requests
import xmltodict
import os

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

@dag(
    dag_id='podcast_summary',
    schedule='@daily',
    start_date=pendulum.datetime(2025,5,24),
    catchup=False
)

def podcast_summary():

    #Tradition methode of writing task
    create_db=SQLExecuteQueryOperator(
        task_id="create_table_sqlite",
        conn_id="podcasts",
        sql=r"""
        CREATE TABLE IF NOT EXISTS episodes(
            link TEXT PRIMARY KEY,
            title TEXT,
            filename TEXT,
            published TEXT,
            description TEXT
            )
            """
    )
    
    #Task Flow API method to write the tasks
    @task()
    def get_episodes():
        data=requests.get("https://feeds.publicradio.org/public_feeds/marketplace")
        feed=xmltodict.parse(data.text) #Data is in XML so we use xmltodict.parse(data) into text
        episodes=feed["rss"]["channel"]["item"]  #we take only the item from the feed
        print(f"Found {len(episodes)} episodes")
        return episodes


    
    @task()
    def load_episodes(episodes):
        hook=SqliteHook(sqlite_conn_id="podcasts")  #Created a hook by connecting to sqlite_conn_id. hook helps to communicate with SQLite in python
        stored=hook.get_pandas_df("SELECT * FROM episodes;")  #Query directly using hook
        new_episodes=[]
        for episode in episodes:
            if episode["link"] not in stored["link"].values:    #since link is primary key, we check with link and check will all the rows 
                filename=f"{episode["link"].split('/')[-1]}" 
                new_episodes.append([episode["link"], episode["title"], episode["pubDate"], episode["description"], filename])  #storing each episode's info
        hook.insert_rows(table="episodes", rows=new_episodes, target_fields=["link", "title", "published", "description", "filename"])  #target_field will help the sqlite to order the column correct order even if the order mismatch

    podcast_episodes=get_episodes()
    create_db >> podcast_episodes
    load_episodes(podcast_episodes)
     
    

    @task()
    def download_episodes(episodes):
        
        for episode in episodes:
            filename=f"{episode['link'].split('/')[-1]}.mp3"
            audio_path=os.path.join("episodes", filename)   #created the path with foldername & filename
            if not os.path.exists(audio_path):        #if the path(file) doesn't exist
                print(f"Downloading {filename}")
                audio=requests.get(episode["enclosure"]["@url"])  #Downloads the file from the url
                with open(audio_path, "wb+") as f:      #We open the audio_path in WB mode
                    f.write(audio.content)          #write the audio as content inside

    download_episodes(podcast_episodes)


summary=podcast_summary()



