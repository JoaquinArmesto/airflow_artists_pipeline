import psycopg2 as psy
import spotipy as spf
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests as rq
import logging


def get_musicbrainz_list_of_artists(country = str, limit = int) -> list:
    """
    Get a list of artists from a country using the MusicBrainz API.

    Parameters:
    country (str): The country to search for artists.
    limit (int): The number of artists to return.

    Returns:
    list: A list of artists.

    Example:
    country = "Argentina"
    limit   = 10
    artists = get_musicbrainz_list_of_artists(country, limit)
    """

    artists = rq.get(
      f"https://musicbrainz.org/ws/2/artist?query=area:{country}&limit={limit}&fmt=json"
      ).json()['artists']

    artists_list = [
        artist['name'] 
        for artist in artists
        ]

    return artists_list



def db_conn() -> psy.extensions.connection:
    conn = psy.connect(
      dbname   = os.getenv('REDSHIFT_NAME'),
      user     = os.getenv('REDSHIFT_USER'),
      password = os.getenv('REDSHIFT_PASSWORD'),
      host     = os.getenv('REDSHIFT_HOST'),
      port     = os.getenv('REDSHIFT_PORT')
      )
    
    return conn



def query_database(queries, params = None, fetch_method = True) -> pd.DataFrame:
    """
    Execute a SQL query on a PostgreSQL database and return the result as a Pandas DataFrame.

    Parameters:
    query (str): The SQL query to execute.
    params (tuple | list | dict): SQL query parameters accept tuple, list or dict values
        https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries

    Returns:
    pd.DataFrame: A DataFrame containing the query result.

    Example:
    query = "SELECT * FROM my_table"
    result = query_database_data(query)
    """
    
    if isinstance(queries, list) is False:
        queries = [queries]
    
    if fetch_method is True: 
        try:
            with db_conn() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(queries[0], params)
                    data    = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]

            dataframe = pd.DataFrame(
                data    = data,
                columns = columns
                )

        except psy.Error as e:
            print(f"Database error: {e}")
            dataframe = pd.DataFrame()

        return dataframe
    
    else:
        try:
            with db_conn() as connection:
                with connection.cursor() as cursor:
                    for query in queries:
                        cursor.execute(query, params)
                    
                    connection.commit()

        except psy.Error as e:
            print(f"Database error: {e}")


def retrieve_data_from_api(spotify_credentials, **context):
    countries = [
        'Argentina', 'United States', 'Germany', 
        'France', 'England', 'Uruguay', 'Chile', 
        'Brazil', 'Spain', 'Italy', 'Japan', 'China', 
        'Australia', 'Canada', 'Russia', 'Mexico', 
        'Colombia', 'Peru', 'Venezuela', 'Ecuador', 
        'Paraguay', 'Bolivia', 'Cuba'
        ]

    artists_and_countries = {}
    for country in countries:
        for artist in get_musicbrainz_list_of_artists(country, 100):
            artists_and_countries.update({artist: country})


        sp = spf.Spotify(
            client_credentials_manager = spotify_credentials
            )


    artists_dict = {}
    for artist_name in artists_and_countries.keys():
        try:
            spotify_artist_json = sp.search(
            q     = artist_name,
            type  = "artist", 
            limit = 10
            )

            artist_data = {
                spotify_artist_json['artists']['items'][0]['name']: {
                'popularity' : spotify_artist_json['artists']['items'][0]['popularity'],
                'genres'     : spotify_artist_json['artists']['items'][0]['genres'],
                'followers'  : spotify_artist_json['artists']['items'][0]['followers']['total'],
                'country'    : artists_and_countries.get(artist_name)
                }
                }
            
            artists_dict.update(artist_data)

        except:
            pass

    main_df = pd.DataFrame.from_dict(artists_dict).T.reset_index()
    artists_data = (
        main_df.loc[:, ['index', 'popularity', 'followers', 'country']]
            .rename(
                columns = {'index': 'artist_name'}
                )
        )
    
    logging.info("Data retrieved from API")
    logging.info(f"Entries count: {artists_data.shape[0]}")

    context['ti'].xcom_push(
        key     = 'artists_data', 
        value   = artists_data
        )


def load_data_to_dw(**context):
    conn = db_conn()

    artists_data = context['ti'].xcom_pull(key='artists_data')
    artists_data.head(5)

    data = zip(
        artists_data.artist_name, 
        artists_data.popularity, 
        artists_data.followers, 
        artists_data.country
        )

    with conn.cursor() as cur:
        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS artist_data (
                    artist_name VARCHAR(255) primary key,
                    popularity  INT,
                    followers   INT,
                    country     VARCHAR(255)
                );""")
        
        cur.execute(
            """
                CREATE table IF NOT EXISTS temp_artist_data (
                    temp_artist_name VARCHAR(255),
                    temp_popularity  INT,
                    temp_followers   INT,
                    temp_country     VARCHAR(255)
                );
            """
            )

        cur.executemany(
            """
                INSERT INTO temp_artist_data (temp_artist_name, temp_popularity, temp_followers, temp_country)
                values (%s, %s, %s, %s);
            """, data)
        
        cur.execute(
            """
                MERGE INTO artist_data
                USING temp_artist_data s
                ON artist_name = s.temp_artist_name
                WHEN MATCHED THEN UPDATE SET
                popularity = s.temp_popularity,
                followers  = s.temp_followers
                WHEN NOT MATCHED THEN
                    INSERT (artist_name, popularity, followers, country)
                    values (s.temp_artist_name, s.temp_popularity, s.temp_followers, s.temp_country);
            """)
     
        cur.execute("""DROP TABLE temp_artist_data;""")
        conn.commit()
        conn.close()
        logging.info("Data loaded to Redshift")



default_args = {
    'owner'           : 'Joaquin Armesto',
    'depends_on_past' : False,
    'start_date'      : datetime(2024, 1, 15),
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes = 5),
}


# Spotify client object
spotify_credentials_object = SpotifyClientCredentials(
    client_id     = os.getenv('SPOTIFY_CLIENT_ID'),
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    )



with DAG('artist-data-to-redshift',
          description       = 'DAG that retrieves data from  and saves it into a table in a Data Warehouse',
          schedule_interval = '0 12 * * *',
          catchup           = False,
          default_args      = default_args) as dag:

    get_data = PythonOperator(
        task_id         = 'get_data_from_api', 
        python_callable = retrieve_data_from_api, 
        dag             = dag, 
        provide_context = True, 
        op_kwargs       = {'spotify_credentials': spotify_credentials_object}
        )

    save_data = PythonOperator(
        task_id         = 'load_data_to_redshift', 
        python_callable = load_data_to_dw, 
        dag             = dag, 
        provide_context = True
        )

    get_data >> save_data 