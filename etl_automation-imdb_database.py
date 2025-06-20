import pandas as pd
import os
import shutil
import sqlite3
import logging
import requests

#Logging Configuration
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=log_format)

#Loggin Handler
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "script_logs.log")
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter(log_format))
logging.getLogger().addHandler(file_handler)

#Definition for Data Scraping at the website
DATABASE = "imdb_data.db"
DIRECTORY = "data"
URL = "https://datasets.imdbws.com/"
FILES = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz"
]


#Function for Data Extraction
def extract(
    data_url: str = URL,
    files: list = FILES,
    data_dir: str = DIRECTORY,
) -> None:
    """
    TODO: Docstring.
    """

    os.makedirs(data_dir, exist_ok=True)

    for file in files:
        url = data_url + file
        dir_path = os.path.join(data_dir, file)

        if not os.path.exists(dir_path):
            logging.info(f"Downloading {file}...")
            response = requests.get(url, stream=True)

            if response.status_code == 200:
                with open(dir_path, "wb") as f:
                    shutil.copyfileobj(response.raw, f)

                logging.info(f"{file} successfully downloaded!")

            else:
                logging.error(f"Fail downloading {file}! Status Code: {response.status_code}")

            del response
        else:
            logging.info(f"{file} already exists! Skipping download...")

    logging.info("Download Finished!")


#Function for Data Transform
def transform(
    files: list = FILES,
) -> None:
    """
    TODO: Docstring.
    """

    data_dir = "data"
    df_dir = os.path.join(data_dir, "processed")

    os.makedirs(df_dir, exist_ok=True)

    for file in files:
        dir_path = os.path.join(data_dir, file)

        if os.path.isfile(dir_path) and file.endswith(".gz"):
            logging.debug(f"Reading and processing data {file}...")

            df = pd.read_csv(
                dir_path,
                sep="\t",
                compression="gzip",
                low_memory=False,
                nrows=1_000
                #chunksize=1_000_000
            )

            df.replace({"\\N": None}, inplace=True)

            dest_dir = os.path.join(df_dir, file[:-3])
            df.to_csv(dest_dir, sep="\t", index=False)

            logging.debug(f"Processing done for {file}. Processed file saved in {dest_dir}!")

            #Remove file after processing
            #os.remove(dir_path)

    logging.info("All files processed and saved on folder 'processed'!")


#Function for Data Load
def load(
    conn
) -> None:
    """
    TODO: Docstring.
    """

    df_dir = os.path.join("data", "processed")

    files = os.listdir(df_dir)

    for file in files:
        file_path = os.path.join(df_dir, file)

        if os.path.isfile(file_path) and file.endswith(".tsv"):
            df = pd.read_csv(file_path, sep="\t", low_memory=False)

            table_name = os.path.splitext(file)[0]
            table_name = table_name.replace(".", "_").replace("-", "_")

            df.to_sql(table_name, conn, index=False, if_exists="replace")

            logging.info(f"File {file} saved as table {table_name} to the database.")

            #Remove file path after load
            #os.remove(file_path)

    logging.info("All files have been saved to the database!")


#Function for Data Testing and Set Analytic Views
def analytic_views(
    conn
) -> None:
    """
    TODO: Docstring.
    """

    title_analytics = """
    CREATE TABLE IF NOT EXISTS title_analytics AS

    WITH 
    participants AS (
        SELECT
            tconst,
            COUNT(DISTINCT nconst) as qtParticipants
        
        FROM title_principals
        
        GROUP BY 1
    )

    SELECT
        tb.tconst,
        tb.titleType,
        tb.originalTitle,
        tb.startYear,
        tb.endYear,
        tb.genres,
        tr.averageRating,
        tr.numVotes,
        tp.qtParticipants

    FROM title_basics tb 

    LEFT JOIN title_ratings tr
        ON tr.tconst = tb.tconst

    LEFT JOIN participants tp
        ON tp.tconst = tb.tconst
    """

    participants_analytics = """
    CREATE TABLE IF NOT EXISTS participants_analytics AS

    SELECT
        tp.nconst,
        tp.tconst,
        tp.ordering,
        tp.category,
        tb.genres

    FROM title_principals tp

    LEFT JOIN title_basics tb
        ON tb.tconst = tp.tconst
    """

    queries = [
        "DROP TABLE IF EXISTS title_analytics",
        "DROP TABLE IF EXISTS participants_analytics",
        title_analytics,
        participants_analytics
    ]

    logging.info("Saving analytical tables to the database...")

    for query in queries:
        conn.execute(query)
        

    logging.info("Analytical tables successfully created!")

    print("End of ETL Process!")


#ETL Execution
if __name__ == "__main__":
    extract()
    transform()

    conn = sqlite3.connect(DATABASE)

    load(conn=conn)
    analytic_views(conn=conn)

    conn.close()