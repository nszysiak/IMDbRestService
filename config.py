import os

dirpath = os.getcwd()

# Database configuration

DATABASE_CONFIG = {
    'DB_USER': 'postgres',
    'DB_PWD': '',
    'DB_HOST': 'localhost',
    'DB_PORT': 5432,
    'DB_NAME': 'postgres'
}

# Terminate all sessions except yours

TERMINATE_SESSIONS = "SELECT  pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '%s';"

# Count databases

COUNT_DATABASES = "SELECT COUNT(1) FROM pg_database WHERE datname = '%s' ;"

# Drop db statement

DROP_DB = "DROP DATABASE %s ;"

# Create db statement

CREATE_DB = "CREATE DATABASE %s  ;"



# SQL statements to create tables

CREATE_TABLES_STMT = (
        """
        CREATE TABLE IF NOT EXISTS title (
            title_id VARCHAR(2000) PRIMARY KEY NOT NULL,
            title_type VARCHAR(2000) NOT NULL,
            primary_title VARCHAR(2000),
            original_title VARCHAR(2000),
            is_adult VARCHAR(2000),
            start_year VARCHAR(2000),
            end_year VARCHAR(2000),
            run_time_minutes VARCHAR(2000),
            genres VARCHAR(2000)[]
        )
        """,
        """ CREATE TABLE IF NOT EXISTS name (
                name_id VARCHAR(2000) PRIMARY KEY NOT NULL,
                primary_name VARCHAR(2000),
                birth_year VARCHAR(2000),
                death_year VARCHAR(2000),
                primary_profession VARCHAR(2000)[]
                )
        """,
        """ CREATE TABLE IF NOT EXISTS title_per_name (
                        name_id VARCHAR(2000) REFERENCES name(name_id),
                        title VARCHAR(2000) REFERENCES title(title_id)
                        )
                """
)
# Name of the new database

IDBM_DB_NAME = 'idbm'

# File name of title data

TITLE_FILE_NAME = 'title.basics.tsv.gz'

# File name of names data

NAME_FILE_NAME = 'name.basics.tsv.gz'

# Zip file names and it's paths

ZIP_FILE_PATHS = {TITLE_FILE_NAME: dirpath + '\IOFiles\IFiles', NAME_FILE_NAME: dirpath + '\IOFiles\IFiles'}

# Path to extracted zip files

EXTRACTED_FILE_PATH = dirpath + '\IOFiles\OFiles'

#Insert into title statement

INSERT_TITLE = "INSERT INTO title(title_id, title_type, primary_title, original_title, is_adult, start_year, end_year, run_time_minutes, genres) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"

# Insert into name statement

INSERT_NAME = "INSERT INTO name(name_id, primary_name, birth_year, death_year, primary_profession) VALUES (%s, %s, %s, %s, %s);"

# Insert into title_per_name statement

INSERT_TITLE_PER_NAME = "INSERT INTO title_per_name(name_id, title) VALUES (%s, %s);"

# Select get_title_and_per_based_on_yr statement

GET_TITLE_AND_PERS_BASED_ON_YR = "SELECT t.original_title, n.primary_name FROM title AS t, title_per_name AS tpn, name AS n WHERE t.title_id = tpn.title AND n.name_id = tpn.name_id AND t.start_year = '%s' ORDER BY t.original_title DESC LIMIT %s OFFSET %s;"

# Select get_title_and_per_based_on_genre statement

GET_TITLE_AND_PERS_BASED_ON_GENRE = "SELECT t.original_title, n.primary_name FROM title AS t, title_per_name AS tpn, name AS n WHERE t.title_id = tpn.title AND n.name_id = tpn.name_id AND t.start_year = '%s' ORDER BY t.original_title DESC LIMIT %s OFFSET %s;"

# Select get_related_titles statement

GET_RELATED_TITLES = "SELECT t.original_title FROM title AS t INNER JOIN title_per_name AS tpn ON t.title_id = tpn.title WHERE tpn.name_id IN (SELECT tpn.name_id FROM title_per_name AS tpn WHERE tpn.title IN (SELECT tpn.title FROM title_per_name AS tpn INNER JOIN name AS n ON tpn.name_id = n.name_id WHERE n.primary_name = '%s')) LIMIT %s OFFSET %s"


