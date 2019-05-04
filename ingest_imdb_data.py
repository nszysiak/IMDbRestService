import psycopg2
import config
import os
import gzip
import csv


def create_database():

    connection = None
    wk_db_name = config.IDBM_DB_NAME

    try:
        # Establish connection
        connection = psycopg2.connect(user=config.DATABASE_CONFIG['DB_USER'],
                                      password=config.DATABASE_CONFIG['DB_PWD'],
                                      host=config.DATABASE_CONFIG['DB_HOST'],
                                      port=config.DATABASE_CONFIG['DB_PORT'],
                                      database=config.DATABASE_CONFIG['DB_NAME'])
        # Autocommit true
        connection.autocommit = True
        print("Database connected successfully!")
        # Create cursor
        cur = connection.cursor()
        # Terminate all sessions in the working database except yours session
        cur.execute(config.TERMINATE_SESSIONS % wk_db_name)
        # Create counter database statement
        cnt_stmt = (config.COUNT_DATABASES % wk_db_name)
        # Execute counter database statement
        cur.execute(cnt_stmt)
        # Fetch amount od provided databases
        cnt_pointer = cur.fetchone()
        # Create or drop database if already exists and then create
        if cnt_pointer[0] == 1:
            cur.execute(config.DROP_DB % wk_db_name)
            cur.execute(config.CREATE_DB % wk_db_name)
        else:
            cur.execute(config.CREATE_DB % wk_db_name)
        print(wk_db_name + " database has been created.")
        return wk_db_name
    except (Exception, psycopg2.Error) as error:
        print("Error while creating/dropping database: ", error)
    finally:
        if connection is not None:
            # Close connection
            connection.close()
            print('Database connection closed.')


def create_tables(db_name):
    connection = None
    commands = config.CREATE_TABLES_STMT
    try:
        # Establish connection
        connection = psycopg2.connect(user=config.DATABASE_CONFIG['DB_USER'],
                                      password=config.DATABASE_CONFIG['DB_PWD'],
                                      host=config.DATABASE_CONFIG['DB_HOST'],
                                      port=config.DATABASE_CONFIG['DB_PORT'],
                                      database=db_name)
        # Autocommit true
        connection.autocommit = True
        # Create cursor
        cur = connection.cursor()
        print("Trying to create tables...")
        # Create tables on provided database
        for command in commands:
            cur.execute(command)
            print("Table has been created.")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating/dropping tables: ", error)
    finally:
        if connection is not None:
            # Close connection
            connection.close()
            print('Database connection closed.')


def insert_tsv_data(db_name):
    connection = None
    try:
        # Establish connection
        connection = psycopg2.connect(user=config.DATABASE_CONFIG['DB_USER'],
                                      password=config.DATABASE_CONFIG['DB_PWD'],
                                      host=config.DATABASE_CONFIG['DB_HOST'],
                                      port=config.DATABASE_CONFIG['DB_PORT'],
                                      database=db_name)
        # Autocommit true
        connection.autocommit = True
        # Create cursor
        cur = connection.cursor()

        for filename, path in config.ZIP_FILE_PATHS.items():
            # Open gzip file
            inp = gzip.open(os.path.join(path, filename), 'rb')
            # Read from the gzip file
            s = inp.read()
            # Close file
            inp.close()

            # Replace extension
            out_file_name = filename.replace('.gz', '')
            # Open new tsv file
            output = open(os.path.join(config.EXTRACTED_FILE_PATH, out_file_name), 'wb')
            # Write content from gzip file to an opened, tsv file
            output.write(s)
            # Close file
            output.close()

            # Open tsv file with utf-8 encoding
            with open(os.path.join(config.EXTRACTED_FILE_PATH, out_file_name), encoding="utf8") as output_file_path:
                # Create csv reader with tabular delimiter
                tsv_rows_reader = csv.reader(output_file_path, delimiter='\t')
                # Skipping first header row and inserts rows with proper data
                next(tsv_rows_reader)
                print("Inserting rows...")
                # Iterate on rows
                if filename == config.TITLE_FILE_NAME:
                    for row in tsv_rows_reader:
                        # Create a values row to insert
                        record_to_insert = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], '{' + row[8] + '}')
                        # Execute statement and row
                        cur.execute(config.INSERT_TITLE, record_to_insert)
                        print('')
                if filename == config.NAME_FILE_NAME:
                    for row in tsv_rows_reader:
                        # Create a values row to insert
                        record_to_insert = (row[0], row[1], row[2], row[3], '{' + row[4] + '}')
                        # Execute statement and row
                        cur.execute(config.INSERT_NAME, record_to_insert)
                        # Create a list of titles that correspond with an actor
                        titles_list = row[5].split(',')
                        # Insert title per actor to dictionary table
                        for i in range(len(titles_list)):
                            # Create a values row to insert
                            record_to_insert = (row[0], titles_list[i-1])
                            # Execute statement and row
                            cur.execute(config.INSERT_TITLE_PER_NAME, record_to_insert)
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting data into tables: ", error)
    finally:
        print("Inserting completed!")
        if connection is not None:
            # Close connection
            connection.close()
            print('Database connection closed.')


def main():
    db_name = create_database()
    create_tables(db_name=db_name)
    insert_tsv_data(db_name=db_name)


if __name__ == "__main__":
    main()
