import psycopg2
import config
import json


class EndPoint:

    def __init__(self, db_name):
        self.db_name = db_name

    def get_related_titles(self, surname, offset, per_page):

        connection = None
        try:
            # Establish connection
            connection = psycopg2.connect(user=config.DATABASE_CONFIG['DB_USER'],
                                          password=config.DATABASE_CONFIG['DB_PWD'],
                                          host=config.DATABASE_CONFIG['DB_HOST'],
                                          port=config.DATABASE_CONFIG['DB_PORT'],
                                          database=self.db_name)
            # Autocommit true
            connection.autocommit = True
            # Create cursor
            cur = connection.cursor()
            # Execute immediately statement variables
            variables = (surname, per_page, offset)
            # Execute get_title_and_pers statement
            cur.execute(config.GET_RELATED_TITLES, variables)
            # Fetch all rows
            rows = cur.fetchall()
            # Declare list of json objects
            json_list = []
            for row in rows:
                # Create dictionaries in the loop with fetched data
                dict_data = {'original_title': row[0], 'primary_name': row[1]}
                # Create json object from dictionary
                json_data = json.dumps(dict_data)
                # Append json object to the list
                json_list.append(json_data)
            # Return json collection called results
            return json.dumps({'results': json_list})
        except (Exception, psycopg2.Error) as error:
            print("Error while retrieving data: ", error)
        finally:
            print("GET method finished successfully!")
            if connection is not None:
                # Close connection
                connection.close()
                print('Database connection closed.')

    def get_title_and_persons_based_on_genre(self, genre, offset, per_page):

        connection = None
        try:
            # Establish connection
            connection = psycopg2.connect(user=config.DATABASE_CONFIG['DB_USER'],
                                          password=config.DATABASE_CONFIG['DB_PWD'],
                                          host=config.DATABASE_CONFIG['DB_HOST'],
                                          port=config.DATABASE_CONFIG['DB_PORT'],
                                          database=self.db_name)
            # Autocommit true
            connection.autocommit = True
            # Create cursor
            cur = connection.cursor()
            # Execute immediately statement variables
            variables = (genre, per_page, offset)
            # Execute get_title_and_pers statement
            cur.execute(config.GET_TITLE_AND_PERS_BASED_ON_GENRE, variables)
            # Fetch all rows
            rows = cur.fetchall()
            # Declare list of json objects
            json_list = []
            for row in rows:
                # Create dictionaries in the loop with fetched data
                dict_data = {'original_title': row[0], 'primary_name': row[1]}
                # Create json object from dictionary
                json_data = json.dumps(dict_data)
                # Append json object to the list
                json_list.append(json_data)
            # Return json collection called results
            return json.dumps({'results': json_list})
        except (Exception, psycopg2.Error) as error:
            print("Error while retrieving data: ", error)
        finally:
            print("GET method finished successfully!")
            if connection is not None:
                # Close connection
                connection.close()
                print('Database connection closed.')

    def get_title_and_persons_based_on_yr(self, start_year, offset, per_page):

        connection = None
        try:
            # Establish connection
            connection = psycopg2.connect(user=config.DATABASE_CONFIG['DB_USER'],
                                          password=config.DATABASE_CONFIG['DB_PWD'],
                                          host=config.DATABASE_CONFIG['DB_HOST'],
                                          port=config.DATABASE_CONFIG['DB_PORT'],
                                          database=self.db_name)
            # Autocommit true
            connection.autocommit = True
            # Create cursor
            cur = connection.cursor()
            # Execute immediately statement variables
            variables = (start_year, per_page, offset)
            # Execute get_title_and_pers statement
            cur.execute(config.GET_TITLE_AND_PERS_BASED_ON_YR, variables)
            # Fetch all rows
            rows = cur.fetchall()
            # Declare list of json objects
            json_list = []
            for row in rows:
                # Create dictionaries in the loop with fetched data
                dict_data = {'original_title': row[0], 'primary_name': row[1]}
                # Create json object from dictionary
                json_data = json.dumps(dict_data)
                # Append json object to the list
                json_list.append(json_data)
            # Return json collection called results
            return json.dumps({'results': json_list})
        except (Exception, psycopg2.Error) as error:
            print("Error while retrieving data: ", error)
        finally:
            print("GET method finished successfully!")
            if connection is not None:
                # Close connection
                connection.close()
                print('Database connection closed.')
