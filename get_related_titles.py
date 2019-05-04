import psycopg2
import config
import json


def get_related_titles(surname, offset, per_page):
    connection = None
    wk_db_name = config.IDBM_DB_NAME
    try:
        # Establish connection
        connection = psycopg2.connect(user=config.DATABASE_CONFIG['DB_USER'],
                                      password=config.DATABASE_CONFIG['DB_PWD'],
                                      host=config.DATABASE_CONFIG['DB_HOST'],
                                      port=config.DATABASE_CONFIG['DB_PORT'],
                                      database=wk_db_name)
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


def main():
    get_related_titles(surname='', offset=, per_page=)


if __name__ == "__main__":
    main()







