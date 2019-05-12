# IMDbRestService

## Getting Started

### Prerequisites

What things you need to install the software and how to install them

```
1. Delete "delete_me.txt" files from IOFiles subdirectories.
2. Instal dependencies from "requirements.txt" file.
3. Provide input .gz files into IMDbRestService\IOFiles\IFiles in order to ingest database.
4. Configure database in config.py.
5. Provide input parameters for end-points functions and pagination parameters.

In order to ingest database, please create a parametrized instance of Importer class.
End-points are under EndPoint class (functions: get_title_and_pers_based_on_yr.py, get_title_and_pers_based_on_genre.py, get_related_titles.py).
You can provide db_name parameter for each instance using config.IDBM_DB_NAME.
```
## Authors

* **Norbert Szysiak** - [NorbertSzysiak](https://github.com/nszysiak)
