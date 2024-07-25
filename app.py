import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# Chemin vers le fichier CSV
csv_file_path = 'updated_spotify_data.csv'

# Connexion à la base de données MySQL
db_config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'chargecsv' # tu mets le nom de ta base de donnée que tu as créé ici
}

# Lire le fichier CSV
df = pd.read_csv(csv_file_path)

# Nettoyer les noms de colonnes
df.columns = [col.replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_').replace('/', '_') for col in df.columns]

# Se connecter à MySQL
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Nom de la table
table_name = 'my_table' # tu peux modifier le nom de la table ici

# Créer la table automatiquement
def create_table(df, table_name):
    # Déterminer les types de données des colonnes
    dtypes = df.dtypes
    column_definitions = []

    for column_name, dtype in dtypes.items():
        if "int" in str(dtype):
            sql_type = "INT"
        elif "float" in str(dtype):
            sql_type = "FLOAT"
        elif "datetime" in str(dtype):
            sql_type = "DATETIME"
        else:
            sql_type = "VARCHAR(255)"

        column_definitions.append(f"`{column_name}` {sql_type}")

    columns_sql = ", ".join(column_definitions)
    create_table_sql = f"CREATE TABLE {table_name} ({columns_sql});"

    return create_table_sql

# Créer la table
create_table_sql = create_table(df, table_name)
cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
cursor.execute(create_table_sql)
conn.commit()

# Insérer les données
# Utiliser SQLAlchemy pour simplifier l'insertion des données
engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
df.to_sql(table_name, engine, index=False, if_exists='append')

# Fermer la connexion
cursor.close()
conn.close()

print(f"Les données du fichier CSV ont été insérées dans la table '{table_name}' avec succès.")
