
from datetime import date

%run ./pipeline_GCP.ipynb

def test_transformation_df():
    
    df = pd.DataFrame({
        "col1": [1, None, None, 1, 1],
        "col2": [1, None, None, 1, 2]
    })
    
    transformed = transformation_df(df)
    print("Résultat transformé :")
    print(transformed)
    

    assert "date_chargement" in transformed.columns
    assert transformed["date_chargement"].iloc[0] == date.today()
    assert len(transformed) == 2  
    assert pd.isna(transformed[['col1', 'col2']]).sum().sum() == 0  
    print("✅ Test FINAL réussi")

def create_sql_engine():
    """Create SQL Alchemy engine using environment variables"""
    # Charger les variables d'environnement si ce n'est pas déjà fait
    load_dotenv()
    
    # Utiliser la même approche que celle qui fonctionne avec pyodbc
    server_name = os.getenv('SQL_SERVER').replace('\\\\', '\\')
    
    # Construction de la chaîne de connexion ODBC exactement comme dans votre exemple fonctionnel
    odbc_conn_str = (
        f"DRIVER={{{os.getenv('SQL_DRIVER')}}};"
        f"SERVER={server_name};"
        f"DATABASE={os.getenv('SQL_DATABASE')};"
        f"UID={os.getenv('SQL_USERNAME')};"
        f"PWD={os.getenv('SQL_PASSWORD')};"
        f"TrustServerCertificate=yes;"
    )
    
    # Utilisation de la notation pyodbc:// avec la chaîne encodée
    conn_str = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_conn_str)}"
    
    # Création du moteur SQLAlchemy avec des paramètres optimisés
    return create_engine(
        conn_str, 
        fast_executemany=True,  # Optimisation pour les insertions multiples
        pool_pre_ping=True      # Vérifie que la connexion est toujours active
    )


def test_sql_server_integration():
    """Teste que les données sont bien stockées dans SQL Server"""

    engine = create_sql_engine()

    tables_to_check = [
        "dim_produit",
        "dim_entrepot",
        "stg_commandes",
        "stg_livrasion",
        "stg_mouvements",
        "FACT_Mouvement",
        "FACT_Livraison",
        "FACT_commandes"
    ]
    
    
    for table in tables_to_check:
      try:  
        df = pd.read_sql(f"SELECT TOP 1 * FROM {table}", engine)
        if  df.empty :
            logger.info(f"la  Table {table} est vide")
        else :
            logger.info(f"✅ Table {table} contient des données ")
      except Exception as e:
            logger.error(f"❌ Erreur sur la table {table} : {str(e)}")
            raise

def test_bigquery_integration():
    """Teste que les données sont bien stockées dans BigQuery"""

    credential_path = r"C:\Users\user\Pipeline_09_05_2025\credential_path\bigquery_credentials.json"
    credentials = service_account.Credentials.from_service_account_file(
    credential_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],)
    client = bigquery.Client(credentials=credentials, project="pipeline-458019")
    
    

    tables_to_check = {
        "FACT_Mouvement": "vente",
        "FACT_Livraison": "vente",
        "FACT_commandes": "vente" ,
         "dim_produit" :"vente" ,
         "dim_entrepot" :"vente"
    }
    
    # 3. Vérification de chaque table
    for table, dataset in tables_to_check.items():
        try:
            query = f"SELECT * FROM `pipeline-458019.{dataset}.{table}` LIMIT 1"
            df = client.query(query).to_dataframe()
            assert not df.empty, f"La table {table} est vide"
            logger.info(f"✅ Table BigQuery {dataset}.{table} contient des données")
        except Exception as e:
            logger.error(f"❌ Erreur sur la table {dataset}.{table} : {str(e)}")
            raise

    logger.info("Test BigQuery réussi - Toutes les tables contiennent des données")

def main():
    test_transformation_df()
    test_sql_server_integration()
    test_bigquery_integration()

if __name__ == "__main__" :
    main()