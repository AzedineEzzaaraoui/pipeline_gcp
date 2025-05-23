
from pipeline_GCP import *

from datetime import date
from dotenv import load_dotenv
load_dotenv()


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
    
    server_name = os.getenv('SQL_SERVER').replace('\\\\', '\\')
    conn_str = (
        f"DRIVER={{{os.getenv('SQL_DRIVER')}}};"
        f"SERVER={server_name};"
        f"DATABASE={os.getenv('SQL_DATABASE')};"
        f"UID={os.getenv('SQL_USERNAME')};"
        f"PWD={os.getenv('SQL_PASSWORD')};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


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
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID")
    ##credential_path = r"C:\Users\user\Pipeline_09_05_2025\credential_path\bigquery_credentials.json"
    credentials = service_account.Credentials.from_service_account_file(
            os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
    client = bigquery.Client(credentials=credentials, project=project_id)    
    
    
   

    tables_to_check = {
        "fact_mouvement": dataset_id,
        "fact_livraison": dataset_id,
        "fact_commandes": dataset_id ,
         "dim_produit" :dataset_id ,
         "dim_entrepot" :dataset_id
    }
    
    # 3. Vérification de chaque table
    for table, dataset in tables_to_check.items():
        try:
            query = f"SELECT * FROM `{project_id}.{dataset}.{table}` LIMIT 1"
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
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID")
    test_bigquery_integration()
    
if __name__ == "__main__" :
    main()
    
    
