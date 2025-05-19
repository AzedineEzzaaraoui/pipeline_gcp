import pandas as pd
import logging
import pyodbc
import urllib.parse
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date
import os
from dotenv import load_dotenv
import csv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def load_csv_files(base_path):
    """Load CSV files from specified directory"""
    logger.info(f"Loading CSV files from {base_path}")
    df_product = pd.read_csv(f"{base_path}\\produit.csv")
    df_entrepots = pd.read_csv(f"{base_path}\\entrepots.csv")
    df_commandes = pd.read_csv(f"{base_path}\\commandes.csv")
    df_livrasion = pd.read_csv(f"{base_path}\\livrasion.csv", sep=';')
    df_mouvements = pd.read_csv(f"{base_path}\\mouvements.csv", sep=';')  
    return df_product, df_entrepots, df_commandes, df_livrasion, df_mouvements

def transformation_df(df):
    """Clean and transform dataframe"""
    df = df.dropna()
    df = df.drop_duplicates()
    df['date_chargement'] = pd.Timestamp.now().date() 
    return df

def create_connection():
    """Create a direct pyodbc connection using environment variables"""
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

def create_table_if_not_exists(conn, name, df):
    """Create table if it doesn't exist based on DataFrame structure"""
    # Map pandas dtypes to SQL Server types
    type_map = {
        'int64': 'INT',
        'float64': 'FLOAT',
        'bool': 'BIT',
        'datetime64[ns]': 'DATETIME',
        'object': 'NVARCHAR(255)'
    }
    
    # Build CREATE TABLE statement
    columns = []
    for col_name, dtype in df.dtypes.items():
        sql_type = type_map.get(str(dtype), 'NVARCHAR(255)')
        columns.append(f"[{col_name}] {sql_type}")
    
    create_stmt = f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{name}') CREATE TABLE [{name}] ({', '.join(columns)})"
    
    # Execute create statement
    cursor = conn.cursor()
    cursor.execute(create_stmt)
    conn.commit()
    cursor.close()

def load_to_sql_direct(df, conn, name):
    """Load dataframe to SQL Server using direct pyodbc connection"""
    logger.info(f"--- Chargement de {name} dans SQL Server ---")
    try:
        # Create table if it doesn't exist
        create_table_if_not_exists(conn, name, df)
        
        # Clear existing data
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM [{name}]")
        conn.commit()
        
        # Create column list for insert statement
        columns = ', '.join([f"[{col}]" for col in df.columns])
        
        # Create parameter placeholders
        placeholders = ', '.join(['?' for _ in range(len(df.columns))])
        
        # Create insert statement
        insert_stmt = f"INSERT INTO [{name}] ({columns}) VALUES ({placeholders})"
        
        # Insert data in batches
        batch_size = 1000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            cursor.executemany(insert_stmt, batch.values.tolist())
            conn.commit()
            logger.info(f"Processed {min(i+batch_size, len(df))} of {len(df)} rows")
        
        cursor.close()
        logger.info(f"--- {name} chargé avec succès ---")
    except Exception as e:
        logger.error(f"Erreur lors du chargement de {name}: {str(e)}")
        raise

def execute_query(conn, query):
    """Execute SQL query and return result as DataFrame"""
    return pd.read_sql(query, conn)

def generate_fact_tables(conn):
    """Génère les tables de faits à partir de requêtes SQL"""
    logger.info("Generating fact tables from SQL queries")
    query_fact_commandes = """
    SELECT id_commande, prod.id_produit, dim.id_entrepot,
    UPPER(DATENAME(MONTH, stg_com.date_commande)) AS Month_Name,
    UPPER(DATENAME(YEAR, stg_com.date_commande)) AS Year_name,
    COUNT(*) OVER() AS nombre_total_commandes,
    SUM(prix_total) OVER() CA,
    SUM(quantite) OVER(PARTITION BY stg_com.id_produit) quantite_vendu_par_produit
    FROM [Fact_Commandes].[dbo].[stg_commandes] stg_com
    LEFT JOIN [dbo].[dim_entrepot] dim ON dim.id_entrepot = stg_com.id_entrepot
    LEFT JOIN [dbo].[dim_produit] prod ON prod.id_produit = stg_com.id_produit
    """

    query_fact_livraison = """
    SELECT dim.id_entrepot, prod.id_produit, id_client,
    SUM(CASE WHEN statut_livraison = 'livré' THEN 1 ELSE 0 END) OVER() nbre_commandes_livré,
    SUM(CASE WHEN statut_livraison = 'en retard' THEN 1 ELSE 0 END) OVER() nbre_commandes_en_retard,
    SUM(CASE WHEN statut_livraison = 'en transit' THEN 1 ELSE 0 END) OVER() nbre_commandes_en_transit,
    SUM(quantite) OVER() quantite_livré,
    CASE WHEN quantite < 0 THEN 'Anomalies_livraisons' ELSE 'Pas anomalie' END AS Anomalies_livraisons,
    DATEDIFF(day, date_commande, date_livraison) délai_livraison
    FROM [Fact_Commandes].[dbo].[stg_livrasion] stg_liv
    LEFT JOIN [dbo].[dim_entrepot] dim ON dim.id_entrepot = CAST(stg_liv.entrepot_source AS varchar)
    LEFT JOIN [dbo].[dim_produit] prod ON prod.id_produit = CAST(stg_liv.id_produit AS varchar)
    """

    query_fact_mouvement = """
    SELECT date_mouvement, PROD.id_produit, entr.id_entrepot,
    SUM(CASE WHEN type_mouvement = 'réception' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_produit) Total_réceptionné_par_produit,
    SUM(CASE WHEN type_mouvement = 'expédition' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_produit) Total_expéditié_par_produit,
    (SUM(CASE WHEN type_mouvement = 'réception' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_produit) - 
     SUM(CASE WHEN type_mouvement = 'expédition' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_produit)
    ) AS Stock_théorique_par_produit,
    (SUM(CASE WHEN type_mouvement = 'réception' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_entrepot) - 
     SUM(CASE WHEN type_mouvement = 'expédition' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_entrepot)
    ) AS Stock_théorique_par_entrepot
    FROM [Fact_Commandes].[dbo].[stg_mouvements] stg_m
    LEFT JOIN [dbo].[dim_produit] PROD ON stg_m.id_produit = PROD.id_produit
    LEFT JOIN [dbo].[dim_entrepot] entr ON stg_m.id_entrepot = entr.id_entrepot
    """

    df_mouvement_fact = execute_query(conn, query_fact_mouvement)
    df_livraison_fact = execute_query(conn, query_fact_livraison)
    df_commandes_fact = execute_query(conn, query_fact_commandes)
    
    return df_mouvement_fact, df_livraison_fact, df_commandes_fact

def load_to_bigquery(facts, dataset_id, project_id, job_config):
    """Load data to BigQuery"""
    logger.info(f"Connecting to BigQuery with project ID: {project_id}")
    try:
        credentials = service_account.Credentials.from_service_account_file(
            os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        for table_name, df in facts.items():
            table_id = f"{project_id}.{dataset_id}.{table_name}"
            logger.info(f"📤 Uploading {table_name} to BigQuery → {table_id}...")
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            logger.info(f"✅ Table {table_name} uploaded successfully.")
    except Exception as e:
        logger.error(f"Error loading to BigQuery: {str(e)}")
        raise

def main():
    try:
        logger.info("🏁 Début du pipeline")
        
        base_path = os.getenv('DATA_BASE_PATH')
        if not base_path:
            raise ValueError("DATA_BASE_PATH environment variable not set")
            
        df_product, df_entrepots, df_commandes, df_livrasion, df_mouvements = load_csv_files(base_path)
        
        df_product = transformation_df(df_product)
        df_entrepot = transformation_df(df_entrepots)
        df_commandes = transformation_df(df_commandes)
        df_livrasion = transformation_df(df_livrasion)
        df_mouvements = transformation_df(df_mouvements)
        
        # Create a direct connection instead of using SQLAlchemy
        conn = create_connection()
        
        # Load data using direct connection
        load_to_sql_direct(df_product, conn, "dim_produit")
        load_to_sql_direct(df_entrepot, conn, "dim_entrepot")
        load_to_sql_direct(df_commandes, conn, "stg_commandes")
        load_to_sql_direct(df_livrasion, conn, "stg_livrasion")
        load_to_sql_direct(df_mouvements, conn, "stg_mouvements")
        
        # Generate fact tables
        df_mouvement_fact, df_livraison_fact, df_commandes_fact = generate_fact_tables(conn)
        
        # Load fact tables
        load_to_sql_direct(df_mouvement_fact, conn, "fact_mouvement")
        load_to_sql_direct(df_livraison_fact, conn, "fact_livraison")
        load_to_sql_direct(df_commandes_fact, conn, "fact_commandes")
        
        # Close connection
        conn.close()
        
        facts = {
            "fact_mouvement": df_mouvement_fact,
            "fact_livraison": df_livraison_fact,
            "fact_commandes": df_commandes_fact ,
            "dim_produit" :df_product ,
            "dim_entrepot":df_entrepot
        }
        
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        project_id = os.getenv("GCP_PROJECT_ID")
        dataset_id = os.getenv("BQ_DATASET_ID")
        
        if not project_id or not dataset_id:
            raise ValueError("GCP_PROJECT_ID or BQ_DATASET_ID environment variables not set")
            
        load_to_bigquery(facts, dataset_id, project_id, job_config)

        logger.info("🏁 Pipeline terminé avec succès")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
