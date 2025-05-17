import pandas as pd
import logging
import pyodbc
from sqlalchemy import create_engine
import urllib.parse
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date
import os
from dotenv import load_dotenv

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
    df_product = pd.read_csv(os.path.join(base_path, 'produit.csv'))
    df_entrepots = pd.read_csv(os.path.join(base_path, 'entrepots.csv'))
    df_commandes = pd.read_csv(os.path.join(base_path, 'commandes.csv'))
    df_livrasion = pd.read_csv(os.path.join(base_path, 'livrasion.csv'), sep=';')
    df_mouvements = pd.read_csv(os.path.join(base_path, 'mouvements.csv'), sep=';')
    return df_product, df_entrepots, df_commandes, df_livrasion, df_mouvements 
    return df_product, df_entrepots, df_commandes, df_livrasion, df_mouvements

def transformation_df(df):
    """Clean and transform dataframe"""
    df = df.dropna()
    df = df.drop_duplicates()
    df['date_chargement'] = pd.Timestamp.now().date() 
    return df

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

def load_to_sql(df, engine, name):
    """Load dataframe to SQL Server"""
    logger.info(f" ---Chargement de {name} dans SQL Server---")
    df.to_sql(
        name=name,
        con=engine,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )

def generate_fact_tables(engine):
    """Generate fact tables from SQL queries"""
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

    df_mouvement_fact = pd.read_sql(query_fact_mouvement, engine)
    df_livraison_fact = pd.read_sql(query_fact_livraison, engine)
    df_commandes_fact = pd.read_sql(query_fact_commandes, engine)
    
    return df_mouvement_fact, df_livraison_fact, df_commandes_fact

def load_to_bigquery(facts, dataset_id, project_id, job_config):
    """Load data to BigQuery"""
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

def main():
    logger.info("🏁 Début du pipeline")
    
    # Load data
    base_path = os.getenv('DATA_BASE_PATH')
    df_product, df_entrepots, df_commandes, df_livrasion, df_mouvements = load_csv_files(base_path)
    
    # Transform data
    df_product = transformation_df(df_product)
    df_entrepot = transformation_df(df_entrepots)
    df_commandes = transformation_df(df_commandes)
    df_livrasion = transformation_df(df_livrasion)
    df_mouvements = transformation_df(df_mouvements)
    
    # SQL operations
    engine = create_sql_engine()
    df_mouvement_fact, df_livraison_fact, df_commandes_fact = generate_fact_tables(engine)
    
    # Load to SQL
    load_to_sql(df_product, engine, "dim_produit")
    load_to_sql(df_entrepot, engine, "dim_entrepot")
    load_to_sql(df_commandes, engine, "stg_commandes")
    load_to_sql(df_livrasion, engine, "stg_livrasion")
    load_to_sql(df_mouvements, engine, "stg_mouvements")
    load_to_sql(df_mouvement_fact, engine, "FACT_Mouvement")
    load_to_sql(df_livraison_fact, engine, "FACT_Livraison")
    load_to_sql(df_commandes_fact, engine, "FACT_commandes")
    
    # Load to BigQuery
    facts = {
        "FACT_Mouvement": df_mouvement_fact,
        "FACT_Livraison": df_livraison_fact,
        "FACT_commandes": df_commandes_fact,
        "dim_produit": df_product,
        "dim_entrepot": df_entrepot
    }
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    load_to_bigquery(
        facts=facts,
        dataset_id=os.getenv('BQ_DATASET_ID'),
        project_id=os.getenv('GCP_PROJECT_ID'),
        job_config=job_config
    )
    
    logger.info("✅ Pipeline exécuté avec succès")

if __name__ == "__main__":
    main()
