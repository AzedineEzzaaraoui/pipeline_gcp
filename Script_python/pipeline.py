import pandas as pd
import logging
import pyodbc
from sqlalchemy import create_engine
import urllib.parse
from google.cloud import bigquery
from google.oauth2 import service_account

import os
#load variables from .env
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Affiche les logs dans la console
    ]
)

logger = logging.getLogger(__name__)
def load_csv_files(base_path):
    
    df_product = pd.read_csv(f"{base_path}\\produit.csv")
    df_entrepots=pd.read_csv(fr"{base_path}\entrepots.csv")
    df_commandes=pd.read_csv(fr"{base_path}\commandes.csv" )
    df_livrasion=pd.read_csv(fr"{base_path}\livrasion.csv " ,sep=';')
    df_mouvements =pd.read_csv(fr"{base_path}\mouvements.csv" ,sep=';')  
    return df_product , df_entrepots ,df_commandes,df_livrasion ,df_mouvements

def transformation_df(df):
   
   df['date_chargement']=pd.Timestamp.now().date() 
   df = df.dropna(how="all").drop_duplicates()
   return df

def create_sql_engine(server, database, username, password, driver):
    conn_str = (
        f"mssql+pyodbc://{username}:{urllib.parse.quote_plus(password)}@"
        f"{server}/{database}?driver={urllib.parse.quote_plus(driver)}"
    )
    return create_engine(conn_str, fast_executemany=True, pool_pre_ping=True, connect_args={"connect_timeout": 30})


def load_to_sql(df, engine, name):
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

  query_fact_commandes = """SELECT  id_commande,prod.id_produit ,dim.id_entrepot ,
  upper(DATENAME(MONTH, stg_com.date_commande) ) AS Month_Name ,
  upper(DATENAME(YEAR, stg_com.date_commande) ) AS  Year_name ,
  COUNT(*) OVER() AS nombre_total_commandes ,
  sum(prix_total) over () CA ,
  SUM(quantite) OVER(PARTITION BY stg_com.id_produit) quantite_vendu_par_roduit
  FROM [Fact_Commandes].[dbo].[stg_commandes] stg_com
  left join [dbo].[dim_entrepot] dim on dim.id_entrepot=stg_com.id_entrepot
  left join [dbo].[dim_produit] prod on prod.id_produit=stg_com.id_produit """
  
  query_fact_livraison = """SELECT dim.id_entrepot ,prod.id_produit ,id_client , 
 sum(CASE WHEN statut_livraison='livré' THEN 1 ELSE 0 END) OVER(  ) nbre_commandes_livré ,
 sum(CASE WHEN statut_livraison='en retard' THEN 1 ELSE 0 END) OVER(  )   nbre_commandes_en_retard ,
 sum(CASE WHEN statut_livraison='en transit' THEN 1 ELSE 0 END) OVER(  ) nbre_commandes_en_transit ,
 SUM(quantite) OVER() quantite_livré ,
 case when quantite <0 then  'Anomalies_livraisons' else 'Pas anomalie' end as Anomalies_livraisons ,
 DATEDIFF ( day ,date_commande,date_livraison  ) délai_livraison
 FROM [Fact_Commandes].[dbo].[stg_livrasion] stg_liv
 left join [dbo].[dim_entrepot] dim on dim.id_entrepot=cast(stg_liv.entrepot_source AS varchar)
  left join [dbo].[dim_produit] prod on prod.id_produit=cast( stg_liv.id_produit AS varchar) """
    
  query_fact_mouvement = """SELECT date_mouvement , PROD.id_produit ,entr.id_entrepot 
, SUM( case when type_mouvement='réception' then quantite else 0 end ) over( partition by stg_m.id_produit ) Total_réceptionné_par_produit ,
  SUM( case when type_mouvement='expédition' then quantite else 0 end ) over( partition by stg_m.id_produit ) Total_expéditié_par_produit ,
   (SUM(CASE WHEN type_mouvement = 'réception' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_produit) 
     - 
     SUM(CASE WHEN type_mouvement = 'expédition' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_produit)
    ) AS Stock_théorique_par_produit ,
	 (SUM(CASE WHEN type_mouvement = 'réception' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_entrepot) 
     - 
     SUM(CASE WHEN type_mouvement = 'expédition' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_entrepot)
    ) AS Stock_théorique_par_entrepot
  FROM [Fact_Commandes].[dbo].[stg_mouvements] stg_m
  left join [dbo].[dim_produit] PROD ON stg_m.id_produit=PROD.id_produit
  left join [dbo].[dim_entrepot] entr on stg_m.id_entrepot =entr.id_entrepot """

  df_mouvement_fact = pd.read_sql(query_fact_mouvement, engine)
  df_livraison_fact = pd.read_sql(query_fact_livraison, engine)
  df_commandes_fact = pd.read_sql(query_fact_commandes, engine)
  return df_mouvement_fact , df_livraison_fact  ,df_commandes_fact


def load_to_bigquery(facts, credentials, dataset_id  , project_id , job_config):
    
    client = bigquery.Client(credentials=credentials, project=project_id)
    
    for table_name, df in facts.items():
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        print(f"📤 Uploading {table_name} to BigQuery → {table_id} ...")
       
        
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for job to complete

        print(f"✅ Table {table_name} uploaded successfully.")

def main():
    logger.info("🏁 Début du pipeline")
    credential_path = r"C:\Users\user\Pipeline_09_05_2025\credential_path\bigquery_credentials.json"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    credentials = service_account.Credentials.from_service_account_file(
    credential_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],)
    base_path = r'C:\Users\user\Pipeline_09_05_2025\data'
    logger.info("🏁 Début chargement les fichiers")
    df_product, df_entrepots, df_commandes, df_livrasion, df_mouvements = load_csv_files(base_path)
    logger.info("🏁 Début transformation les données")
    df_product=transformation_df(df_product)
    df_entrepots=transformation_df(df_entrepots)
    df_commandes=transformation_df(df_commandes)
    df_livrasion=transformation_df(df_livrasion)
    df_mouvements=transformation_df(df_mouvements)
    #print(df_product.head())
    #print(df_entrepots.head())
    #print(df_commandes.head())
    #print(df_livrasion.head())
    #print(df_mouvements.head())
    

    engine = create_sql_engine(
        server='DESKTOP-Q8EQVSL\\MSSQLSERVER2022',
        database='Fact_Commandes',
        username='aze',
        password='aze',
        driver='ODBC Driver 17 for SQL Server'
    )
    df_mouvement_fact , df_livraison_fact  ,df_commandes_fact=generate_fact_tables(engine)
    load_to_sql(df_product, engine, "dim_produit")
    load_to_sql(df_entrepots, engine, "dim_entrepot")
    load_to_sql(df_commandes, engine, "stg_commandes")
    load_to_sql(df_livrasion, engine, "stg_livrasion")
    load_to_sql(df_mouvements, engine, "stg_mouvements")
    load_to_sql(df_mouvement_fact, engine, "FACT_Mouvement")
    load_to_sql(df_livraison_fact, engine, "FACT_Livraison")
    load_to_sql(df_commandes_fact, engine, "FACT_commandes")
    facts = {
    "FACT_Mouvement": df_mouvement_fact,
    "FACT_Livraison": df_livraison_fact,
    "FACT_commandes": df_commandes_fact
     }
    load_to_bigquery(
    facts=facts,
    credentials=credentials,
    dataset_id="vente",
    project_id="pipeline-458019",
    job_config=job_config )
    
if __name__ == "__main__" :
    main()
