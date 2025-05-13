/****** Script for SelectTopNRows command from SSMS  ******/


SELECT dim.id_entrepot ,prod.id_produit ,id_client , sum(CASE WHEN statut_livraison='livré' THEN 1 ELSE 0 END) OVER(  ) nbre_commandes_livré ,
 sum(CASE WHEN statut_livraison='en retard' THEN 1 ELSE 0 END) OVER(  )   nbre_commandes_en_retard ,
 sum(CASE WHEN statut_livraison='en transit' THEN 1 ELSE 0 END) OVER(  ) nbre_commandes_en_transit ,
 SUM(quantite) OVER() quantite_livré ,
 case when quantite <0 then  'Anomalies_livraisons' else 'Pas anomalie' end as Anomalies_livraisons ,
 DATEDIFF ( day ,date_commande,date_livraison  ) délai_livraison
 FROM [Fact_Commandes].[dbo].[stg_livrasion] stg_liv
 left join [dbo].[dim_entrepot] dim on dim.id_entrepot=cast(stg_liv.entrepot_source AS varchar)
  left join [dbo].[dim_produit] prod on prod.id_produit=cast( stg_liv.id_produit AS varchar)


  select * from [Fact_Commandes].[dbo].[stg_livrasion]

  update [Fact_Commandes].[dbo].[stg_livrasion] set id_produit='' where id_produit=102


  select * from [dbo].[dim_produit]

  ALTER TABLE  [Fact_Commandes].[dbo].[stg_livrasion]
ALTER COLUMN entrepot_source varchar(255) ;


select * from [dbo].[dim_entrepot]