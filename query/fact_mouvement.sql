/****** Script for SelectTopNRows command from SSMS  ******/
SELECT date_mouvement , PROD.id_produit ,entr.id_entrepot 
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
  left join [dbo].[dim_entrepot] entr on stg_m.id_entrepot =entr.id_entrepot


  