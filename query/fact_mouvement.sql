/****** Script for SelectTopNRows command from SSMS  ******/
SELECT date_mouvement , PROD.id_produit ,entr.id_entrepot 
, SUM( case when type_mouvement='r�ception' then quantite else 0 end ) over( partition by stg_m.id_produit ) Total_r�ceptionn�_par_produit ,
  SUM( case when type_mouvement='exp�dition' then quantite else 0 end ) over( partition by stg_m.id_produit ) Total_exp�diti�_par_produit ,
   (SUM(CASE WHEN type_mouvement = 'r�ception' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_produit) 
     - 
     SUM(CASE WHEN type_mouvement = 'exp�dition' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_produit)
    ) AS Stock_th�orique_par_produit ,
	 (SUM(CASE WHEN type_mouvement = 'r�ception' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_entrepot) 
     - 
     SUM(CASE WHEN type_mouvement = 'exp�dition' THEN quantite ELSE 0 END) OVER(PARTITION BY stg_m.id_entrepot)
    ) AS Stock_th�orique_par_entrepot
  FROM [Fact_Commandes].[dbo].[stg_mouvements] stg_m
  left join [dbo].[dim_produit] PROD ON stg_m.id_produit=PROD.id_produit
  left join [dbo].[dim_entrepot] entr on stg_m.id_entrepot =entr.id_entrepot


  