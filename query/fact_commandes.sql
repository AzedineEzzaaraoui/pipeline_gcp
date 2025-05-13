

SELECT  id_commande,prod.id_produit ,dim.id_entrepot ,
upper(DATENAME(MONTH, stg_com.date_commande) ) AS Month_Name ,
upper(DATENAME(YEAR, stg_com.date_commande) ) AS  Year_name ,
COUNT(*) OVER() AS nombre_total_commandes ,
sum(prix_total) over () CA ,
SUM(quantite) OVER(PARTITION BY stg_com.id_produit) quantite_vendu_par_roduit
  FROM [Fact_Commandes].[dbo].[stg_commandes] stg_com
  left join [dbo].[dim_entrepot] dim on dim.id_entrepot=stg_com.id_entrepot
  left join [dbo].[dim_produit] prod on prod.id_produit=stg_com.id_produit
  


 select * from [Fact_Commandes].[dbo].[stg_commandes] 


 select * from [dbo].[dim_produit]


 select * from [dbo].[dim_entrepot]

