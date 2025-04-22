declare date_deb date default "2022-01-01";
declare date_fin date default date_sub(date_trunc(current_date, month), interval 1 day);

create or replace table `ddp-bus-commerce-prd-frlm.animation_vente.vente_fait_rayon` as (
      --------------------------------- ca marchandises --------------------------------------------------------
       with product_ref as (

              select distinct
                     art.num_art                         as product_id, 
                     libart.lib_art                      as product_label,
                     art.num_ray                         as product_department_number,
                     libray.libnumray                    as product_department_label,
                     art.num_sray                        as product_sub_department_number,
                     libsray.libcodsray                  as product_sub_department_label,
                     art.num_typ                         as product_type_number,
                     libtyp.libcodtyp                    as product_type_label,
                     art.num_styp                        as product_sub_type_number,
                     libstyp.libcodstyp                  as product_sub_type_label,
                     art.num_typart                      

              from `dfdp-teradata6y.ProductCatalogLmfr.TA001_RAR_ART` as art

              left join `dfdp-teradata6y.ProductCatalogLmfr.TD001_RAR_LIBART`  as libart
                            on art.num_art = libart.num_art

              left join `dfdp-teradata6y.BaseGeneriqueLmfr.TA001_BAG_LIBRAY`  as libray
                            on art.cod_ray = libray.numray

              left join  `dfdp-teradata6y.BaseGeneriqueLmfr.TA001_BAG_LIBSRAY`  as libsray
                            on  (art.cod_ray =  libsray.numray) 
                            and (art.cod_sray = libsray.codsray)

              left join  `dfdp-teradata6y.BaseGeneriqueLmfr.TA001_BAG_LIBTYP`  as libtyp
                            on  (art.cod_typ =  libtyp.codtyp) 
                            and (art.cod_sray = libtyp.codsray) 
                            and (art.cod_ray =  libtyp.numray)
                     
              left join  `dfdp-teradata6y.BaseGeneriqueLmfr.TA001_BAG_LIBSTYP` as libstyp
                            on  (art.cod_styp = libstyp.codstyp) 
                            and (art.cod_typ =  libstyp.codtyp) 
                            and (art.cod_sray = libstyp.codsray) 
                            and (art.cod_ray =  libstyp.numray)

       ),

       flag_retrait_web AS (
              select * 
              from (
                     select num_ett_tic,
                            num_trn,
                            num_art,
                            num_ligtrn,
                            num_rgrpcli,
                            dat_trn,
                            dat_rtr,
                            top_web_retrait

                     from `lmfr-ddp-dwh-prd.customer_order_teradata.TF_CDE_CMD` sld
              )
              where top_web_retrait = 1
              
       ),

       tickets as (
              select 
                     tic.dat_vte as customer_sale_date,
                     tic.num_ett as entity_number,
                     tic.num_rgrpcli as client_id,

                     format_date('%Y%m%d',tic.dat_vte) || '~' || 
                     tic.num_tic || '~' || 
                     tic.heu_tic || '~' || 
                     tic.num_cai || '~' || 
                     tic.num_ett || '~' || 
                     tic.num_typett || '~' || 
                     tic.num_bu as customer_sale_id,

                     tic.num_cde,
                     tic.num_ligcde,
                     art.product_department_number,
                     art.product_department_label,
                     art.product_sub_department_number,
                     art.product_sub_department_label,
                     art.product_type_number,
                     art.product_type_label,
                     tic.mnt_ttcdevbu ca_ttc,
                     tic.mnt_mrg marge,
                     tic.mnt_ht ca_ht,
                     tic.qte_art qte,
                     tic.num_art,

              from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_LIGTICCAI` tic
              left join product_ref art on tic.num_art = art.product_id
                            
              where tic.num_bu = 1
              and tic.num_typett = 1
              and tic.dat_vte between date_deb and date_fin
              and tic.num_typtrn in (48,50,52,47,1,54,49,2,9,8) 
              and num_typart <> 9    
              and num_ett <> 380
       ),

       tickets_retrait as (
              select tickets.*, rweb.dat_trn, rweb.dat_rtr, rweb.top_web_retrait
              from tickets
              left join flag_retrait_web rweb
                     on ( tickets.entity_number = rweb.num_ett_tic
                     and tickets.num_cde = rweb.num_trn
                     and tickets.num_art = rweb.num_art
                     and tickets.num_ligcde = rweb.num_ligtrn + 1 
                     and tickets.client_id = rweb.num_rgrpcli 
              )
              where top_web_retrait is null
       ),
       
       sales_agg as (
              select date_trunc(customer_sale_date, month) Mois,
                     entity_number,
                     product_department_number,
                     sum(ca_ttc) ca_ttc_g,
                     sum(qte) qte_g,
                     sum(if(cpt_tic = 1, nb_articles_positive, -1*nb_articles_negative)) nb_articles_g,
                     sum(cpt_tic) nb_tickets_g
              from (
                     select customer_sale_id,
                            entity_number,
                            customer_sale_date,
                            product_department_number, 
                            sum(ca_ttc) ca_ttc,
                            sum(qte) qte,
                            --    count(distinct num_art) nb_articles,
                            count(distinct if(ca_ttc >= 0,num_art, null)) nb_articles_positive,
                            count(distinct if(ca_ttc < 0,num_art, null)) nb_articles_negative,
                            if(sum(ca_ttc) >= 0, 1,-1) cpt_tic

                     from tickets_retrait

                     group by all
              )
              group by all


       ),

      --------------------------------------- En - commande -----------------------------------------------------
      commandes as (

              select num_ett, 
                     cmd.num_ray,
                     mnt_ttc,
                     dat_trn,
                     dat_valligtrn,
                     dat_ligtrn,
                     dat_sldligtrn,
                     dat_anlligtrn,
                     dat_livtrn,
                     dat_livligtrn,
                     dat_rtrligtrn


              from `lmfr-ddp-dwh-prd.customer_order_teradata.TF_CDE_CMD` cmd
              left join `dfdp-teradata6y.ProductCatalogLmfr.TA001_RAR_ART` art 
                     on cmd.num_art = art.num_art 
              where true
              and   num_etaligtrn >= 20 
              and   num_etatrn >= 20 
              and   num_etaligtrn <= 90 
              and   num_etatrn <= 90 
              and   ( art.num_typart <> 9 or art.num_typart is null )
              and   num_ett != 7
      ),

      perimetre as (
          select date_sub(date, interval 1 day) as Fin_mois,  date_trunc(date, week(monday)) as last_monday
          from unnest(generate_date_array(date("2021-02-01"), date_trunc(current_date(), month), interval 1 month)) as date
          order by 1
      ),

      portefeuille as (

              select date_trunc(Fin_mois, month) mois, num_ett, num_ray, sum(mnt_ttc) mnt_ttc_portefeuille,
              from perimetre 
              left join commandes 
                     on    dat_valligtrn <= Fin_mois
                     and   dat_ligtrn <= Fin_mois
                     and   (Fin_mois < dat_rtrligtrn or dat_rtrligtrn is null)
                     and   (Fin_mois < dat_anlligtrn or dat_anlligtrn is null)
              group by 1,2,3
      ),

      --------------------------------------- Livrable -----------------------------------------------------

      livrable as (
      
              select date_trunc(Fin_mois, month) mois, num_ett, num_ray, sum(mnt_ttc) mnt_ttc_livrable,
              from perimetre 
              left join commandes 
                     on    dat_livtrn <= Fin_mois
                     and   dat_livligtrn <= Fin_mois
                     and   dat_trn <= Fin_mois
                     and   (Fin_mois < dat_rtrligtrn or dat_rtrligtrn is null)
                     and   (Fin_mois < dat_anlligtrn or dat_anlligtrn is null)
              group by 1,2,3
      ),
    --------------------------------------- Taux de rupture -----------------------------------------------------
      tx_rupture as (
              select date_trunc(perimetre.last_monday, month) as mois,
                     num_mag,
                     num_ray,
                     sum(nb_rupt) nb_rupture,
                     sum(nb_top1) nb_top1

              from perimetre
              left join `ddp-dtm-supply-prd-frlm.procurement.tf_dispo_stock` stock 
                     on perimetre.last_monday = stock.lundi
              where num_mag != 380
              and classification_abc in ('A','B') -- best_ref
              and classification_xyz in ('X','Y') -- best_ref
              group by 1,2,3
      ),
      --------------------------------------- Simulation--------------------------------------------------------

      simulation as (
              select  mois, 
                      num_ett, 
                      num_ray, 
                      sum(nb_simulation_cree) nb_simulation_cree,
                      sum(nb_simulation_integre) nb_simulation_integre,
                      sum(nb_simulation_transforme) nb_simulation_transforme
              from (
                     select date_trunc(dat_cre_ofr, month) mois,
                            num_ett,
                            case when trim(tf_sim.typ_sim) = "OAA3D:002" then 5 -- Cuisine
                                   when trim(tf_sim.typ_sim) = "OAA3D:001" then 7 -- Salle de bains
                                   when trim(tf_sim.typ_sim) = "845125b0-0cd1-406b-af3b-16e85d80a0e7" then 12 -- Store d'intérieur (Mise à dimension)
                                   when trim(tf_sim.typ_sim) = "OAA3D:006" then 7 -- Salle de bain 3D IS6
                            else safe_cast(td_sim.num_ray as int64) end as num_ray,
                            td_sim.typ_sim,
                            td_sim.lib_typ_sim,
                            -- td_prj.num_prj,
                            count(distinct cod_sim) nb_simulation_cree, 
                            count(distinct if(top_trn = 1, cod_sim, null)) nb_simulation_integre,
                            count(distinct if(top_trn = 1 and ((num_cmd is not null and num_etaligcmd between 20 and 80)
                                                 or (num_bv is not null and num_etaligbv = 210)),
                                          cod_sim,null)
                            ) as nb_simulation_transforme
                     from `lmfr-ddp-dwh-prd.customer_simulation_teradata.TF_CDE_SIM` tf_sim
                     left join `lmfr-ddp-dwh-prd.customer_order_teradata.TD_CDE_SIM` td_sim on (tf_sim.typ_sim = td_sim.typ_sim)
                     -- left join `ddp-dtm-inhabitant-prd-frlm.unv_cde001.TD_CDE_PRJ` td_prj on (td_prj.num_prj = tf_sim.num_prj)
                     where true 
                     and (cod_cansim != "internet" or cod_cansim is null)
                     and dat_cre_ofr between date_deb and date_fin
                     and tf_sim.typ_sim in ( 'OAA:002','OAA:003','OAA:004','OAA:005','OAA:006','OAA:007','OAA:009','OAA:010','OAA:011','OAA:012','OAA:014','OAA:015','OAA:016','OAA:019','OAA:020','OAA:025','OAA:027','OAA3D:001','OAA3D:002','OAA:030','OAA:032','OAA3D:005','OAA3D:006','OAA:034','OAA:035','OAA:044','OAA:045','5c27f633-537b-4ed1-b33b-70fef9a2fa7d','845125b0-0cd1-406b-af3b-16e85d80a0e7','03e20d2b-39a9-49d3-8f9e-8506c9f1a9a8','7319f742-a553-4a6f-bfd2-64ff834115d4','621a7a34-33bb-47a3-a00e-00a6742af3f7','6bb34a82-237d-4511-a164-dad73f302ad1') --,'OAV'  )
                     group by 1,2 ,3 ,4,5 --,6
              )
              group by 1 ,2,3
      ),
      --------------------------------------- Devis ---------------------------------------------------------------------

      devis as (

              select date_trunc(dat_trn, month) mois, 
                     num_ett,
                     dvs.num_ray,
                     count(distinct concat(num_typett, '-', num_ett, "-", num_typtrn, "-", num_trn)) Nb_devis_crees,
                     count(distinct if( num_etaligtrn = 110, concat(num_typett, '-', num_ett, "-", num_typtrn, "-", num_trn), null)) Nb_devis_transf,
                     sum(if( num_etaligtrn = 110, mnt_ttc,0)) mnt_ttc_devis_transf

              from `lmfr-ddp-dwh-prd.sales_operations_teradata.TF_CDE_DVS` dvs
              left join `dfdp-teradata6y.ProductCatalogLmfr.TA001_RAR_ART` art 
                     on dvs.num_art = art.num_art 
              where true 
              and dat_trn between date_deb and date_fin
              and ( art.num_typart <> 9 or art.num_typart is null )
              and (num_etaligtrn not in (90,120,220,450) or num_etatrn in (90,120,220,450))
              and abs(mnt_ttc) < 300000
              group by 1,2 ,3
      ),
      --------------------------------------- Taux association pose -----------------------------------------------------

       asso_pose as (
              select date_trunc(transaction_date, month) mois,
                     entity_number,
                     department_number,
                     sum(associated_project_count) associated_project_count,
                     sum(installation_worksite_ordered_count) installation_worksite_ordered_count
              from `ddp-dtm-services-prd-frlm.dtm_services.ta_installation_association_kpi`
              where transaction_date between date_deb and date_fin 
              group by 1,2 ,3
      ),

       kpi_rdv as (
              select   date_trunc(appointment_begin_date, month) mois,
                       entity_number,
                       NumRayon,
                       count(distinct if(appointment_canceled_flag = 0 and coalesce(appointment_status,'Venu') = 'Venu' and customer_group_number is not null, appointment_id,null)) nb_rdv_honore_identifie,
                       sum(if(appointment_canceled_flag = 0 and coalesce(appointment_status,'Venu') = 'Venu' and customer_group_number is not null, customer_completed_qualification ,0)) nb_rdv_honore_qualif_finalise

                     from `ddp-dtm-inhabitant-prd-frlm.vad.vf_customer_appointment` vfca
                     left join  `ddp-bus-commerce-prd-frlm.animation_vente.rayon_rdv` rrdv
                            on vfca.calendar_sub_type = rrdv.Nom_du_RDV
                     where appointment_begin_date between date_deb and date_fin

                     group by 1,2,3
       ),

       pascai_booster_ray as (

              select date_trunc(dat_vte, Month) Mois,
                     NUM_ETT as Num_mag, 
                     NUM_RAY as Num_rayon,
                     sum(Nbr_pascai) as Nb_passage_caisse_booster,
                     sum(mnt_net) as CA_Net,
                     sum(mnt_ht) as CA_Ht,
                     sum(qte_vte) as Qte_vendu

              from `lmfr-ddp-dwh-prd.store_sale_agg_seg_teradata.T_AGG_AGGVTE_RAYJOUR`
              where num_bu = 1
              and date(dat_vte) between  "2024-05-01" and "2024-05-31"
              group by 1,2,3

       )

      
      select coalesce(sales_agg.mois, portefeuille.mois, livrable.mois, tx_rupture.mois, devis.mois, tvte.mois, asso_pose.mois, simulation.mois, kpi_rdv.mois, pascai_booster_ray.mois) as mois,
             coalesce(sales_agg.entity_number, portefeuille.num_ett, livrable.num_ett, tx_rupture.num_mag, devis.num_ett,  tvte.Num_mag,  asso_pose.entity_number, simulation.num_ett, kpi_rdv.entity_number, pascai_booster_ray.Num_mag) as num_mag,
             coalesce(sales_agg.product_department_number, portefeuille.num_ray, livrable.num_ray, tx_rupture.num_ray, devis.num_ray, tvte.num_ray, asso_pose.department_number,simulation.num_ray, kpi_rdv.NumRayon, pascai_booster_ray.Num_rayon) as num_rayon,
             sales_agg.* except(mois, entity_number, product_department_number),
             portefeuille.mnt_ttc_portefeuille,
             livrable.mnt_ttc_livrable,
             devis.Nb_devis_crees,
             devis.Nb_devis_transf,
             devis.mnt_ttc_devis_transf,
             tx_rupture.nb_rupture,
             tx_rupture.nb_top1,

             tvte.* except(Mois, Num_mag, num_ray),

             associated_project_count,
             installation_worksite_ordered_count,
             simulation.nb_simulation_cree,
             simulation.nb_simulation_integre,
             simulation.nb_simulation_transforme,

             kpi_rdv.nb_rdv_honore_identifie,
             kpi_rdv.nb_rdv_honore_qualif_finalise,

             pascai_booster_ray.Nb_passage_caisse_booster

      from sales_agg
      full join portefeuille 
             on sales_agg.mois = portefeuille.mois 
             and sales_agg.entity_number = portefeuille.num_ett 
             and sales_agg.product_department_number = portefeuille.num_ray 
      full join livrable 
             on sales_agg.mois = livrable.mois 
             and sales_agg.entity_number = livrable.num_ett 
             and sales_agg.product_department_number = livrable.num_ray 
      full join tx_rupture 
             on sales_agg.mois = tx_rupture.mois 
             and sales_agg.entity_number = tx_rupture.num_mag 
             and sales_agg.product_department_number = tx_rupture.num_ray 
      full join devis 
             on sales_agg.mois = devis.mois 
             and sales_agg.entity_number = devis.num_ett 
             and sales_agg.product_department_number = devis.num_ray 
      full join `ddp-bus-commerce-prd-frlm.animation_vente.vente_fait_rayon_typevte` tvte
             on tvte.Mois = sales_agg.mois
             and tvte.Num_mag = sales_agg.entity_number
             and tvte.num_ray = sales_agg.product_department_number 
      full join asso_pose 
             on asso_pose.mois = sales_agg.mois 
             and asso_pose.entity_number = sales_agg.entity_number
             and asso_pose.department_number = sales_agg.product_department_number
      full join simulation 
             on simulation.mois = sales_agg.mois
             and simulation.num_ett = sales_agg.entity_number
             and simulation.num_ray = sales_agg.product_department_number
      full join kpi_rdv
             on kpi_rdv.mois = sales_agg.mois 
             and kpi_rdv.entity_number = sales_agg.entity_number
             and kpi_rdv.NumRayon = sales_agg.product_department_number
      full join pascai_booster_ray
             on pascai_booster_ray.Mois = sales_agg.mois 
             and pascai_booster_ray.Num_mag = sales_agg.entity_number
             and pascai_booster_ray.Num_rayon = sales_agg.product_department_number
        


);

-- declare date_deb date default "2021-01-01";
-- declare date_fin date default date_sub(date_trunc(current_date, month), interval 1 day);

--------- maille magasin 
create or replace table `ddp-bus-commerce-prd-frlm.animation_vente.vente_fait_mag` as (

    with product_ref as (

              select distinct
                     art.num_art                         as product_id, 
                     libart.lib_art                      as product_label,
                     art.num_ray                         as product_department_number,
                     libray.libnumray                    as product_department_label,
                     art.num_sray                        as product_sub_department_number,
                     libsray.libcodsray                  as product_sub_department_label,
                     art.num_typ                         as product_type_number,
                     libtyp.libcodtyp                    as product_type_label,
                     art.num_styp                        as product_sub_type_number,
                     libstyp.libcodstyp                  as product_sub_type_label,
                     art.num_typart                      

              from `dfdp-teradata6y.ProductCatalogLmfr.TA001_RAR_ART` as art

              left join `dfdp-teradata6y.ProductCatalogLmfr.TD001_RAR_LIBART`  as libart
                            on art.num_art = libart.num_art

              left join `dfdp-teradata6y.BaseGeneriqueLmfr.TA001_BAG_LIBRAY`  as libray
                            on art.cod_ray = libray.numray

              left join  `dfdp-teradata6y.BaseGeneriqueLmfr.TA001_BAG_LIBSRAY`  as libsray
                            on  (art.cod_ray =  libsray.numray) 
                            and (art.cod_sray = libsray.codsray)

              left join  `dfdp-teradata6y.BaseGeneriqueLmfr.TA001_BAG_LIBTYP`  as libtyp
                            on  (art.cod_typ =  libtyp.codtyp) 
                            and (art.cod_sray = libtyp.codsray) 
                            and (art.cod_ray =  libtyp.numray)
                     
              left join  `dfdp-teradata6y.BaseGeneriqueLmfr.TA001_BAG_LIBSTYP` as libstyp
                            on  (art.cod_styp = libstyp.codstyp) 
                            and (art.cod_typ =  libstyp.codtyp) 
                            and (art.cod_sray = libstyp.codsray) 
                            and (art.cod_ray =  libstyp.numray)

       ),

       flag_retrait_web AS (
              select * 
              from (
                     select num_ett_tic,
                            num_trn,
                            num_art,
                            num_ligtrn,
                            num_rgrpcli,
                            dat_trn,
                            dat_rtr,
                            top_web_retrait

                     from `lmfr-ddp-dwh-prd.customer_order_teradata.TF_CDE_CMD` sld
              )
              where top_web_retrait = 1   
       ),

       tickets as (
              select 
                     tic.dat_vte as customer_sale_date,
                     tic.num_ett as entity_number,
                     tic.num_rgrpcli as client_id,

                     format_date('%Y%m%d',tic.dat_vte) || '~' || 
                     tic.num_tic || '~' || 
                     tic.heu_tic || '~' || 
                     tic.num_cai || '~' || 
                     tic.num_ett || '~' || 
                     tic.num_typett || '~' || 
                     tic.num_bu as customer_sale_id,

                     tic.num_cde,
                     tic.num_ligcde,
                     art.product_department_number,
                     art.product_department_label,
                     art.product_sub_department_number,
                     art.product_sub_department_label,
                     art.product_type_number,
                     art.product_type_label,
                     tic.mnt_ttcdevbu ca_ttc,
                     tic.mnt_mrg marge,
                     tic.mnt_ht ca_ht,
                     tic.qte_art qte,
                     tic.num_art,

              from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_LIGTICCAI` tic
              left join product_ref art on tic.num_art = art.product_id
                            
              where tic.num_bu = 1
              and tic.num_typett = 1
              and tic.dat_vte between date_deb and date_fin
              and tic.num_typtrn in (48,50,52,47,1,54,49,2,9,8) 
              and num_typart <> 9    
              and num_ett <> 380
       ),

       tickets_retrait as (
              select tickets.*, rweb.dat_trn, rweb.dat_rtr, rweb.top_web_retrait
              from tickets
              left join flag_retrait_web rweb
                     on ( tickets.entity_number = rweb.num_ett_tic
                     and tickets.num_cde = rweb.num_trn
                     and tickets.num_art = rweb.num_art
                     and tickets.num_ligcde = rweb.num_ligtrn + 1 
                     and tickets.client_id = rweb.num_rgrpcli 
              )
              where top_web_retrait is null
       ),
       
       sales_agg as (
              select date_trunc(customer_sale_date, month) Mois,
                     entity_number,
                     sum(ca_ttc) ca_ttc_g,
                     sum(qte) qte_g,
                     sum(if(cpt_tic = 1, nb_articles_positive, -1*nb_articles_negative)) nb_articles_g,
                     sum(cpt_tic) nb_tickets_g
              from (
                     select customer_sale_id,
                            entity_number,
                            customer_sale_date,
                            sum(ca_ttc) ca_ttc,
                            sum(qte) qte,
                            --    count(distinct num_art) nb_articles,
                            count(distinct if(ca_ttc >= 0,num_art, null)) nb_articles_positive,
                            count(distinct if(ca_ttc < 0,num_art, null)) nb_articles_negative,
                            if(sum(ca_ttc) >= 0, 1,-1) cpt_tic

                     from tickets_retrait

                     group by all
              )
              group by all


       ),
       
       pascai_booster as (

       select date_trunc(dat_vte, Month) Mois,
              Num_ett as Num_mag, 
              sum(Nbr_pascai) as Nb_passage_caisse_booster,
              sum(mnt_net) as CA_Net,
              sum(mnt_ht) as CA_Ht,
              sum(qte_vte) as Qte_vendu

       from `lmfr-ddp-dwh-prd.store_sale_agg_seg_teradata.T_AGG_AGGVTE_MAGJOUR`
       where num_bu = 1
       and date(dat_vte) between "2024-05-01" and "2024-05-31"
       group by 1,2

    )

       select coalesce(tvte.mois, transfo.mois) as mois,
              coalesce(tvte.Num_mag, transfo.Num_mag) as num_mag,
              tvte.* except(Mois,Num_mag),
              transfo.* except(Mois, Num_mag),
              sales_agg.* except (Mois, entity_number),

              attr.Nb_rayon_tickets,
              attr.Nb_tickets as Nb_tickets_attr,

              pascai_booster.Nb_passage_caisse_booster
              
       from `ddp-bus-commerce-prd-frlm.animation_vente.vente_fait_mag_typevte` tvte        
       full join  `ddp-bus-commerce-prd-frlm.concept_mag.concept_transfo` transfo
              on tvte.Mois = transfo.mois
              and tvte.Num_mag = transfo.Num_mag
       full join `ddp-bus-commerce-prd-frlm.concept_mag.concept_attractivite` attr
              on attr.mois = tvte.Mois
              and attr.entity_number = tvte.Num_mag
       full join pascai_booster
              on pascai_booster.mois = tvte.Mois
              and pascai_booster.Num_mag = tvte.Num_mag
       full join sales_agg
              on sales_agg.mois = tvte.Mois
              and sales_agg.entity_number = tvte.Num_mag
             


);

--------------------------------------maille sous rayon--------------------------------------------------------
create or replace table `ddp-bus-commerce-prd-frlm.animation_vente.vente_fait_srayon` as (

    with srayon as (
       select distinct 
              numray as Num_ray,
              codsray as Num_sray, initcap(LIBCODSRAY) as Lib_srayon, 
              case when numray in (5,7) then "Projets et Aménagement"
                   when numray in (1,2,3,8) then "Projets Techniques"
                   when numray in (4,10) then "Bricoler"
                   when numray = 9 then "Vivre Dehors"
                   when numray in (6,11,12,13) then "Projets Décoration" 
                   else null end as lib_marche,
              
       from `dfdp-teradata6y.BaseGeneriqueLmfr.TA001_BAG_LIBSRAY` 
    )

       select tvte.*,
              Lib_srayon
       from `ddp-bus-commerce-prd-frlm.animation_vente.vente_fait_sous_rayon_typevte` tvte 
       left join srayon on srayon.num_ray = tvte.num_ray
                        and srayon.num_sray = tvte.num_sray      
       
         
);

----------------------------------------------------------------------------------------------

create or replace table `ddp-bus-commerce-prd-frlm.animation_vente.vente_glossaire` as (
SELECT * FROM `ddp-bus-commerce-prd-frlm.animation_vente.vente_glossaire_sheet`
);

-----------------------------------------------------------------------------------------------


create or replace table `ddp-bus-commerce-prd-frlm.animation_vente.vente_fait_simulation` as (

select date_trunc(date_periode, month) mois, 
       num_ett,
       num_ray,
       typ_sim,
       lib_typ_sim, 
       concat(num_ray, " - ", SimulatorsRegroupement) SimulatorsRegroupement,
       count(distinct if(flag_sim = 0, cod_sim, null)) nb_simulation_cree,
       count(distinct if(flag_sim = 1, cod_sim, null)) nb_simulation_integre,
       count(distinct if(flag_sim = 2, cod_sim, null)) nb_simulation_transforme,
       sum(if(flag_sim = 2, mnt_ca_reel, null)) ca_transforme
from (
    select 0 as flag_sim, top_magasin, dat as date_periode, num_ett, num_ray, typ_sim, lib_typ_sim, cod_sim, 0 as mnt_ca_reel  from `dfdp-frlm-inhab-prod.performance_configurators.SalesSimulationsSaved` 
    union all
    select 1 as flag_sim, top_magasin, dat_intpyx as date_periode, num_ett, num_ray, typ_sim, lib_typ_sim, cod_sim, 0 mnt_ca_reel from `dfdp-frlm-inhab-prod.performance_configurators.SalesSimulationsIntegrated` 
    union all
    select 2 as flag_sim, top_magasin, dat_transfo as date_periode, num_ett, num_ray, typ_sim, lib_typ_sim, cod_sim, mnt_ca_reel from `dfdp-frlm-inhab-prod.performance_configurators.SalesSimulationsTransformed` 
) base
left join `dfdp-frlm-inhab-prod.performance_configurators.ParameterTypSim` Pts on Pts.TypSim = base.typ_sim
where true 
and top_magasin = 1
group by all

)