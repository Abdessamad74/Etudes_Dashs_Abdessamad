declare date_deb date default "2022-01-01";
declare date_fin date default current_date; --date_sub(date_trunc(current_date, month), interval 1 day);

create or replace table `ddp-bus-commerce-prd-frlm.animation_vente.vente_tickets_step` as (


with flag_retrait_web AS (

    select num_ett_tic,
           num_trn,
           num_art,
           num_ligtrn,
           num_rgrpcli,
           dat_trn,
           dat_rtr,
           top_web_retrait
    from `lmfr-ddp-dwh-prd.customer_order_teradata.TF_CDE_CMD` sld
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
          tic.num_bulvte,
          art.num_ray,
          art.num_sray,
          art.num_typ,
          tic.mnt_ttcdevbu ca_ttc,
          tic.mnt_mrg marge,
          tic.mnt_ht ca_ht,
          tic.qte_art qte,
          tic.num_art

    from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_LIGTICCAI` tic
    left join `dfdp-teradata6y.ProductCatalogLmfr.TA001_RAR_ART` as art on tic.num_art = art.num_art
       
    where tic.num_bu = 1
    and   tic.num_typett = 1
    and   tic.dat_vte between date_deb and date_fin
    and   tic.num_typtrn in (48,50,52,47,1,54,49,2,9,8) 
    and   num_typart <> 9    
    and   num_ett <> 380
),

tickets_magasin as (

    select tickets.*, rweb.dat_trn, rweb.dat_rtr, rweb.top_web_retrait
    from tickets
    left join flag_retrait_web rweb
            on ( tickets.entity_number = rweb.num_ett_tic
            and tickets.num_cde = rweb.num_trn
            and tickets.num_art = rweb.num_art
            and tickets.num_ligcde = rweb.num_ligtrn + 1 
            and tickets.client_id = rweb.num_rgrpcli 
            )
    where top_web_retrait is null -- je garde que les tickets en magasin hors retrait web
),

/****************************************************************************************************************************************/
-- Agrégation à la maille article avec calcul des kpis ca, marge, qte ..etc
/****************************************************************************************************************************************/

agg_tickets as (

  select customer_sale_id, 
         customer_sale_id as customer_sale_id_origin, 
         client_id, 
         num_cde, 
         num_bulvte, 
         num_ligcde,
         customer_sale_date, 
         entity_number, 
         num_art,
         num_ray, 
         num_sray, 
         num_typ, 
         sum(ca_ttc) ca_ttc,
         sum(ca_ht) ca_ht,
         sum(marge) marge, 
         sum(qte) qte 

  from tickets_magasin
  group by all

),

/****************************************************************************************************************************************/
-- Intégration de la table des rdv pour ajouter l'information avec ou sans rdv
/****************************************************************************************************************************************/

appointement as (
  select distinct customer_group_number, 
                  appointment_begin_date, 
                  NumRayon
  from `ddp-dtm-inhabitant-prd-frlm.vad.vf_customer_appointment` vfca
  inner join  `ddp-bus-commerce-prd-frlm.animation_vente.rayon_rdv` rrdv
          on vfca.calendar_sub_type = rrdv.Nom_du_RDV
  where customer_group_number is not null
  and   appointment_canceled_flag=0
  and   coalesce(appointment_status,'Venu')='Venu'
),

/****************************************************************************************************************************************/
-- Selection des commandes  pyxis
/****************************************************************************************************************************************/

commandes as (

  select num_trn, 
         num_rgrpcli as num_cli_cmd, 
         dat_trn, 
         dat_rtr,
         num_ligtrn, 
         num_ett as num_ett_cmd, 
         num_art as num_art_cmd,
         max(if(cod_offori is not null, 1,0)) top_simulation_cmd,
         max(if(cod_offori is not null and appointement.customer_group_number is not null, 1,0)) top_appointement_cmd,
         max(if(appointement.customer_group_number is not null, 1,0)) top_only_appointement_cmd

  from `lmfr-ddp-dwh-prd.customer_order_teradata.TF_CDE_CMD` cmd 
  left join appointement on cmd.num_rgrpcli = appointement.customer_group_number
                         and cmd.num_ray = appointement.NumRayon
                         and cmd.dat_trn between appointment_begin_date and date_add(appointment_begin_date, interval 180 day)
  group by all

),

/****************************************************************************************************************************************/
-- Selection des BV  
/****************************************************************************************************************************************/

bulletin_vente as (

  select num_trn as num_bve, 
         num_rgrpcli as num_cli_bve, 
         dat_trn as dat_bve, 
         num_ett as num_ett_bve, 
         num_art as num_art_bve,
         max(if(cod_offori is not null, 1,0)) top_simulation_bve,
         max(if(cod_offori is not null and appointement.customer_group_number is not null, 1,0)) top_appointement_bve,
         max(if(appointement.customer_group_number is not null, 1,0)) top_only_appointement_bve

  from `lmfr-ddp-dwh-prd.sales_operations_teradata.TF_CDE_BVE` bve
  left join appointement on bve.num_rgrpcli = appointement.customer_group_number
                         and bve.num_ray = appointement.NumRayon
                         and bve.dat_trn between appointment_begin_date and date_add(appointment_begin_date, interval 180 day)
  group by all

)

/****************************************************************************************************************************************/
-- Flague des commandes et BV dans les lignes Tickets 
/****************************************************************************************************************************************/


select * 
from agg_tickets
left join commandes on commandes.num_ett_cmd = agg_tickets.entity_number
                    and commandes.num_trn = agg_tickets.num_cde
                    and commandes.num_art_cmd = agg_tickets.num_art
                    and commandes.num_ligtrn + 1 = agg_tickets.num_ligcde
                    and commandes.num_cli_cmd = agg_tickets.client_id
                    
left join bulletin_vente on bulletin_vente.num_bve = agg_tickets.num_bulvte
                        and bulletin_vente.dat_bve = agg_tickets.customer_sale_date
                        and bulletin_vente.num_ett_bve = agg_tickets.entity_number
                        and bulletin_vente.num_cli_bve = agg_tickets.client_id 
                        and bulletin_vente.num_art_bve = agg_tickets.num_art

);

/****************************************************************************************************************************************/
-- Regroupement commandes et LS avec un Update
/****************************************************************************************************************************************/


update `ddp-bus-commerce-prd-frlm.animation_vente.vente_tickets_step` tic

      set customer_sale_id = ifnull(
        ( 
          select min(customer_sale_id)
          from (select * from `ddp-bus-commerce-prd-frlm.animation_vente.vente_tickets_step` where num_cde is not null and ca_ttc >= 0) cmd
          where tic.client_id = cmd.client_id
          and   tic.entity_number = cmd.entity_number
          and  (tic.customer_sale_date = cmd.dat_trn or tic.customer_sale_date = cmd.dat_rtr)
          and   tic.ca_ttc >= 0),

          customer_sale_id
        ),

          customer_sale_date = ifnull(
        ( 
          select min(customer_sale_date)
          from (select * from `ddp-bus-commerce-prd-frlm.animation_vente.vente_tickets_step` where num_cde is not null and ca_ttc >= 0) cmd
          where tic.client_id = cmd.client_id
          and   tic.entity_number = cmd.entity_number
          and  (tic.customer_sale_date = cmd.dat_trn or tic.customer_sale_date = cmd.dat_rtr)
          and   tic.ca_ttc >= 0),

          customer_sale_date
        )

      where true
;

/****************************************************************************************************************************************/
-- Aggrégation au rayon 
/****************************************************************************************************************************************/


create or replace table `ddp-bus-commerce-prd-frlm.animation_vente.vente_fait_rayon_typevte` as (

with tickets_type_vente as (

  select *,
           case when ticket_identifie = 1 and (ticket_cmde = 1 or ticket_bve = 1) and (ticket_cmde_oaa_oav = 1 or ticket_bve_oaa_oav = 1) and (ticket_cmde_oaa_oav_rdv = 1 or ticket_bve_oaa_oav_rdv = 1) then "Vente projet avec rdv"
                when ticket_identifie = 1 and (ticket_cmde = 1 or ticket_bve = 1) and (ticket_cmde_oaa_oav = 1 or ticket_bve_oaa_oav = 1) and ticket_cmde_oaa_oav_rdv = 0 and ticket_bve_oaa_oav_rdv = 0 then "Vente projet sans rdv"
                when ticket_identifie = 1 and (ticket_cmde = 1 or ticket_bve = 1) and ticket_cmde_oaa_oav = 0 and ticket_bve_oaa_oav = 0 then "LS avec acte de vente"
                when ticket_identifie = 1 and ticket_cmde = 0 and ticket_bve = 0 then "LS sans acte de vente"
                when ticket_identifie = 0 and (ticket_cmde = 1 or ticket_bve = 1) then "LS avec acte de vente"
                when ticket_identifie = 0 and ticket_cmde = 0 and ticket_bve = 0 then "LS sans acte de vente"
                else "error"
            end as type_vente


  from (
          select customer_sale_id,
                 customer_sale_date,
                 max(if(client_id is not null, 1,0)) ticket_identifie, 
                 max(if(num_cde is not null, 1,0)) ticket_cmde,
                 max(if(num_bulvte <> 0 or (num_ligcde is not null and num_cde is null), 1,0)) ticket_bve,
                 max(if(num_cde is not null and top_simulation_cmd = 1, 1,0)) ticket_cmde_oaa_oav,
                 max(if((num_bulvte <> 0 or (num_ligcde is not null and num_cde is null)) and top_simulation_bve = 1, 1,0)) ticket_bve_oaa_oav,
                 max(if(num_cde is not null and top_simulation_cmd = 1 and top_appointement_cmd = 1, 1,0)) ticket_cmde_oaa_oav_rdv,
                 max(if((num_bulvte <> 0 or (num_ligcde is not null and num_cde is null)) and top_simulation_bve = 1 and top_appointement_bve = 1, 1,0)) ticket_bve_oaa_oav_rdv

          from `ddp-bus-commerce-prd-frlm.animation_vente.vente_tickets_step`
          group by all
  )
),

agg_vente_tickets as (
  select 
       customer_sale_id,
       customer_sale_date,
       entity_number,
       num_ray,
       if(sum(ca_ttc) >= 0, 1,-1) cpt_tic,
       sum(ca_ttc) ca_ttc,
       sum(ca_ht) ca_ht,
       sum(marge) marge,
       sum(qte) qte,
       count(distinct num_art) nb_articles,

       max(if(num_ray = 2 and num_sray = 30 and num_typ = 10, 1,0)) tic_decoupe,
       sum(if(num_ray = 2 and num_sray = 30 and num_typ = 10, ca_ttc,0)) ca_decoupe

  from `ddp-bus-commerce-prd-frlm.animation_vente.vente_tickets_step`
  group by all
)



select date_trunc(ttv.customer_sale_date,month) Mois,
       entity_number as Num_mag,
       num_ray,
       sum(cpt_tic) nb_tickets,
       sum(if(type_vente = "LS sans acte de vente" , cpt_tic, 0)) nb_tic_sans_acte,
       sum(if(type_vente = "LS avec acte de vente" , cpt_tic, 0)) nb_tic_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv" , cpt_tic, 0)) nb_tic_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv" , cpt_tic, 0)) nb_tic_avec_rdv,
       sum(ca_ttc) ca_ttc,
       sum(if(type_vente = "LS sans acte de vente", ca_ttc, 0)) ca_ttc_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", ca_ttc, 0)) ca_ttc_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", ca_ttc, 0)) ca_ttc_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv", ca_ttc, 0)) ca_ttc_avec_rdv,
       sum(ca_ht) ca_ht,
       sum(if(type_vente = "LS sans acte de vente", ca_ht, 0)) ca_ht_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", ca_ht, 0)) ca_ht_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", ca_ht, 0)) ca_ht_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv", ca_ht, 0)) ca_ht_avec_rdv,
       sum(marge) marge,
       sum(if(type_vente = "LS sans acte de vente", marge, 0)) marge_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", marge, 0)) marge_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", marge, 0)) marge_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv", marge, 0)) marge_avec_rdv,
       sum(qte) qte,
       sum(if(type_vente = "LS sans acte de vente", qte, 0)) qte_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", qte, 0)) qte_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", qte, 0)) qte_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv", qte, 0)) qte_avec_rdv,
       sum(nb_articles * cpt_tic) nb_articles,
       sum(if(type_vente = "LS sans acte de vente", nb_articles * cpt_tic,0)) nb_articles_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", nb_articles * cpt_tic,0)) nb_articles_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", nb_articles * cpt_tic,0)) nb_articles_sans_rdv, 
       sum(if(type_vente = "Vente projet avec rdv", nb_articles * cpt_tic,0)) nb_articles_avec_rdv,

       sum(if(tic_decoupe = 1, cpt_tic,0)) nb_tic_decoupe,
       sum(ca_decoupe) ca_decoupe

       
       
from tickets_type_vente ttv
left join agg_vente_tickets on agg_vente_tickets.customer_sale_id = ttv.customer_sale_id 
                            and agg_vente_tickets.customer_sale_date = ttv.customer_sale_date
group by 1,2,3

);


/****************************************************************************************************************************************/
-- Aggrégation au magasin 
/****************************************************************************************************************************************/

create or replace table `ddp-bus-commerce-prd-frlm.animation_vente.vente_fait_mag_typevte` as (

with tickets_type_vente as (

  select *,
           case when ticket_identifie = 1 and (ticket_cmde = 1 or ticket_bve = 1) and (ticket_cmde_oaa_oav = 1 or ticket_bve_oaa_oav = 1) and (ticket_cmde_oaa_oav_rdv = 1 or ticket_bve_oaa_oav_rdv = 1) then "Vente projet avec rdv"
                when ticket_identifie = 1 and (ticket_cmde = 1 or ticket_bve = 1) and (ticket_cmde_oaa_oav = 1 or ticket_bve_oaa_oav = 1) and ticket_cmde_oaa_oav_rdv = 0 and ticket_bve_oaa_oav_rdv = 0 then "Vente projet sans rdv"
                when ticket_identifie = 1 and (ticket_cmde = 1 or ticket_bve = 1) and ticket_cmde_oaa_oav = 0 and ticket_bve_oaa_oav = 0 then "LS avec acte de vente"
                when ticket_identifie = 1 and ticket_cmde = 0 and ticket_bve = 0 then "LS sans acte de vente"
                when ticket_identifie = 0 and (ticket_cmde = 1 or ticket_bve = 1) then "LS avec acte de vente"
                when ticket_identifie = 0 and ticket_cmde = 0 and ticket_bve = 0 then "LS sans acte de vente"
                else "error"
            end as type_vente


  from (
          select customer_sale_id,
                 customer_sale_date,
                 max(if(client_id is not null, 1,0)) ticket_identifie, 
                 max(if(num_cde is not null, 1,0)) ticket_cmde,
                 max(if(num_bulvte <> 0 or (num_ligcde is not null and num_cde is null), 1,0)) ticket_bve,
                 max(if(num_cde is not null and top_simulation_cmd = 1, 1,0)) ticket_cmde_oaa_oav,
                 max(if((num_bulvte <> 0 or (num_ligcde is not null and num_cde is null)) and top_simulation_bve = 1, 1,0)) ticket_bve_oaa_oav,
                 max(if(num_cde is not null and top_simulation_cmd = 1 and top_appointement_cmd = 1, 1,0)) ticket_cmde_oaa_oav_rdv,
                 max(if((num_bulvte <> 0 or (num_ligcde is not null and num_cde is null)) and top_simulation_bve = 1 and top_appointement_bve = 1, 1,0)) ticket_bve_oaa_oav_rdv

          from `ddp-bus-commerce-prd-frlm.animation_vente.vente_tickets_step`
          group by all
  )
),

agg_vente_tickets as (
  select 
       customer_sale_id,
       customer_sale_date,
       entity_number,
       if(sum(ca_ttc) >= 0, 1,-1) cpt_tic,
       sum(ca_ttc) ca_ttc,
       sum(ca_ht) ca_ht,
       sum(marge) marge,
       sum(qte) qte,
       count(distinct num_art) nb_articles,
       count(distinct num_ray) nb_rayons

  from `ddp-bus-commerce-prd-frlm.animation_vente.vente_tickets_step`
  group by all
)



select date_trunc(ttv.customer_sale_date,month) Mois,
       entity_number as Num_mag,
       sum(cpt_tic) nb_tickets,
       sum(nb_rayons*cpt_tic) nbray_attr_mag,
       sum(if(type_vente = "LS sans acte de vente" , cpt_tic, 0)) nb_tic_sans_acte,
       sum(if(type_vente = "LS avec acte de vente" , cpt_tic, 0)) nb_tic_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv" , cpt_tic, 0)) nb_tic_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv" , cpt_tic, 0)) nb_tic_avec_rdv,

       sum(ca_ttc) ca_ttc,
       sum(if(type_vente = "LS sans acte de vente", ca_ttc, 0)) ca_ttc_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", ca_ttc, 0)) ca_ttc_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", ca_ttc, 0)) ca_ttc_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv", ca_ttc, 0)) ca_ttc_avec_rdv,
       sum(ca_ht) ca_ht,
       sum(if(type_vente = "LS sans acte de vente", ca_ht, 0)) ca_ht_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", ca_ht, 0)) ca_ht_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", ca_ht, 0)) ca_ht_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv", ca_ht, 0)) ca_ht_avec_rdv,
       sum(marge) marge,
       sum(if(type_vente = "LS sans acte de vente", marge, 0)) marge_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", marge, 0)) marge_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", marge, 0)) marge_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv", marge, 0)) marge_avec_rdv,
       sum(qte) qte,
       sum(if(type_vente = "LS sans acte de vente", qte, 0)) qte_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", qte, 0)) qte_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", qte, 0)) qte_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv", qte, 0)) qte_avec_rdv,
       sum(nb_articles * cpt_tic) nb_articles,
       sum(if(type_vente = "LS sans acte de vente", nb_articles * cpt_tic,0)) nb_articles_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", nb_articles * cpt_tic,0)) nb_articles_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", nb_articles * cpt_tic,0)) nb_articles_sans_rdv, 
       sum(if(type_vente = "Vente projet avec rdv", nb_articles * cpt_tic,0)) nb_articles_avec_rdv

       
       
from tickets_type_vente ttv
left join agg_vente_tickets on agg_vente_tickets.customer_sale_id = ttv.customer_sale_id 
                            and agg_vente_tickets.customer_sale_date = ttv.customer_sale_date

group by 1,2

);


/****************************************************************************************************************************************/
-- Aggrégation au sous rayon 
/****************************************************************************************************************************************/


create or replace table `ddp-bus-commerce-prd-frlm.animation_vente.vente_fait_sous_rayon_typevte` as (

with tickets_type_vente as (

  select *,
           case when ticket_identifie = 1 and (ticket_cmde = 1 or ticket_bve = 1) and (ticket_cmde_oaa_oav = 1 or ticket_bve_oaa_oav = 1) and (ticket_cmde_oaa_oav_rdv = 1 or ticket_bve_oaa_oav_rdv = 1) then "Vente projet avec rdv"
                when ticket_identifie = 1 and (ticket_cmde = 1 or ticket_bve = 1) and (ticket_cmde_oaa_oav = 1 or ticket_bve_oaa_oav = 1) and ticket_cmde_oaa_oav_rdv = 0 and ticket_bve_oaa_oav_rdv = 0 then "Vente projet sans rdv"
                when ticket_identifie = 1 and (ticket_cmde = 1 or ticket_bve = 1) and ticket_cmde_oaa_oav = 0 and ticket_bve_oaa_oav = 0 then "LS avec acte de vente"
                when ticket_identifie = 1 and ticket_cmde = 0 and ticket_bve = 0 then "LS sans acte de vente"
                when ticket_identifie = 0 and (ticket_cmde = 1 or ticket_bve = 1) then "LS avec acte de vente"
                when ticket_identifie = 0 and ticket_cmde = 0 and ticket_bve = 0 then "LS sans acte de vente"
                else "error"
            end as type_vente


  from (
          select customer_sale_id,
                 customer_sale_date,
                 max(if(client_id is not null, 1,0)) ticket_identifie, 
                 max(if(num_cde is not null, 1,0)) ticket_cmde,
                 max(if(num_bulvte <> 0 or (num_ligcde is not null and num_cde is null), 1,0)) ticket_bve,
                 max(if(num_cde is not null and top_simulation_cmd = 1, 1,0)) ticket_cmde_oaa_oav,
                 max(if((num_bulvte <> 0 or (num_ligcde is not null and num_cde is null)) and top_simulation_bve = 1, 1,0)) ticket_bve_oaa_oav,
                 max(if(num_cde is not null and top_simulation_cmd = 1 and top_appointement_cmd = 1, 1,0)) ticket_cmde_oaa_oav_rdv,
                 max(if((num_bulvte <> 0 or (num_ligcde is not null and num_cde is null)) and top_simulation_bve = 1 and top_appointement_bve = 1, 1,0)) ticket_bve_oaa_oav_rdv

          from `ddp-bus-commerce-prd-frlm.animation_vente.vente_tickets_step`
          group by all
  )
),

agg_vente_tickets as (
  select 
       customer_sale_id,
       customer_sale_date,
       entity_number,
       num_ray,
       num_sray,
       if(sum(ca_ttc) >= 0, 1,-1) cpt_tic,
       sum(ca_ttc) ca_ttc,
       sum(ca_ht) ca_ht,
       sum(marge) marge,
       sum(qte) qte,
       count(distinct num_art) nb_articles,

       max(if(num_ray = 2 and num_sray = 30 and num_typ = 10, 1,0)) tic_decoupe,
       sum(if(num_ray = 2 and num_sray = 30 and num_typ = 10, ca_ttc,0)) ca_decoupe

  from `ddp-bus-commerce-prd-frlm.animation_vente.vente_tickets_step`
  group by all
)

select date_trunc(ttv.customer_sale_date,month) Mois,
       entity_number as Num_mag,
       num_ray,
       num_sray,
       sum(cpt_tic) nb_tickets,
       sum(if(type_vente = "LS sans acte de vente" , cpt_tic, 0)) nb_tic_sans_acte,
       sum(if(type_vente = "LS avec acte de vente" , cpt_tic, 0)) nb_tic_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv" , cpt_tic, 0)) nb_tic_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv" , cpt_tic, 0)) nb_tic_avec_rdv,
       sum(ca_ttc) ca_ttc,
       sum(if(type_vente = "LS sans acte de vente", ca_ttc, 0)) ca_ttc_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", ca_ttc, 0)) ca_ttc_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", ca_ttc, 0)) ca_ttc_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv", ca_ttc, 0)) ca_ttc_avec_rdv,
       sum(ca_ht) ca_ht,
       sum(if(type_vente = "LS sans acte de vente", ca_ht, 0)) ca_ht_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", ca_ht, 0)) ca_ht_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", ca_ht, 0)) ca_ht_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv", ca_ht, 0)) ca_ht_avec_rdv,
       sum(marge) marge,
       sum(if(type_vente = "LS sans acte de vente", marge, 0)) marge_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", marge, 0)) marge_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", marge, 0)) marge_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv", marge, 0)) marge_avec_rdv,
       sum(qte) qte,
       sum(if(type_vente = "LS sans acte de vente", qte, 0)) qte_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", qte, 0)) qte_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", qte, 0)) qte_sans_rdv,
       sum(if(type_vente = "Vente projet avec rdv", qte, 0)) qte_avec_rdv,
       sum(nb_articles * cpt_tic) nb_articles,
       sum(if(type_vente = "LS sans acte de vente", nb_articles * cpt_tic,0)) nb_articles_sans_acte,
       sum(if(type_vente = "LS avec acte de vente", nb_articles * cpt_tic,0)) nb_articles_avec_acte,
       sum(if(type_vente = "Vente projet sans rdv", nb_articles * cpt_tic,0)) nb_articles_sans_rdv, 
       sum(if(type_vente = "Vente projet avec rdv", nb_articles * cpt_tic,0)) nb_articles_avec_rdv

       
       
from tickets_type_vente ttv
left join agg_vente_tickets on agg_vente_tickets.customer_sale_id = ttv.customer_sale_id 
                            and agg_vente_tickets.customer_sale_date = ttv.customer_sale_date
group by 1,2,3,4

);

