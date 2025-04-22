31declare date_deb date default "2025-03-01";
declare date_fin date default "2025-03-30";
declare date_deb_n1 date;
declare date_fin_n1 date;
set date_deb_n1 = (select distinct dat_decalcalen from `lmfr-ddp-dwh-prd.store_sale_teradata.TD001_VTE_DATECOMP` where dat_ref = date_deb);
set date_fin_n1 = (select distinct dat_decalcalen from `lmfr-ddp-dwh-prd.store_sale_teradata.TD001_VTE_DATECOMP` where dat_ref = date_fin);

with product_ref as (

    select distinct
            art.num_art                         as product_id, 
            libart.lib_art                      as product_label,
            art.num_ray                         as product_department_number,
            concat(num_ray, " - ", initcap(libray.libnumray))           as product_department_label,
            art.num_sray                        as product_sub_department_number,
            concat(num_ray, "-", num_sray, " - ", initcap(libsray.libcodsray))    as product_sub_department_label,
            art.num_typ                         as product_type_number,
            concat(num_ray, "-", num_sray, " - ",  num_typ, " - ", initcap( libtyp.libcodtyp ))   as product_type_label,
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

    select num_ett_tic,
            num_trn,
            num_art,
            num_ligtrn,
            num_rgrpcli,
            dat_trn,
            dat_rtr,
            top_web_retrait as top_retraitweb

    from `lmfr-ddp-dwh-prd.customer_order_teradata.TF_CDE_CMD` sld
),

date_comparable as (

    select 
    dat_ref date_n,
    dat_decalcalen date_n_1
    from `lmfr-ddp-dwh-prd.store_sale_teradata.TD001_VTE_DATECOMP` as d
    where dat_ref between date_deb and date_fin
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
          rem.mnt_rem

    from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_LIGTICCAI` tic
    left join product_ref art on tic.num_art = art.product_id

    left join (
              select num_tic, heu_tic, dat_vte, coalesce(num_artfils, num_art) as num_art, sum(mnt_rem) mnt_rem
              from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_REMTICCAI`
              group by 1,2,3,4
       ) rem  
              on  tic.num_tic = rem.num_tic 
              and tic.heu_tic = rem.heu_tic 
              and tic.dat_vte = rem.dat_vte 
              and tic.num_art = rem.num_art
              
    where tic.num_bu = 1
    and tic.num_typett = 1
    and (  (tic.dat_vte between date_deb and date_fin) 
        or (tic.dat_vte between date_deb_n1 and date_fin_n1)) 
    and tic.num_typtrn in (48,50,52,47,1,54,49,2,9,8) 
    and num_typart <> 9    
),

tickets_retrait as (
  select tickets.*, rweb.dat_trn, rweb.dat_rtr, rweb.top_retraitweb
  from tickets
  left join flag_retrait_web rweb
         on ( tickets.entity_number = rweb.num_ett_tic
         and tickets.num_cde = rweb.num_trn
         and tickets.num_art = rweb.num_art
         and tickets.num_ligcde = rweb.num_ligtrn + 1 
         and tickets.client_id = rweb.num_rgrpcli 
         )
),

tickets_magasin as (
    select *, "Magasin" as canal_achat
    from tickets_retrait
    where entity_number <> 380 and (top_retraitweb is null or top_retraitweb = 0)
),


tic_n as (

    select  customer_sale_id,
            entity_number,
            date_n as customer_sale_date,
            date_n_1,
            if(sum(ca_ttc) >= 0, 1,-1) cpt_tic,
            sum(ca_ttc) ca_ttc,
            sum(ca_ht) ca_ht,
            sum(marge) marge,
            sum(qte) qte,
            count(distinct if(ca_ttc >= 0,num_art, null)) nb_articles_positive,
            count(distinct if(ca_ttc < 0,num_art, null)) nb_articles_negative,
            count(distinct product_department_number) nb_rayons

    from date_comparable
    left join tickets_magasin tic on tic.customer_sale_date = date_comparable.date_n
    where customer_sale_date between date_deb and date_fin
    group by all
),

tic_n_1 as (

    select  customer_sale_id,
            entity_number,
            customer_sale_date,
            if(sum(ca_ttc) >= 0, 1,-1) cpt_tic,
            sum(ca_ttc) ca_ttc,
            sum(ca_ht) ca_ht,
            sum(marge) marge,
            sum(qte) qte,
            count(distinct if(ca_ttc >= 0,num_art, null)) nb_articles_positive,
            count(distinct if(ca_ttc < 0,num_art, null)) nb_articles_negative,
            count(distinct product_department_number) nb_rayons

    from date_comparable
    left join tickets_magasin tic on tic.customer_sale_date = date_comparable.date_n_1
    where customer_sale_date between date_deb_n1 and date_fin_n1
    group by all
),

kpi_n as (
    select entity_number,
           lib_mag,
           lib_zone,
           sum(cpt_tic) nb_tickets,
           sum(if(cpt_tic = 1, nb_articles_positive, -1*nb_articles_negative)) nb_ref,
           sum(ca_ttc) ca_ttc,
           sum(qte) qte,
           sum(nb_rayons) nb_rayons_tickets

    from tic_n
    left join `ddp-bus-commerce-prd-frlm.concept_mag.concept_dim_magasins` mag 
        on mag.num_mag = tic_n.entity_number
    group by all
),

kpi_n_1 as (
    select entity_number,
           sum(cpt_tic) Nb_tickets_n_1,
           sum(if(cpt_tic = 1, nb_articles_positive, -1*nb_articles_negative)) nb_ref_n_1,
           sum(ca_ttc) ca_ttc_n_1,
           sum(qte) qte_n_1,
           sum(nb_rayons) nb_rayons_tickets_n_1

    from tic_n_1
    group by all
)

select *
from kpi_n
left join kpi_n_1 using (entity_number)

