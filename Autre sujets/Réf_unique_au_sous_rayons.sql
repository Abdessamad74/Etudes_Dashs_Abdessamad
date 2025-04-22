declare date_deb date default 2024-01-01;
declare date_fin date default 2025-01-31;

create or replace table `ddp-bus-commerce-prd-frlm.animation_vente.decomposition_vente_srayon_month` as (


------------- Maille sous rayon------------------------------------------------------------------------------------------------------------------------------------------
With product_ref as (

    select distinct
            art.num_art                         as product_id,
            libart.lib_art                      as product_label,
            art.num_ray                         as product_department_number,
            concat(art.num_ray, ' - ', initcap(libray.libnumray))  as product_department_label,
            art.num_sray                        as product_sub_department_number,
            concat(art.num_ray, ' - ', art.num_sray, ' - ', initcap(libsray.libcodsray)) as product_sub_department_label,
            art.num_typ                         as product_type_number,
            concat(art.num_ray, ' - ', art.num_sray, ' - ', art.num_typ, ' - ', initcap(libtyp.libcodtyp)) as product_type_label,
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
            top_web_retrait

    from `lmfr-ddp-dwh-prd.customer_order_teradata.TF_CDE_CMD` sld

),

tickets as (
    select
          tic.dat_vte as customer_sale_date,
          tic.num_ett as entity_number,
          tic.num_rgrpcli as client_id,

          format_date('%Y%m%d',tic.dat_vte)  '~'
          tic.num_tic  '~'
          tic.heu_tic  '~'
          tic.num_cai  '~'
          tic.num_ett  '~'
          tic.num_typett  '~'
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
    and tic.dat_vte between date_deb and date_fin
    and tic.num_typtrn in (48,50,52,47,1,54,49,2,9,8)
    and num_typart  9
    -- and num_ett  230
),

tickets_retrait as (
  select tickets., rweb.dat_trn, rweb.dat_rtr, rweb.top_web_retrait
  from tickets
  left join flag_retrait_web rweb
         on ( tickets.entity_number = rweb.num_ett_tic
         and tickets.num_cde = rweb.num_trn
         and tickets.num_art = rweb.num_art
         and tickets.num_ligcde = rweb.num_ligtrn + 1
         and tickets.client_id = rweb.num_rgrpcli
         )
)

select date_trunc(customer_sale_date, month) Mois,
       entity_number,
       product_department_number as num_rayon,
       product_department_label as label_rayon,
       product_sub_department_number as num_srayon,
       product_sub_department_label as label_srayon,
       lib_mag,
       lib_zone,
       if(top_frch = 1,Oui, Non) top_frch,
       canal_achat,
       sum(cpt_tic) Nb_tickets,
       sum(ca_ttc) ca_ttc,
       sum(qte) qte,
       sum(if(cpt_tic = 1, nb_articles_positive, -1nb_articles_negative)) nb_articles,
       sum(remise) remise
from(
        select customer_sale_id,
               entity_number,
               product_department_number,
               product_department_label,
               product_sub_department_number,
               product_sub_department_label,
               case when entity_number  380 and (top_web_retrait is null or top_web_retrait = 0) then Magasin
                    when entity_number = 380 then Web livré
                    when top_web_retrait = 1 then Retrait web
                    else null end
               as canal_achat,
               customer_sale_date,
               sum(ca_ttc) ca_ttc,
               sum(qte) qte,
               count(distinct if(ca_ttc = 0,num_art, null)) nb_articles_positive,
               count(distinct if(ca_ttc  0,num_art, null)) nb_articles_negative,
               if(sum(ca_ttc) = 0, 1,-1) cpt_tic,
               sum(mnt_rem) remise

        from tickets_retrait

        group by all
) tic
left join `ddp-bus-commerce-prd-frlm.concept_mag.concept_dim_magasins` mag
       on mag.num_mag = tic.entity_number
group by all

);

-----------------------------------------------------------------------------


----------------------------------------------------------------------------------------------------

declare date_deb date default 2024-01-01;
declare date_fin date default 2024-12-31 ;

------------- Maille srayon  ---------------------------------------------------------------------------------------------------------------------

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

tickets as (
    select
          tic.dat_vte as customer_sale_date,
          tic.num_ett as entity_number,
          tic.num_rgrpcli as client_id,

          format_date('%Y%m%d',tic.dat_vte)  '~'
          tic.num_tic  '~'
          tic.heu_tic  '~'
          tic.num_cai  '~'
          tic.num_ett  '~'
          tic.num_typett  '~'
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
    and tic.dat_vte between date_deb and date_fin
    and tic.num_typtrn in (48,50,52,47,1,54,49,2,9,8)
    and num_typart  9
),

tickets_retrait as (
  select , case when entity_number  380 and (top_retraitweb is null or top_retraitweb = 0) then Magasin
                 when entity_number = 380 then Web livré
                 when top_retraitweb = 1 then Retrait web
            else null end
            as canal_achat
  from (
        select tickets., rweb.dat_trn, rweb.dat_rtr, rweb.top_retraitweb
        from tickets
        left join flag_retrait_web rweb
                on ( tickets.entity_number = rweb.num_ett_tic
                and tickets.num_cde = rweb.num_trn
                and tickets.num_art = rweb.num_art
                and tickets.num_ligcde = rweb.num_ligtrn + 1
                and tickets.client_id = rweb.num_rgrpcli
                )
  )
),

date_comparable as (
    select
    dat_ref date_n,
    dat_decalcalen date_n_1
    from `lmfr-ddp-dwh-prd.store_sale_teradata.TD001_VTE_DATECOMP` as d
    where dat_ref between date_deb and date_fin
),

rayon as (
       select distinct sray.numray as product_department_number,
                       concat(sray.numray, - , initcap(LIBNUMRAY)) as product_department_label,
                       sray.codsray as product_sub_department_number,
                       concat(sray.numray,  - , sray.codsray,  - , initcap(LIBCODSRAY)) as product_sub_department_label,


       from `dfdp-teradata6y.BaseGeneriqueLmfr.TA001_BAG_LIBSRAY` sray
       left join `dfdp-teradata6y.BaseGeneriqueLmfr.TA001_BAG_LIBRAY` ray on  sray.NNUMRAY = ray.NUMRAY
    ),

tic as (

    select customer_sale_id,
           num_mag as entity_number,
           if(lib_mag is null, 230 - Relais MONTLUCON, lib_mag) lib_mag,
           lib_zone,
           if(top_frch = 1,Oui, Non) top_frch,
           canal.canal_achat,
           rayon.product_department_number,
           rayon.product_department_label,
           rayon.product_sub_department_number,
           rayon.product_sub_department_label,
           date_n as customer_sale_date,
           date_n_1,
           sum(ca_ttc) ca_ttc,
           sum(qte) qte,
           count(distinct if(ca_ttc = 0,num_art, null)) nb_articles_positive,
           count(distinct if(ca_ttc  0,num_art, null)) nb_articles_negative,
           if(sum(ca_ttc) = 0, 1,-1) cpt_tic,
           sum(mnt_rem) remise

    from date_comparable
    left join `ddp-bus-commerce-prd-frlm.concept_mag.concept_dim_magasins` mag on 1=1
    left join rayon on 1=1
    left join (select Magasin as canal_achat union all select Web livré union all select Retrait web ) canal on 1=1
    left join tickets_retrait on  tickets_retrait.customer_sale_date = date_comparable.date_n
                              and tickets_retrait.entity_number = mag.num_mag
                              and tickets_retrait.canal_achat = canal.canal_achat
                              and tickets_retrait.product_department_number = rayon.product_department_number
                              and tickets_retrait.product_sub_department_number = rayon.product_sub_department_number

    group by all

),

data_n as (
    select customer_sale_date,
           date_n_1,
           entity_number,
           lib_mag,
           lib_zone,
           top_frch,
           canal_achat,
           product_department_number as num_rayon,
           product_department_label as label_rayon,
           product_sub_department_number as num_srayon,
           product_sub_department_label as label_srayon,
           sum(cpt_tic) Nb_tickets,
           sum(ca_ttc) ca_ttc,
           sum(qte) qte,
           sum(if(cpt_tic = 1, nb_articles_positive, -1nb_articles_negative)) nb_articles,
           sum(remise) remise
    from tic
    group by all
)

select date_trunc(customer_sale_date, month) Mois,
       entity_number,
       lib_mag,
       lib_zone,
       top_frch,
       canal_achat,
       num_rayon,
       label_rayon,
       num_srayon,
       label_srayon,
       sum(Nb_tickets) Nb_tickets,
       sum(ca_ttc) ca_ttc,
       sum(qte) qte,
       sum(nb_articles) nb_articles,
       sum(remise) remise
from (

    select data_n.,
            sum(tic.cpt_tic) Nb_tickets_n_1,
            sum(tic.ca_ttc) ca_ttc_n_1,
            sum(tic.qte) qte_n_1,
            sum(if(tic.cpt_tic = 1, tic.nb_articles_positive, -1tic.nb_articles_negative)) nb_articles_n_1,
            sum(tic.remise) remise_n_1
    from data_n
    left join tic on  tic.customer_sale_date = data_n.date_n_1
                and tic.entity_number = data_n.entity_number
                and tic.canal_achat = data_n.canal_achat
                and tic.product_department_number = data_n.num_rayon
    group by all
)

group by all
