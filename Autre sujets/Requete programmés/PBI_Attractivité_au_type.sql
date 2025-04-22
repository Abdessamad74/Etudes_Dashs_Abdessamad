

-------------------------------------------------- Maille Type à la période -------------------------------------------------------------
declare date_deb date default "2022-01-01";
declare date_fin date default date_sub(date_trunc(current_date, month), interval 1 day);

create or replace table `ddp-bus-commerce-prd-frlm.concept_mag.concept_fait_type` as (
With product_ref as (

    select distinct
            art.num_art                         as product_id,
            libart.lib_art                      as product_label,
            art.num_ray                         as product_department_number,
            concat(art.num_ray, ' - ', libray.libnumray)  as product_department_label,
            art.num_sray                        as product_sub_department_number,
            concat(art.num_ray, ' - ', art.num_sray, ' - ', libsray.libcodsray) as product_sub_department_label,
            art.num_typ                         as product_type_number,
            concat(art.num_ray, ' - ', art.num_sray, ' - ', art.num_typ, ' - ', libtyp.libcodtyp) as product_type_label,
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
          if(tic.qte_art < 1 and tic.qte_art > 0, 1, tic.qte_art) qte,
          tic.num_art,
          num_typtrn

    from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_LIGTICCAI` tic
    left join product_ref art on tic.num_art = art.product_id


    where tic.num_bu = 1
    and tic.num_typett = 1
    and tic.dat_vte between date_deb and date_fin
    and tic.num_typtrn in (48,50,52,47,1,54,49,2,9,8)
    and num_typart <> 9
--     and num_ett not in (290,291,292,293,294,295,296,297,298)
),

maille_magasin as (
    select mois,
           entity_number,
           sum(cpt_tic) Nb_tickets_magasin
    from (
      select customer_sale_id,
             date_trunc(customer_sale_date, month) mois,
             entity_number,
             if(sum(ca_ttc) >= 0, 1,-1) cpt_tic
      from tickets
      group  by all
    )
    group by all
),

maille_rayon as (
    select mois,
           entity_number,
           product_department_number,
           sum(cpt_tic) Nb_tickets_rayon
    from (
      select customer_sale_id,
             date_trunc(customer_sale_date, month) mois,
             entity_number,
             product_department_number,
             if(sum(ca_ttc) >= 0, 1,-1) cpt_tic
      from tickets
      group  by all
    )
    group by all
),

maille_srayon as (
    select mois,
           entity_number,
           product_department_number,
           product_sub_department_number,
           sum(cpt_tic) Nb_tickets_srayon
    from (
      select customer_sale_id,
             date_trunc(customer_sale_date, month) mois,
             entity_number,
             product_department_number,
             product_sub_department_number,
             if(sum(ca_ttc) >= 0, 1,-1) cpt_tic
      from tickets
      group  by all
    )
    group by all
)


select type.mois,
       type.entity_number,
       type.product_department_number,
       initcap(product_department_label) product_department_label,
       type.product_sub_department_number,
       initcap(product_sub_department_label) product_sub_department_label,
       type.product_type_number,
       initcap(product_type_label) product_type_label,
       sum(cpt_tic) nb_tickets_type,
       sum(ca_ttc) ca_type,
       sum(qte) qte_type,
       sum(marge) marge_type,
       sum(ca_ht) ca_ht,
       max(Nb_tickets_srayon) Nb_tickets_srayon,
       max(Nb_tickets_rayon) Nb_tickets_rayon,
       max(Nb_tickets_magasin) Nb_tickets_magasin
from (
    select date_trunc(customer_sale_date, month) mois,
           entity_number,
           customer_sale_id,
           product_department_number,
           product_department_label,
           product_sub_department_number,
           product_sub_department_label,
           product_type_number,
           product_type_label,
           if(sum(ca_ttc) >= 0, 1,-1) cpt_tic,
           sum(ca_ttc) ca_ttc,
           sum(qte) qte,
           sum(marge) marge,
           sum(ca_ht) ca_ht
    from tickets
    group  by all
) type

left join maille_srayon on  maille_srayon.product_department_number = type.product_department_number
                        and maille_srayon.product_sub_department_number = type.product_sub_department_number
                        and maille_srayon.entity_number = type.entity_number
                        and maille_srayon.mois = type.mois

left join maille_rayon on  maille_rayon.product_department_number = type.product_department_number
                       and maille_rayon.entity_number = type.entity_number
                       and maille_rayon.mois = type.mois

left join maille_magasin on  maille_magasin.entity_number = type.entity_number
                         and maille_magasin.mois = type.mois

group by all

-- having nb_tickets_type > 0

)

-------------------------------------------------- Maille Type --------------------------------------------------------------------
declare date_debut date default date_trunc(date_sub(current_date(), interval 12 month), month);
declare date_fin date default date_sub(date_trunc(current_date(), month), interval 1 day);

create or replace table `ddp-bus-commerce-prd-frlm.concept_mag.concept_fait_type` as (
With product_ref as (

    select distinct
            art.num_art                         as product_id,
            libart.lib_art                      as product_label,
            art.num_ray                         as product_department_number,
            concat(art.num_ray, ' - ', libray.libnumray)  as product_department_label,
            art.num_sray                        as product_sub_department_number,
            concat(art.num_ray, ' - ', art.num_sray, ' - ', libsray.libcodsray) as product_sub_department_label,
            art.num_typ                         as product_type_number,
            concat(art.num_ray, ' - ', art.num_sray, ' - ', art.num_typ, ' - ', libtyp.libcodtyp) as product_type_label,
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
          if(tic.qte_art < 1 and tic.qte_art > 0, 1, tic.qte_art) qte,
          tic.num_art,
          num_typtrn

    from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_LIGTICCAI` tic
    left join product_ref art on tic.num_art = art.product_id


    where tic.num_bu = 1
    and tic.num_typett = 1
    and tic.dat_vte between date_debut and date_fin
    and tic.num_typtrn in (48,50,52,47,1,54,49,2,9,8)
    and num_typart <> 9
--     and num_ett not in (290,291,292,293,294,295,296,297,298)
),

maille_magasin as (
    select entity_number,
           sum(cpt_tic) Nb_tickets_magasin
    from (
      select customer_sale_id,
             customer_sale_date,
             entity_number,
             if(sum(ca_ttc) >= 0, 1,-1) cpt_tic
      from tickets
      group  by all
    )
    group by all
),

maille_rayon as (
    select entity_number,
           product_department_number,
           sum(cpt_tic) Nb_tickets_rayon
    from (
      select customer_sale_id,
             customer_sale_date,
             entity_number,
             product_department_number,
             if(sum(ca_ttc) >= 0, 1,-1) cpt_tic
      from tickets
      group  by all
    )
    group by all
),

maille_srayon as (
    select entity_number,
           product_department_number,
           product_sub_department_number,
           sum(cpt_tic) Nb_tickets_srayon
    from (
      select customer_sale_id,
             customer_sale_date,
             entity_number,
             product_department_number,
             product_sub_department_number,
             if(sum(ca_ttc) >= 0, 1,-1) cpt_tic
      from tickets
      group  by all
    )
    group by all
)


select date_debut,
       date_fin,
       type.entity_number,
       type.product_department_number,
       initcap(product_department_label) product_department_label,
       type.product_sub_department_number,
       initcap(product_sub_department_label) product_sub_department_label,
       type.product_type_number,
       initcap(product_type_label) product_type_label,
       sum(cpt_tic) nb_tickets_type,
       sum(ca_ttc) ca_type,
       sum(qte) qte_type,
       sum(marge) marge_type,
       sum(ca_ht) ca_ht,
       max(Nb_tickets_srayon) Nb_tickets_srayon,
       max(Nb_tickets_rayon) Nb_tickets_rayon,
       max(Nb_tickets_magasin) Nb_tickets_magasin
from (
    select entity_number,
           customer_sale_id,
           product_department_number,
           product_department_label,
           product_sub_department_number,
           product_sub_department_label,
           product_type_number,
           product_type_label,
           if(sum(ca_ttc) >= 0, 1,-1) cpt_tic,
           sum(ca_ttc) ca_ttc,
           sum(qte) qte,
           sum(marge) marge,
           sum(ca_ht) ca_ht
    from tickets
    group  by all
) type

left join maille_srayon on  maille_srayon.product_department_number = type.product_department_number
                        and maille_srayon.product_sub_department_number = type.product_sub_department_number
                        and maille_srayon.entity_number = type.entity_number

left join maille_rayon on  maille_rayon.product_department_number = type.product_department_number
                       and maille_rayon.entity_number = type.entity_number

left join maille_magasin on  maille_magasin.entity_number = type.entity_number

group by all

-- having nb_tickets_type > 0

)
