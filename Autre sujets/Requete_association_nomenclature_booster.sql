declare date_deb date default "2024-01-01";
declare date_fin date default "2024-12-31";

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

      --     tic.num_cde,
      --     tic.num_ligcde,
          art.product_department_number,
          art.product_department_label,
          art.product_sub_department_number,
          art.product_sub_department_label,
          art.product_type_number,
          art.product_type_label,
          sum(tic.mnt_ttcdevbu) ca_ttc,
          sum(tic.mnt_mrg) marge,
          sum(tic.mnt_ht) ca_ht,
          sum(tic.qte_art) qte,
      --     tic.num_art,
      --     rem.mnt_rem

    from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_LIGTICCAI` tic
    left join product_ref art on tic.num_art = art.product_id

--     left join (
--               select num_tic, heu_tic, dat_vte, coalesce(num_artfils, num_art) as num_art, sum(mnt_rem) mnt_rem
--               from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_REMTICCAI`
--               group by 1,2,3,4
--        ) rem
--               on  tic.num_tic = rem.num_tic
--               and tic.heu_tic = rem.heu_tic
--               and tic.dat_vte = rem.dat_vte
--               and tic.num_art = rem.num_art

    where tic.num_bu = 1
    and tic.num_typett = 1
    and tic.dat_vte between date_deb and date_fin
    and tic.num_typtrn in (48,50,52,47,1) --,54,49,2,9,8)
    and num_typart <> 9
    group by all
),

tickets_principal as (
  select *, percentile_cont(nb_tickets_p, 0.5) over() median -- (+ mois) 1 bis saisonnalité
  from (
        select  -- date_trunc(customer_sale_date, month) mois, -- + 1 -saisonnalité
                product_department_number,
                product_sub_department_number,
                product_type_number,
                count(distinct customer_sale_id) nb_tickets_p,
                sum(ca_ttc) ca_ttc,
                sum(qte) qte

        from tickets

        group by all
  )

)


select *
from (
      select *,
            row_number() over(partition by num_rayon_principal, num_srayon_principal, num_type_principal order by nb_combinaisons desc) rn -- (+mois) 2 saisonnalité
      from (
            select    --date_trunc(a.customer_sale_date, month) mois,-- + 3 saisonnalité
                      a.product_department_number as num_rayon_principal,
                      a.product_department_label  as rayon_principal,
                      a.product_sub_department_number as num_srayon_principal,
                      a.product_sub_department_label as srayon_principal,
                      a.product_type_number num_type_principal,
                      a.product_type_label type_principal,

                      b.product_department_number as num_rayon_associe,
                      b.product_department_label as rayon_associe,
                      b.product_sub_department_number as num_srayon_associe,
                      b.product_sub_department_label as srayon_associe,
                      b.product_type_number num_type_associe,
                      b.product_type_label type_associe,

                      count(distinct a.customer_sale_id) nb_combinaisons,
                      sum(b.ca_ttc) ca_ttc_associe,
                      sum(b.marge) marge_associe,
                      max(nb_tickets_p) nb_tickets_principal,
                      max(tp.ca_ttc) ca_ttc_principal,
                      max(tp.qte) qte_principal

            from tickets a
            inner join tickets b on a.customer_sale_id = b.customer_sale_id
                                and a.product_type_label <> b.product_type_label

            left join tickets_principal tp on  a.product_department_number = tp.product_department_number
                                          and a.product_sub_department_number = tp.product_sub_department_number
                                          and a.product_type_number = tp.product_type_number
                                          -- and date_trunc(a.customer_sale_date, month) = tp.mois -- 4 saisonnalité
            -- where nb_tickets_p >= median
            group by all
      )
)
where rn <= 10



-------------------------------------------------------tous les mois pour une combi------------------------------------------------------------------------------

declare date_deb date default "2024-01-01";
declare date_fin date default "2024-12-31";

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
    and tic.dat_vte between date_deb and date_fin
    and tic.num_typtrn in (48,50,52,47,1) --,54,49,2,9,8)
    and num_typart <> 9
),

tickets_principal_an as (

  select distinct product_department_number,
                  product_sub_department_number,
                  product_type_number,
  from (
    select *, percentile_cont(nb_tickets_p_an, 0.5) over() median_an -- 1 bis saisonnalité
    from (
          select  --date_trunc(customer_sale_date, month) mois, -- + 1 -saisonnalité
                  product_department_number,
                  product_sub_department_number,
                  product_type_number,
                  count(distinct customer_sale_id) nb_tickets_p_an,
                  sum(ca_ttc) ca_ttc,
                  sum(qte) qte

          from tickets

          group by all
    )
  )
  where nb_tickets_p_an >= median_an

),

tickets_principal as (
  select *, percentile_cont(nb_tickets_p, 0.5) over(partition by mois) median -- 1 bis saisonnalité
  from (
        select  date_trunc(customer_sale_date, month) mois, -- + 1 -saisonnalité
                product_department_number,
                product_sub_department_number,
                product_type_number,
                count(distinct customer_sale_id) nb_tickets_p,
                sum(ca_ttc) ca_ttc,
                sum(qte) qte

        from tickets

        group by all
  )
  inner join tickets_principal_an using(product_department_number,
                                        product_sub_department_number,
                                        product_type_number)

)


select *
from (
      select *,
            row_number() over(partition by mois, num_rayon_principal, num_srayon_principal, num_type_principal order by nb_combinaisons desc) rn -- 2 saisonnalité
      from (
            select    date_trunc(a.customer_sale_date, month) mois,-- + 3 saisonnalité
                      a.product_department_number as num_rayon_principal,
                      a.product_department_label  as rayon_principal,
                      a.product_sub_department_number as num_srayon_principal,
                      a.product_sub_department_label as srayon_principal,
                      a.product_type_number num_type_principal,
                      a.product_type_label type_principal,

                      b.product_department_number as num_rayon_associe,
                      b.product_department_label as rayon_associe,
                      b.product_sub_department_number as num_srayon_associe,
                      b.product_sub_department_label as srayon_associe,
                      b.product_type_number num_type_associe,
                      b.product_type_label type_associe,

                      count(distinct a.customer_sale_id) nb_combinaisons,
                      max(nb_tickets_p) nb_tickets_principal,
                      max(tp.ca_ttc) ca_ttc_principal,
                      max(tp.qte) qte_principal

            from tickets a
            inner join tickets b on a.customer_sale_id = b.customer_sale_id
                                and a.product_type_label <> b.product_type_label

            inner join tickets_principal tp on  a.product_department_number = tp.product_department_number
                                          and a.product_sub_department_number = tp.product_sub_department_number
                                          and a.product_type_number = tp.product_type_number
                                          and date_trunc(a.customer_sale_date, month) = tp.mois -- 4 saisonnalité
            group by all
      )
)
where rn <= 5
-------------------------------------------------------------------Mag min max taux asso--------------------------------------------------------------------------------------------------------------------

declare date_deb date default "2024-01-01";
declare date_fin date default "2024-12-31";

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

      --     tic.num_cde,
      --     tic.num_ligcde,
          art.product_department_number,
          art.product_department_label,
          art.product_sub_department_number,
          art.product_sub_department_label,
          art.product_type_number,
          art.product_type_label,
          sum(tic.mnt_ttcdevbu) ca_ttc,
          sum(tic.mnt_mrg) marge,
          sum(tic.mnt_ht) ca_ht,
          sum(tic.qte_art) qte,
      --     tic.num_art,
      --     rem.mnt_rem

    from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_LIGTICCAI` tic
    left join product_ref art on tic.num_art = art.product_id

--     left join (
--               select num_tic, heu_tic, dat_vte, coalesce(num_artfils, num_art) as num_art, sum(mnt_rem) mnt_rem
--               from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_REMTICCAI`
--               group by 1,2,3,4
--        ) rem
--               on  tic.num_tic = rem.num_tic
--               and tic.heu_tic = rem.heu_tic
--               and tic.dat_vte = rem.dat_vte
--               and tic.num_art = rem.num_art

    where tic.num_bu = 1
    and tic.num_typett = 1
    and tic.dat_vte between date_deb and date_fin
    and tic.num_typtrn in (48,50,52,47,1) --,54,49,2,9,8)
    and num_typart <> 9
    -- and num_ett <> 380
    group by all
),

tickets_principal as (
  select *, percentile_cont(nb_tickets_p, 0.2) over(partition by entity_number) median -- (+ mois) 1 bis saisonnalité
  from (
        select  -- date_trunc(customer_sale_date, month) mois, -- + 1 -saisonnalité
                entity_number,
                product_department_number,
                product_sub_department_number,
                product_type_number,
                count(distinct customer_sale_id) nb_tickets_p,
                sum(ca_ttc) ca_ttc,
                sum(qte) qte

        from tickets

        group by all
  )

)

select * from(
select   num_rayon_principal,
         rayon_principal,
         num_srayon_principal,
         srayon_principal,
         num_type_principal,
         type_principal,
         num_rayon_associe,
         rayon_associe,
         num_srayon_associe,
         srayon_associe,
         num_type_associe,
         type_associe,
         sum(nb_combinaisons) nb_combinaisons,
         sum(ca_ttc_associe) ca_ttc_associe,
         sum(marge_associe) marge_associe,
         sum(nb_tickets_principal) nb_tickets_principal,
         sum(ca_ttc_principal) ca_ttc_principal,
         sum(qte_principal) qte_principal,
         max(taux_asso) taux_asso_max,
         max(if(rn_taux_max = 1, entity_number, null)) num_mag_taux_asso_max,
         min(taux_asso) taux_asso_min,
         max(if(rn_taux_min = 1, entity_number, null)) num_mag_taux_asso_min,
         row_number() over(partition by num_rayon_principal, num_srayon_principal, num_type_principal order by sum(nb_combinaisons) desc) rn -- (+mois) 2 saisonnalité


from (
      select *,
             safe_divide(nb_combinaisons, nb_tickets_principal) taux_asso,
             row_number() over(partition by num_rayon_principal, num_srayon_principal, num_type_principal, num_rayon_associe, num_srayon_associe, num_type_associe order by safe_divide(nb_combinaisons, nb_tickets_principal) desc) rn_taux_max,
             row_number() over(partition by num_rayon_principal, num_srayon_principal, num_type_principal, num_rayon_associe, num_srayon_associe, num_type_associe order by safe_divide(nb_combinaisons, nb_tickets_principal) asc) rn_taux_min,
      from (
            select    --date_trunc(a.customer_sale_date, month) mois,-- + 3 saisonnalité
                      a.entity_number,
                      a.product_department_number as num_rayon_principal,
                      a.product_department_label  as rayon_principal,
                      a.product_sub_department_number as num_srayon_principal,
                      a.product_sub_department_label as srayon_principal,
                      a.product_type_number num_type_principal,
                      a.product_type_label type_principal,

                      b.product_department_number as num_rayon_associe,
                      b.product_department_label as rayon_associe,
                      b.product_sub_department_number as num_srayon_associe,
                      b.product_sub_department_label as srayon_associe,
                      b.product_type_number num_type_associe,
                      b.product_type_label type_associe,

                      count(distinct a.customer_sale_id) nb_combinaisons,
                      sum(b.ca_ttc) ca_ttc_associe,
                      sum(b.marge) marge_associe,
                      max(nb_tickets_p) nb_tickets_principal,
                      max(tp.ca_ttc) ca_ttc_principal,
                      max(tp.qte) qte_principal

            from tickets a
            inner join tickets b on a.customer_sale_id = b.customer_sale_id
                                and a.product_type_label <> b.product_type_label

            left join tickets_principal tp on  a.product_department_number = tp.product_department_number
                                          and a.product_sub_department_number = tp.product_sub_department_number
                                          and a.product_type_number = tp.product_type_number
                                          and a.entity_number = tp.entity_number
                                          -- and date_trunc(a.customer_sale_date, month) = tp.mois -- 4 saisonnalité
            where nb_tickets_p >= median
            group by all
      )
)

group by all
)
where rn <= 10
--------------------------------------------------------- SR à Type  --------------------------------------------------------------------------------------------------------------------------


declare date_deb date default "2024-01-01";
declare date_fin date default "2024-12-31";

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

      --     tic.num_cde,
      --     tic.num_ligcde,
          art.product_department_number,
          art.product_department_label,
          art.product_sub_department_number,
          art.product_sub_department_label,
          art.product_type_number,
          art.product_type_label,
          sum(tic.mnt_ttcdevbu) ca_ttc,
          sum(tic.mnt_mrg) marge,
          sum(tic.mnt_ht) ca_ht,
          sum(tic.qte_art) qte,
      --     tic.num_art,
      --     rem.mnt_rem

    from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_LIGTICCAI` tic
    left join product_ref art on tic.num_art = art.product_id

--     left join (
--               select num_tic, heu_tic, dat_vte, coalesce(num_artfils, num_art) as num_art, sum(mnt_rem) mnt_rem
--               from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_REMTICCAI`
--               group by 1,2,3,4
--        ) rem
--               on  tic.num_tic = rem.num_tic
--               and tic.heu_tic = rem.heu_tic
--               and tic.dat_vte = rem.dat_vte
--               and tic.num_art = rem.num_art

    where tic.num_bu = 1
    and tic.num_typett = 1
    and tic.dat_vte between date_deb and date_fin
    and tic.num_typtrn in (48,50,52,47,1) --,54,49,2,9,8)
    and num_typart <> 9
    -- and num_ett <> 380
    group by all
),

tickets_principal as (
  select *, percentile_cont(nb_tickets_p, 0.2) over(partition by entity_number) median -- (+ mois) 1 bis saisonnalité
  from (
        select  -- date_trunc(customer_sale_date, month) mois, -- + 1 -saisonnalité
                entity_number,
                product_department_number,
                product_sub_department_number,
                -- product_type_number,
                count(distinct customer_sale_id) nb_tickets_p,
                sum(ca_ttc) ca_ttc,
                sum(qte) qte

        from tickets

        group by all
  )

)

select * from(
select   num_rayon_principal,
         rayon_principal,
         num_srayon_principal,
         srayon_principal,
        --  num_type_principal,
        --  type_principal,
         num_rayon_associe,
         rayon_associe,
         num_srayon_associe,
         srayon_associe,
         num_type_associe,
         type_associe,
         sum(nb_combinaisons) nb_combinaisons,
         sum(ca_ttc_associe) ca_ttc_associe,
         sum(marge_associe) marge_associe,
         sum(nb_tickets_principal) nb_tickets_principal,
         sum(ca_ttc_principal) ca_ttc_principal,
         sum(qte_principal) qte_principal,
         max(taux_asso) taux_asso_max,
         max(if(rn_taux_max = 1, entity_number, null)) num_mag_taux_asso_max,
         min(taux_asso) taux_asso_min,
         max(if(rn_taux_min = 1, entity_number, null)) num_mag_taux_asso_min,
         row_number() over(partition by num_rayon_principal, num_srayon_principal order by sum(nb_combinaisons) desc) rn -- (+mois) 2 saisonnalité


from (
      select *,
             safe_divide(nb_combinaisons, nb_tickets_principal) taux_asso,
             row_number() over(partition by num_rayon_principal, num_srayon_principal, num_rayon_associe, num_srayon_associe, num_type_associe order by safe_divide(nb_combinaisons, nb_tickets_principal) desc) rn_taux_max,
             row_number() over(partition by num_rayon_principal, num_srayon_principal, num_rayon_associe, num_srayon_associe, num_type_associe order by safe_divide(nb_combinaisons, nb_tickets_principal) asc) rn_taux_min,
      from (
            select    --date_trunc(a.customer_sale_date, month) mois,-- + 3 saisonnalité
                      a.entity_number,
                      a.product_department_number as num_rayon_principal,
                      a.product_department_label  as rayon_principal,
                      a.product_sub_department_number as num_srayon_principal,
                      a.product_sub_department_label as srayon_principal,
                    --   a.product_type_number num_type_principal,
                    --   a.product_type_label type_principal,

                      b.product_department_number as num_rayon_associe,
                      b.product_department_label as rayon_associe,
                      b.product_sub_department_number as num_srayon_associe,
                      b.product_sub_department_label as srayon_associe,
                      b.product_type_number num_type_associe,
                      b.product_type_label type_associe,

                      count(distinct a.customer_sale_id) nb_combinaisons,
                      sum(b.ca_ttc) ca_ttc_associe,
                      sum(b.marge) marge_associe,
                      max(nb_tickets_p) nb_tickets_principal,
                      max(tp.ca_ttc) ca_ttc_principal,
                      max(tp.qte) qte_principal

            from tickets a
            inner join tickets b on a.customer_sale_id = b.customer_sale_id
                                and a.product_type_label <> b.product_type_label
                                and a.product_sub_department_number <> b.product_sub_department_number

            left join tickets_principal tp on  a.product_department_number = tp.product_department_number
                                          and a.product_sub_department_number = tp.product_sub_department_number
                                        --   and a.product_type_number = tp.product_type_number
                                          and a.entity_number = tp.entity_number
                                          -- and date_trunc(a.customer_sale_date, month) = tp.mois -- 4 saisonnalité
            where nb_tickets_p >= median
            group by all
      )
)

group by all
)
where rn <= 10

---------------------------------------------------------- SR à SR --------------------------------------------------------------------------------------------------




declare date_deb date default "2024-01-01";
declare date_fin date default "2024-12-31";

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

      --     tic.num_cde,
      --     tic.num_ligcde,
          art.product_department_number,
          art.product_department_label,
          art.product_sub_department_number,
          art.product_sub_department_label,
          art.product_type_number,
          art.product_type_label,
          sum(tic.mnt_ttcdevbu) ca_ttc,
          sum(tic.mnt_mrg) marge,
          sum(tic.mnt_ht) ca_ht,
          sum(tic.qte_art) qte,
      --     tic.num_art,
      --     rem.mnt_rem

    from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_LIGTICCAI` tic
    left join product_ref art on tic.num_art = art.product_id

--     left join (
--               select num_tic, heu_tic, dat_vte, coalesce(num_artfils, num_art) as num_art, sum(mnt_rem) mnt_rem
--               from `lmfr-ddp-dwh-prd.store_sale_teradata.TF001_VTE_REMTICCAI`
--               group by 1,2,3,4
--        ) rem
--               on  tic.num_tic = rem.num_tic
--               and tic.heu_tic = rem.heu_tic
--               and tic.dat_vte = rem.dat_vte
--               and tic.num_art = rem.num_art

    where tic.num_bu = 1
    and tic.num_typett = 1
    and tic.dat_vte between date_deb and date_fin
    and tic.num_typtrn in (48,50,52,47,1) --,54,49,2,9,8)
    and num_typart <> 9
    -- and num_ett <> 380
    group by all
),

tickets_principal as (
  select *, percentile_cont(nb_tickets_p, 0.2) over(partition by entity_number) median -- (+ mois) 1 bis saisonnalité
  from (
        select  -- date_trunc(customer_sale_date, month) mois, -- + 1 -saisonnalité
                entity_number,
                product_department_number,
                product_sub_department_number,
                -- product_type_number,
                count(distinct customer_sale_id) nb_tickets_p,
                sum(ca_ttc) ca_ttc,
                sum(qte) qte

        from tickets

        group by all
  )

)

select * from(
select   num_rayon_principal,
         rayon_principal,
         num_srayon_principal,
         srayon_principal,
        --  num_type_principal,
        --  type_principal,
         num_rayon_associe,
         rayon_associe,
         num_srayon_associe,
         srayon_associe,
        --  num_type_associe,
        --  type_associe,
         sum(nb_combinaisons) nb_combinaisons,
         sum(ca_ttc_associe) ca_ttc_associe,
         sum(marge_associe) marge_associe,
         sum(nb_tickets_principal) nb_tickets_principal,
         sum(ca_ttc_principal) ca_ttc_principal,
         sum(qte_principal) qte_principal,
         max(taux_asso) taux_asso_max,
         max(if(rn_taux_max = 1, entity_number, null)) num_mag_taux_asso_max,
         min(taux_asso) taux_asso_min,
         max(if(rn_taux_min = 1, entity_number, null)) num_mag_taux_asso_min,
         row_number() over(partition by num_rayon_principal, num_srayon_principal order by sum(nb_combinaisons) desc) rn -- (+mois) 2 saisonnalité


from (
      select *,
             safe_divide(nb_combinaisons, nb_tickets_principal) taux_asso,
             row_number() over(partition by num_rayon_principal, num_srayon_principal, num_rayon_associe, num_srayon_associe order by safe_divide(nb_combinaisons, nb_tickets_principal) desc) rn_taux_max,
             row_number() over(partition by num_rayon_principal, num_srayon_principal, num_rayon_associe, num_srayon_associe order by safe_divide(nb_combinaisons, nb_tickets_principal) asc) rn_taux_min,
      from (
            select    --date_trunc(a.customer_sale_date, month) mois,-- + 3 saisonnalité
                      a.entity_number,
                      a.product_department_number as num_rayon_principal,
                      a.product_department_label  as rayon_principal,
                      a.product_sub_department_number as num_srayon_principal,
                      a.product_sub_department_label as srayon_principal,
                    --   a.product_type_number num_type_principal,
                    --   a.product_type_label type_principal,

                      b.product_department_number as num_rayon_associe,
                      b.product_department_label as rayon_associe,
                      b.product_sub_department_number as num_srayon_associe,
                      b.product_sub_department_label as srayon_associe,
                    --   b.product_type_number num_type_associe,
                    --   b.product_type_label type_associe,

                      count(distinct a.customer_sale_id) nb_combinaisons,
                      sum(b.ca_ttc) ca_ttc_associe,
                      sum(b.marge) marge_associe,
                      max(nb_tickets_p) nb_tickets_principal,
                      max(tp.ca_ttc) ca_ttc_principal,
                      max(tp.qte) qte_principal

            from tickets a
            inner join tickets b on a.customer_sale_id = b.customer_sale_id
                                and a.product_type_label <> b.product_type_label
                                and a.product_sub_department_label <> b.product_sub_department_label

            left join tickets_principal tp on  a.product_department_number = tp.product_department_number
                                          and a.product_sub_department_number = tp.product_sub_department_number
                                        --   and a.product_type_number = tp.product_type_number
                                          and a.entity_number = tp.entity_number
                                          -- and date_trunc(a.customer_sale_date, month) = tp.mois -- 4 saisonnalité
            where nb_tickets_p >= median
            group by all
      )
)

group by all
)
where rn <= 10
