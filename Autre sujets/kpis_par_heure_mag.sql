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
            art.num_typart,
            art.COD_GAMART

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
          safe_cast(
            concat(
                  substr(right(concat('000000', tic.heu_tic), 6), 1, 2), ':', -- Hours
                  substr(right(concat('000000', tic.heu_tic), 6), 3, 2), ':', -- Minutes
                  substr(right(concat('000000', tic.heu_tic), 6), 5, 2) -- Seconds
            ) as time
          ) as heure_ticket,

          safe_cast( substr(right(concat('000000', tic.heu_tic), 6), 1, 2) as int64) as Hour,
          safe_cast( substr(right(concat('000000', tic.heu_tic), 6), 3, 2) as int64) as Minute,
          safe_cast( substr(right(concat('000000', tic.heu_tic), 6), 5, 2) as int64) as Second,

          tic.num_cde,
          tic.num_ligcde,
          art.product_department_number,
          art.product_department_label,
          art.product_sub_department_number,
          art.product_sub_department_label,
          art.product_type_number,
          art.product_type_label,
          art.product_sub_type_number,
          art.product_sub_type_label,
          tic.mnt_ttcdevbu ca_ttc,
          tic.mnt_mrg marge,
          tic.mnt_ht ca_ht,
          tic.qte_art qte,
          tic.num_art,
          art.product_label,
          art.cod_gamart,
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
    and (tic.dat_vte between "2025-03-01" and "2025-03-12") -- or /*(tic.dat_vte between "2024-03-02" and "2024-03-13")*/
    and tic.num_typtrn in (48,50,52,47,1,54,49,2,9,8)
    and num_typart <> 9
    and art.product_department_number <> 1
    -- and num_ett not in (290,291,292,293,294,295,296,297,298)
)

select entity_number,
       customer_sale_date,
       case when minute between 0 and 14  then concat(cast(hour as string), ":", "00")
            when minute between 15 and 29 then concat(cast(hour as string), ":", "15")
            when minute between 30 and 44 then concat(cast(hour as string), ":", "30")
            when minute between 45 and 59 then concat(cast(hour as string), ":", "45")
       end
       as heure_ticket,
       sum(cpt_tic) nb_tickets,
       sum(ca_ttc) ca_ttc,
       sum(qte) qte
from (
        select entity_number,
               customer_sale_date,
               customer_sale_id,
               heure_ticket,
               hour,
               minute,
               if(sum(ca_ttc) >= 0, 1,-1) cpt_tic,
               sum(ca_ttc) ca_ttc,
               sum(qte) qte
        from tickets
        where entity_number in (45, 172)
        and hour between 7 and 9
        group by all
)

group by all
