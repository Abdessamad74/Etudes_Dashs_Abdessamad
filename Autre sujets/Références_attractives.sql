with product_ref as (

    select distinct
            art.num_art                         as product_id,
            initcap(libart.lib_art)             as product_label,
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
          rem.mnt_rem,
          num_typart

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
    and tic.dat_vte between "2024-01-01" and "2024-12-31"
    and tic.num_typtrn in (48,50,52,47,1,54,49,2,9,8)
--     and num_typart = 1 --not in (2,3,7,8,9)
    and num_ett not in (16,116, 380)
),

tickets_magasins as (
       select entity_number,
              sum(cpt_tic) nb_tickets_magasins
       from (
              select customer_sale_id,
                     entity_number,
                     if(sum(ca_ttc) >= 0, 1,-1) cpt_tic
              from tickets
              where num_typart <> 9
              group by all
       )
       group by all
)
-- select * from tickets_magasins
-- select *
-- from (
--        select
--               num_rayon,
--               num_srayon,
--               num_type,
--               num_art,
--               product_label,
--               safe_divide(sum(if(rn=1, ca_ttc, 0)), sum(if(rn=1,qte, 0))) pvm_mag_rang_1,
--               sum(if(rn=1, nb_tickets, 0)) nb_tickets_mag_rang_1,
--               sum(if(rn=1, ca_ttc, 0)) ca_ttc_mag_rang_1,
--               sum(if(rn=1, ca_ht, 0)) ca_ht_mag_rang_1,
--               sum(if(rn=1, marge, 0)) marge,
--               sum(if(rn=1, qte, 0)) qte_mag_rang_1,
--               max(attractivite_ref) attractivite_ref,
--               max(if(rn=1, entity_number, null)) magasin_rang_1,
--               max(nb_tickets_mag_15) nb_tickets_mag_15,
--               max(ca_mag_15) ca_mag_15,
--               max(ca_ht_mag_15) ca_ht_mag_15,
--               max(marge_mag_15) marge_mag_15,
--               max(qte_mag_15) qte_mag_15



--        from (
--               select num_rayon,
--                      num_srayon,
--                      num_type,
--                      num_art,
--                      product_label,
--                      entity_number,
--                      sum(cpt_tic) nb_tickets,
--                      sum(ca_ttc) ca_ttc,
--                      sum(ca_ht) ca_ht,
--                      sum(marge) marge,
--                      sum(qte) qte,
--                      max(sum(if(entity_number = 15, cpt_tic, 0))) over(partition by num_art) nb_tickets_mag_15,
--                      max(sum(if(entity_number = 15, ca_ttc, 0))) over(partition by num_art) ca_mag_15,
--                      max(sum(if(entity_number = 15, ca_ht, 0))) over(partition by num_art) ca_ht_mag_15,
--                      max(sum(if(entity_number = 15, marge, 0))) over(partition by num_art) marge_mag_15,
--                      max(sum(if(entity_number = 15, qte, 0))) over(partition by num_art) qte_mag_15,

--                      max(nb_tickets_magasins) nb_tickets_magasins,
--                      safe_divide(sum(cpt_tic), max(nb_tickets_magasins)) attractivite_ref,
--                      row_number() over(partition by num_art order by safe_divide(sum(cpt_tic), max(nb_tickets_magasins)) desc) as rn

--               from (

--                      select product_department_number num_rayon,
--                             product_sub_department_number num_srayon,
--                             product_type_number num_type,
--                             num_art,
--                             product_label,
--                             customer_sale_id,
--                             entity_number,
--                             if(sum(ca_ttc) >= 0, 1,-1) cpt_tic,
--                             sum(ca_ttc) ca_ttc,
--                             sum(ca_ht) ca_ht,
--                             sum(marge) marge,
--                             sum(qte) qte

--                      from tickets
                     --    where num_typart = 1

--                      group by all

--               )
--               left join tickets_magasins using(entity_number)

--               group by all
--        )

--        group by all
-- )
-- order by attractivite_ref desc
-- limit 5000

------------------------------------------------------------------------------------------------------------------------------------------------
-- -- select num_art, count(distinct entity_number)
-- -- from (
-- select * from (
--               select num_rayon,
--                      num_srayon,
--                      num_type,
--                      num_art,
--                      product_label,
--                      entity_number,
--                      sum(cpt_tic) nb_tickets,
--                      max(nb_tickets_magasins) nb_tickets_magasins,
--                      safe_divide(sum(cpt_tic), max(nb_tickets_magasins)) attractivite_ref,
--                      safe_divide(sum(ca_ttc), sum(qte)) pvm,
--                      row_number() over(partition by num_art order by safe_divide(sum(cpt_tic), max(nb_tickets_magasins)) desc) as rang_ref

--               from (

--                      select num_art,
--                             product_department_number num_rayon,
--                             product_sub_department_number num_srayon,
--                             product_type_number num_type,
--                             product_label,
--                             customer_sale_id,
--                             entity_number,
--                             if(sum(ca_ttc) >= 0, 1,-1) cpt_tic,
--                             sum(ca_ttc) ca_ttc,
--                             sum(ca_ht) ca_ht,
--                             sum(marge) marge,
--                             sum(qte) qte

--                      from tickets
--                      where num_typart = 1
--                      group by all

--               )
--               left join tickets_magasins using(entity_number)

--               group by all
-- )
-- -- where entity_number = 6 -- 142, 180, 117, 6, 63
-- order by attractivite_ref desc
-- limit 10000
-- -- )
-- -- group by all

-----------------------------------------------------------------------------------------------------------------------------------------
select * from (

       select num_art,
              product_label,
              max(if(entity_number <> 15, entity_number, 0)) mag,
              sum( if(entity_number <> 15, cpt_tic, 0)) nb_tickets_mag,
              sum( if(entity_number <> 15, ca_ttc, 0)) ca_ttc_mag,
              sum( if(entity_number <> 15, ca_ht, 0)) ca_ht_mag,
              sum( if(entity_number <> 15, marge, 0)) marge_mag,
              sum( if(entity_number <> 15, qte, 0)) qte_mag,
              max( if(entity_number <> 15, nb_tickets_magasins, null)) nb_tickets_total_mag,
              safe_divide(sum(if(entity_number <> 15, cpt_tic, 0)), max(if(entity_number <> 15, nb_tickets_magasins, 0))) attractivite_ref_mag,
              max(if(entity_number <> 15, rang_ref, 0)) rang_ref_mag,


              sum( if(entity_number = 15, cpt_tic, 0)) nb_tickets_mag_15,
              sum( if(entity_number = 15, ca_ttc, 0)) ca_ttc_mag_15,
              sum( if(entity_number = 15, ca_ht, 0)) ca_ht_mag_15,
              sum( if(entity_number = 15, marge, 0)) marge_mag_15,
              sum( if(entity_number = 15, qte, 0)) qte_mag_15,
              max( if(entity_number = 15, nb_tickets_magasins, null)) nb_tickets_total_mag_15,
              safe_divide(sum(if(entity_number = 15, cpt_tic, 0)), max(if(entity_number = 15, nb_tickets_magasins, 0))) attractivite_ref_mag_15,
              max(if(entity_number = 15, rang_ref, 0)) rang_ref_mag_15

       from (
              select
                     num_art,
                     product_label,
                     entity_number,
                     sum(cpt_tic) cpt_tic,
                     sum(ca_ttc) ca_ttc,
                     sum(ca_ht) ca_ht,
                     sum(marge) marge,
                     sum(qte) qte,
                     max(nb_tickets_magasins) nb_tickets_magasins,
                     row_number() over(partition by num_art order by safe_divide(sum(cpt_tic), max(nb_tickets_magasins)) desc) as rang_ref
              from (

                     select num_art,
                            product_label,
                            customer_sale_id,
                            entity_number,
                            if(sum(ca_ttc) >= 0, 1,-1) cpt_tic,
                            sum(ca_ttc) ca_ttc,
                            sum(ca_ht) ca_ht,
                            sum(marge) marge,
                            sum(qte) qte


                     from tickets
                     where num_typart = 1
                     group by all

              )
              left join tickets_magasins using(entity_number)
              group by all
       )
       where entity_number in (147, 15) -- 290
       group by all

)
order by attractivite_ref_mag desc
limit 1000
