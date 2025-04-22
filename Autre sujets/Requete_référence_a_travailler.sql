create or replace table animation_vente.ref_a_travailler as (

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
    and tic.dat_vte between "2024-01-01" and "2024-12-31"
    and tic.num_typtrn in (48,50,52,47,1,54,49,2,9,8)
    and num_typart <> 9
    -- and num_ett not in (290,291,292,293,294,295,296,297,298)
),


agg_tickets as (
    select * except(customer_sale_id, cpt_tic, ca_ttc, qte),
           sum(cpt_tic) nb_tickets,
           sum(ca_ttc) ca_ttc,
           sum(qte) qte
    from (
          select  customer_sale_id,
                  entity_number,
                  product_department_number,
                  product_department_label,
                  product_sub_department_number,
                  product_sub_department_label,
                  product_type_number,
                  product_type_label,
                  product_sub_type_number,
                  product_sub_type_label,
                  num_art,
                  product_label,
                  cod_gamart,
                  if(sum(ca_ttc) >= 0, 1,-1) cpt_tic,
                  sum(ca_ttc) ca_ttc,
                  sum(qte) qte
          from tickets
          group by all
    )
    where trim(cod_gamart) = "A"
    group by all
    having nb_tickets >= 0
),
nb_tickets_magasins as (
    select entity_number, sum(cpt_tic) nb_tickets_mag
    from (
        select customer_sale_id,
               entity_number,
               if(sum(ca_ttc) >= 0, 1,-1) cpt_tic
        from tickets
        group by all
    )
    group by all
),
---------------------------------------------------------------------------------------------------------------------
-- select count(distinct num_art) from agg_tickets -- 43550 ref gamme A
---------------------------------------------------------------------------------------------------------------------

rang_s_type as (
    select  * except(nb_tickets, nb_tickets_mag), row_number() over(partition by concat(product_department_number,'-',
                                                     product_sub_department_number,'-',
                                                     product_type_number, '-', product_sub_type_number)
                                order by safe_divide(nb_tickets, nb_tickets_mag) asc) rang_stype -- order ascendant
    from (
        select  * except(customer_sale_id, cpt_tic, nb_tickets_mag),
                sum(cpt_tic) nb_tickets,
                max(nb_tickets_mag) nb_tickets_mag
        from (
                select customer_sale_id,
                       entity_number,
                       product_department_number,
                       product_sub_department_number,
                       product_type_number,
                       product_sub_type_number,
                       if(sum(ca_ttc) >= 0, 1,-1) cpt_tic
                from tickets
                group  by all
        )
        left join nb_tickets_magasins using (entity_number)
        group by all
    )
),

-- select * from rang_s_type


-- select * from nb_tickets_magasins

ref_moins_vendu as (
    select * except(nb_tickets, nb_tickets_mag)
    from (
        select *,
               percentile_cont(safe_divide(nb_tickets, nb_tickets_mag), 0.1) over (partition by num_art) quantile
        from (
            select num_art,
                   entity_number,
                   sum(nb_tickets) nb_tickets,
                   max(nb_tickets_mag) nb_tickets_mag
            from agg_tickets
            left join nb_tickets_magasins using(entity_number)
            group by all
        )
    )
    where safe_divide(nb_tickets, nb_tickets_mag) < quantile
),

-- select entity_number, lib_mag, count(distinct num_art) nb_articles
-- from ref_moins_vendu
-- left join `ddp-bus-commerce-prd-frlm.concept_mag.concept_dim_magasins` on num_mag = entity_number
-- group by all

-- OLD
-- ref_moins_vendu as (
--     select * except(nb_tickets)
--     from (
--         select num_art,
--                entity_number,
--                sum(nb_tickets) nb_tickets,
--                percentile_cont(sum(nb_tickets), 0.1) over (partition by num_art) quantile
--         from agg_tickets
--         group by all
--     )
--     where nb_tickets < quantile
-- ),

---------------------------------------------------------------------------------------------------------------------
-- select distinct num_art from ref_moins_vendu -- 601 429 ref -- 105274 unique total
-- gamme A : 38900
---------------------------------------------------------------------------------------------------------------------

nb_mag_ref as (

    select num_art,
           count(distinct entity_number) nb_mag
    from agg_tickets
    group by all
    having nb_mag/143 >= 0.5
),

---------------------------------------------------------------------------------------------------------------------
-- select * from nb_mag_ref -- 55 426 ref total
-- gamme A : 35479
---------------------------------------------------------------------------------------------------------------------

-- pareto_ref_qte as (

--     select num_art,  cumul_ca, ca_total
--     from (
--         select *,
--             sum(ca_ttc) over (order by poids_qte DESC) AS cumul_ca
--         from (
--             select num_art,
--                     sum(qte) qte,
--                     safe_divide(sum(qte), sum(sum(qte)) over()) poids_qte,
--                     sum(ca_ttc) ca_ttc,
--                     sum(sum(ca_ttc)) over() ca_total
--             from agg_tickets
--             group by all
--         )
--     )
--     where safe_divide(cumul_ca, ca_total) <= 0.8

-- ),

pareto_ref_mag as (
    select num_art, count(distinct if(safe_divide(cumul_qte, qte_total_ref) <= 0.81, entity_number, null)) nb_mag
    from (
           select *,
                  sum(qte) over (partition by num_art order by qte desc) as cumul_qte
           from (
                select num_art,
                       entity_number,
                       sum(ca_ttc) ca_ttc,
                       sum(qte) qte,
                       sum(sum(qte)) over(partition by num_art) qte_total_ref
                from agg_tickets
                group by all
            )
    )
    group by all
    having nb_mag <= 30

),

-- select nb_mag, count(distinct num_art) nb_articles
-- from pareto_ref_mag
-- group by all
--
-- select * from pareto_ref_mag -- where num_art = 80131148

---------------------------------------------------------------------------------------------------------------------
-- select * from pareto_ref_qte -- 38273
---------------------------------------------------------------------------------------------------------------------


-- références cible :

ref_cible as (
    select num_art, safe_divide(sum(nb_tickets),sum(nb_tickets_mag)) attr_moyen , safe_divide(sum(ca_ttc),sum(qte)) pvm_moyen
    from (
         select *,
                row_number() over(partition by num_art order by safe_divide(nb_tickets, nb_tickets_mag) desc) rang_ref
         from agg_tickets
         left join nb_tickets_magasins using(entity_number)
    )
    where rang_ref <= 20
    group by all
)

-- select * from ref_cible
select tab.*,
       rang_s_type.rang_stype,
       mag.lib_mag,
       mag.lib_zone,
       safe_divide(ca_ttc, qte) pvm,
       pvm_moyen,
       attr_moyen,
       nb_tickets_mag
from (
        select product_department_number,
               product_department_label,
               product_sub_department_number,
               product_sub_department_label,
               product_type_number,
               product_type_label,
               product_sub_type_number,
               product_sub_type_label,
               num_art,
               product_label,
               cod_gamart,
               entity_number,
               sum(nb_tickets) nb_tickets,
               sum(ca_ttc) ca_ttc,
               sum(qte) qte
        from agg_tickets
        inner join ref_moins_vendu using (entity_number, num_art)
        inner join nb_mag_ref using(num_art) -- les réfs vendus au moins une fois dans plus de 50% des magasins
        -- inner join pareto_ref_qte using (num_art) -- les réf qui gnère 80% du CA
        inner join pareto_ref_mag using(num_art) -- les réfs pourlequelles seulement 10mag au max qui généère 80% de leurs CA
        group  by all
) tab
left join rang_s_type using(entity_number, product_department_number, product_sub_department_number, product_type_number, product_sub_type_number)
left join `ddp-bus-commerce-prd-frlm.concept_mag.concept_dim_magasins` mag on mag.num_mag = tab.entity_number
-- where rang_s_type.rang_stype <= 10 -- -- 93 019 ref unique
left join ref_cible using(num_art)
left join nb_tickets_magasins using (entity_number)
where rang_s_type.rang_stype <= 70
group by all

)
