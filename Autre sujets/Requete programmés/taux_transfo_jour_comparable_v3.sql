declare date_debut date default "2022-01-01";
declare date_fin date default current_date ();

create or replace table `ddp-bus-commerce-prd-frlm.concept_mag.taux_transfo_jour_comparable_v3` as (

with date_comparable as (
    select dat_ref date_n,
           dat_decalcalen date_n_1
    from `lmfr-ddp-dwh-prd.store_sale_teradata.TD001_VTE_DATECOMP` as d
    where dat_ref between date_debut and date_fin
),
/* ---------------------Base clients ------------------------------------------------------------------*/
clients_n as (
    select date(dat_vte) dat_vte,
           date_n_1,
           num_ett,
           coalesce(sum(nbr_pascai),0) as Nb_clients
    from `lmfr-ddp-dwh-prd.store_sale_agg_seg_teradata.T_AGG_AGGVTE_MAGJOUR` mj
    left join date_comparable on date_comparable.date_n = mj.dat_vte
    where num_bu = 1
    and dat_vte between date_debut and date_fin
    group by all
), -- table source des clients

clients as (
    select a.*,
           coalesce(sum(b.Nb_clients), 0) Nb_clients_n_1
    from clients_n a
    left join clients_n b on a.date_n_1 = b.dat_vte
                        and a.num_ett = b.num_ett
    group by all
    --  if(extract(month from a.date_n_1) = 5 and extract(day from a.date_n_1) = 1, date_sub(a.date_n_1, interval 7 day), a.date_n_1) = b.dat_vte
),
/* ---------------------Base Visiteurs ------------------------------------------------------------------*/

tmp as (
    select safe_cast(store_id as int64) store_id,
       --     safe_cast(idr.entranceID as int64) zone_id,
       --     safe_cast(zone_name as string) zone_label,
           date,
           time,
           parse_date('%Y%m%d', date) as count_date,
           safe_cast(substr(time,1,4) as int64) as count_time,
           enters,
           exits

    from `lmfr-ddp-ods-prd.shoppertrak.shoppertrak_quotidien_v1` r
--     left join `lmfr-ddp-dwh-prd.store_management.shoppertrak_id_reprise` idr
--            on idr.storeid = safe_cast(r.store_id as integer)
--            and idr.entranceName = r.zone_name

    where lower(zone_name) not like '%ssa%'
),

base_visiteurs as (
    select count_date,
           entity_number,
           sum(enters_count) enters_count
    from (

        select store_id as entity_number,
              --  zone_id,
              --  zone_label,
               count_date,
               count_time,
              --  heu.begin_hour as begin_time,
              --  heu.end_hour as end_time,
               safe_cast(coalesce(tmp.enters, 0) as numeric) as enters_count,
               safe_cast(coalesce(tmp.exits, 0) as numeric) as exits_count
        from tmp
       --  inner join `ddp-dtm-perf-prd-frlm.data_for_analysis.td_hours` heu
       --          on tmp.count_time >= heu.begin_hour
       --          and tmp.count_time <= heu.end_hour
    )

    where count_date  between date_debut and date_fin

    group by all
),

visiteurs_n as (
    select date(count_date) count_date,
           date_n_1,
           entity_number,
           coalesce(sum(enters_count), 0) as Nb_visiteurs,
    from base_visiteurs
    left join date_comparable
           on date_comparable.date_n = date(base_visiteurs.count_date)
    group by all
),

visiteurs as (
    select a.*,
           coalesce(sum(b.Nb_visiteurs),0) Nb_visiteurs_n_1
    from visiteurs_n a
    left join visiteurs_n b on a.date_n_1 = b.count_date
                        and a.entity_number = b.entity_number
    group by all
),


Mag AS -- 145 magasin
(

    SELECT DISTINCT MAG.NUM_ETT as num_mag, concat(MAG.NUM_ETT, " - " , libett.LIB_ETT) as lib_mag ,MAG.NUM_REG as num_zone, LIBRGRP.LIB_RGRP as lib_zone, top_frch
    FROM `dfdp-teradata6y.BaseGeneriqueLmfr.TA001_BAG_MAG` MAG
    INNER JOIN `dfdp-teradata6y.BaseGeneriqueLmfr.TD001_BAG_LIBETT` libett
    ON (libett.NUM_BU = 1 AND libett.NUM_ETT = MAG.NUM_ETT AND MAG.NUM_TYPETT=libett.NUM_TYPETT)
    INNER JOIN `dfdp-teradata6y.BaseGeneriqueLmfr.TD001_BAG_LIBRGRP` LIBRGRP
    ON (LIBRGRP.NUM_BU = 1 AND LIBRGRP.COD_STA=1 AND MAG.NUM_REG = LIBRGRP.NUM_RGRP AND LIBRGRP.NUM_TYPRGRP = 2) -- 2 <-> Zone de conquête habitant
    WHERE MAG.NUM_TYPETT=1
    AND MAG.DAT_OUV <= CURRENT_DATE()
    AND (MAG.DAT_FERM IS NULL OR MAG.DAT_FERM > CURRENT_DATE())
    AND MAG.NUM_ETT not in (300,380,382,383, 384,998) --MAG.NUM_REG =9

)


select
    Annee,
    Mois,
    Semaine,
    Jour,
    num_zone,
    lib_zone,
    Num_Etablissement as Num_mag,
    Mag.lib_mag,
    Nb_clients,
    if(Nb_clients_n_1 is null, null, Nb_clients) Nb_clients_up,
    Nb_visiteurs,
    if(Nb_visiteurs_n_1 is null, null, Nb_visiteurs) Nb_visiteurs_up,
    Nb_clients_Corr,
    if(Nb_clients_n_1_Corr is null, null, Nb_clients_Corr) Nb_clients_Corr_up,
    Nb_visiteurs_Corr,
    if(Nb_visiteurs_n_1_Corr is null, null, Nb_visiteurs_Corr) Nb_visiteurs_Corr_up,
    Nb_clients_n_1,
    if(Nb_clients is null, null, Nb_clients_n_1) Nb_clients_n_1_up,
    Nb_visiteurs_n_1,
    if(Nb_visiteurs is null, null, Nb_visiteurs_n_1) Nb_visiteurs_n_1_up,
    Nb_clients_n_1_Corr,
    if(Nb_clients_Corr is null, null, Nb_clients_n_1_Corr) Nb_clients_n_1_Corr_up,
    Nb_visiteurs_n_1_Corr,
    if(Nb_visiteurs_Corr is null, null, Nb_visiteurs_n_1_Corr) Nb_visiteurs_n_1_Corr_up,
    case when Nb_clients_Corr/nullif(Nb_visiteurs_Corr,0) < 0.3
    or Nb_clients_Corr/nullif(Nb_visiteurs_Corr,0) > 0.7
    then null
    else Nb_clients_Corr/nullif(Nb_visiteurs_Corr,0)
    end as Tx_transfo

from
(
    select
    extract(year from dat_vte) Annee,
    extract(month from dat_vte) Mois,
    extract(week from dat_vte) Semaine,
    dat_vte Jour,
    clients.NUM_ETT as Num_Etablissement,
    sum(Nb_clients) Nb_clients,
    sum(Nb_visiteurs) Nb_visiteurs,
    sum(Nb_clients_n_1) Nb_clients_n_1,
    sum(Nb_visiteurs_n_1) Nb_visiteurs_n_1,
    sum(case when Nb_visiteurs is null or Nb_visiteurs=0 or Nb_clients/nullif(Nb_visiteurs,0) < 0.3 OR Nb_clients/nullif(Nb_visiteurs,0) > 0.7 THEN null ELSE Nb_clients end) as Nb_clients_Corr,
    sum(case when Nb_visiteurs is null or Nb_visiteurs=0 or Nb_clients/nullif(Nb_visiteurs,0) < 0.3 OR Nb_clients/nullif(Nb_visiteurs,0) > 0.7 THEN null ELSE Nb_visiteurs end) as Nb_visiteurs_Corr,
    -- on considère qu'un taux en dehors de ces bornes est erronné

    sum(case when Nb_visiteurs_n_1 is null or Nb_visiteurs_n_1=0 or Nb_clients_n_1/nullif(Nb_visiteurs_n_1,0) < 0.3 OR Nb_clients_n_1/nullif(Nb_visiteurs_n_1,0) > 0.7 THEN null ELSE Nb_clients_n_1 end) as Nb_clients_n_1_Corr,
    sum(case when Nb_visiteurs_n_1 is null or Nb_visiteurs_n_1=0 or Nb_clients_n_1/nullif(Nb_visiteurs_n_1,0) < 0.3 OR Nb_clients_n_1/nullif(Nb_visiteurs_n_1,0) > 0.7 THEN null ELSE Nb_visiteurs_n_1 end) as Nb_visiteurs_n_1_Corr

    from clients
    left join visiteurs
           on clients.dat_vte = visiteurs.count_date
           and clients.num_ett = visiteurs.entity_number

    where clients.dat_vte < current_date()
    -- and clients.num_ett not in (290,292,293,294,295,296,297,298,380) -- hors magasins franchisés
    group by 1,2,3,4,5
) A
left join Mag on A.Num_Etablissement = Mag.num_mag

)
