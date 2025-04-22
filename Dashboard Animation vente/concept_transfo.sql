
----------- Version 5 ---------------------------------------------------------------------------------------------------------
declare date_debut date default "2023-01-01";
declare date_fin date default date_sub(date_trunc(current_date, month), interval 1 day);


create or replace table `ddp-bus-commerce-prd-frlm.concept_mag.concept_transfo` as (

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
           coalesce(sum(nbr_pascai),0) as Nb_pascai_transf
    from `lmfr-ddp-dwh-prd.store_sale_agg_seg_teradata.T_AGG_AGGVTE_MAGJOUR` mj
    left join date_comparable on date_comparable.date_n = mj.dat_vte
    where num_bu = 1
    and dat_vte between date_debut and date_fin
    group by all
), -- table source des clients

clients as (
    select a.*,
           coalesce(sum(b.Nb_pascai_transf), 0) Nb_pascai_transf_n_1
    from clients_n a
    left join clients_n b on a.date_n_1 = b.dat_vte
                        and a.num_ett = b.num_ett
    group by all
    --  if(extract(month from a.date_n_1) = 5 and extract(day from a.date_n_1) = 1, date_sub(a.date_n_1, interval 7 day), a.date_n_1) = b.dat_vte
),

/* ---------------------Base Visiteurs ------------------------------------------------------------------*/

tmp as (
    select safe_cast(store_id as int64) store_id,
           date,
           time,
           parse_date('%Y%m%d', date) as count_date,
           safe_cast(substr(time,1,4) as int64) as count_time,
           enters,
           exits

    from `lmfr-ddp-ods-prd.shoppertrak.shoppertrak_quotidien_v1` r

    where lower(zone_name) not like '%ssa%'
),

base_visiteurs as (
    select count_date,
           entity_number,
           sum(enters_count) enters_count
    from (

        select store_id as entity_number,
               count_date,
               count_time,
               safe_cast(coalesce(tmp.enters, 0) as numeric) as enters_count,
               safe_cast(coalesce(tmp.exits, 0) as numeric) as exits_count
        from tmp
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
)

 select date_trunc(dat_vte, month) Mois,
              Num_mag,
              sum(Nb_pascai_transf) Nb_pascai_transf,
              sum(Nb_pascai_transf_up) Nb_pascai_transf_up,
              sum(Nb_visiteurs) Nb_visiteurs,
              sum(Nb_visiteurs_up) Nb_visiteurs_up,
              sum(Nb_pascai_transf_Corr) Nb_pascai_transf_Corr,
              sum(Nb_pascai_transf_Corr_up) Nb_pascai_transf_Corr_up,
              sum(Nb_visiteurs_Corr) Nb_visiteurs_Corr,
              sum(Nb_visiteurs_Corr_up) Nb_visiteurs_Corr_up,
              sum(Nb_pascai_transf_n_1) Nb_pascai_transf_n_1,
              sum(Nb_pascai_transf_n_1_up) Nb_pascai_transf_n_1_up,
              sum(Nb_visiteurs_n_1) Nb_visiteurs_n_1,
              sum(Nb_visiteurs_n_1_up) Nb_visiteurs_n_1_up,
              sum(Nb_pascai_transf_n_1_Corr) Nb_pascai_transf_n_1_Corr,
              sum(Nb_pascai_transf_n_1_Corr_up) Nb_pascai_transf_n_1_Corr_up,
              sum(Nb_visiteurs_n_1_Corr) Nb_visiteurs_n_1_Corr,
              sum(Nb_visiteurs_n_1_Corr_up) Nb_visiteurs_n_1_Corr_up
       from (

                     select dat_vte, Num_Etablissement as Num_mag,
                            Nb_pascai_transf,
                            if(Nb_pascai_transf_n_1 is null, null, Nb_pascai_transf) Nb_pascai_transf_up,
                            Nb_visiteurs,
                            if(Nb_visiteurs_n_1 is null, null, Nb_visiteurs) Nb_visiteurs_up,
                            Nb_pascai_transf_Corr,
                            if(Nb_pascai_transf_n_1_Corr is null, null, Nb_pascai_transf_Corr) Nb_pascai_transf_Corr_up,
                            Nb_visiteurs_Corr,
                            if(Nb_visiteurs_n_1_Corr is null, null, Nb_visiteurs_Corr) Nb_visiteurs_Corr_up,
                            Nb_pascai_transf_n_1,
                            if(Nb_pascai_transf is null, null, Nb_pascai_transf_n_1) Nb_pascai_transf_n_1_up,
                            Nb_visiteurs_n_1,
                            if(Nb_visiteurs is null, null, Nb_visiteurs_n_1) Nb_visiteurs_n_1_up,
                            Nb_pascai_transf_n_1_Corr,
                            if(Nb_pascai_transf_Corr is null, null, Nb_pascai_transf_n_1_Corr) Nb_pascai_transf_n_1_Corr_up,
                            Nb_visiteurs_n_1_Corr,
                            if(Nb_visiteurs_Corr is null, null, Nb_visiteurs_n_1_Corr) Nb_visiteurs_n_1_Corr_up,
                            case when Nb_pascai_transf_Corr/nullif(Nb_visiteurs_Corr,0) < 0.3
                                   or Nb_pascai_transf_Corr/nullif(Nb_visiteurs_Corr,0) > 0.7
                            then null
                            else Nb_pascai_transf_Corr/nullif(Nb_visiteurs_Corr,0)
                            end as Tx_transfo

                     from
                     (
                            select
                            dat_vte, clients.NUM_ETT as Num_Etablissement,
                            sum(Nb_pascai_transf) Nb_pascai_transf,
                            sum(Nb_visiteurs) Nb_visiteurs,
                            sum(Nb_pascai_transf_n_1) Nb_pascai_transf_n_1,
                            sum(Nb_visiteurs_n_1) Nb_visiteurs_n_1,
                            sum(case when Nb_visiteurs is null or Nb_visiteurs=0 or Nb_pascai_transf/nullif(Nb_visiteurs,0) < 0.3 OR Nb_pascai_transf/nullif(Nb_visiteurs,0) > 0.7 THEN null ELSE Nb_pascai_transf end) as Nb_pascai_transf_Corr,
                            sum(case when Nb_visiteurs is null or Nb_visiteurs=0 or Nb_pascai_transf/nullif(Nb_visiteurs,0) < 0.3 OR Nb_pascai_transf/nullif(Nb_visiteurs,0) > 0.7 THEN null ELSE Nb_visiteurs end) as Nb_visiteurs_Corr,
                            -- on considère qu'un taux en dehors de ces bornes est erronné
                            sum(case when Nb_visiteurs_n_1 is null or Nb_visiteurs_n_1=0 or Nb_pascai_transf_n_1/nullif(Nb_visiteurs_n_1,0) < 0.3 OR Nb_pascai_transf_n_1/nullif(Nb_visiteurs_n_1,0) > 0.7 THEN null ELSE Nb_pascai_transf_n_1 end) as Nb_pascai_transf_n_1_Corr,
              sum(case when Nb_visiteurs_n_1 is null or Nb_visiteurs_n_1=0 or Nb_pascai_transf_n_1/nullif(Nb_visiteurs_n_1,0) < 0.3 OR Nb_pascai_transf_n_1/nullif(Nb_visiteurs_n_1,0) > 0.7 THEN null ELSE Nb_visiteurs_n_1 end) as Nb_visiteurs_n_1_Corr

                            from clients
                            left join visiteurs
                                   on clients.dat_vte = visiteurs.count_date
                                   and clients.num_ett = visiteurs.entity_number

                            where clients.dat_vte < date_trunc(current_date(), month) -- on ne prend pas en compte les données du mois en cours car incomplètes
                            -- and clients.num_ett not in (290,292,293,294,295,296,297,298,380) -- hors magasins franchisés
                            group by 1,2
                            order by 1,2
                     )
       )
       group by 1,2
);
