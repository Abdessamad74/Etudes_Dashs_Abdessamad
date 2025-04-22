-- Version 5 iso booster avec client identifié -----------------------------------------------------------------------------------

declare date_debut date default "2023-01-01";
declare date_fin date default date_sub(date_trunc(current_date, month), interval 1 day);

create or replace table `ddp-bus-commerce-prd-frlm.concept_mag.concept_attractivite` as (

      with sales as (
            select distinct
            entity_number,
            customer_sale_date,
            customer_sale_id,
            client_id,
            flag_sale,
            product_nomenclature_department_number,
            customer_sale_line_amount_vat_included

            from `lmfr-ddp-dwh-prd.store_sale.tf_customer_sale_line` p
            left join `lmfr-ddp-dwh-prd.product_repository.td_product_nomenclature` q
                   on p.product_number= cast (q.product_number as INT64)

            where true
            and p.business_unit_id = 1
            and p.entity_type_number = 1
            -- and is_franchised_entity_flag = 0
            and customer_sale_date between date_debut and date_fin
            and flag_ignore_sale = 0
            and product_type_number != 9 -- hors services (articles 49)
      ),

      pros as (
            select distinct p.num_cli,
                  date_histo,
                  case when  (TOP_PRO_AVERE = 1 or TOP_SUSPICION_PRO =1) then "2 - Déclaré & suspectés"
                        when  (TOP_PRO_AVERE = 0 and TOP_SUSPICION_PRO =0) then "1 - Particuliers"
                        else null end typ_pro,
                  case when  (TOP_PRO_AVERE = 1 or TOP_SUSPICION_PRO =1) and a.ape_code is not null then "2 - Pros habitat"
                        when  (TOP_PRO_AVERE = 1 or TOP_SUSPICION_PRO =1) and a.ape_code is null then "3 - Autres pros"
                        when  (TOP_PRO_AVERE = 0 and TOP_SUSPICION_PRO =0) then "1 - Particuliers"
                        else null end typ_cli

            from `lmfr-ddp-ods-prd.detection_pro.th_detection_pro`p
            left join (select distinct idclient_ens, ape from `dfdp-valliuz-lmfr.CustomerScoring.B2B` )  b
                  on p.num_cli=safe_cast(b.idclient_ens as int64)
            left join `ddp-bus-commerce-prd-frlm.concept_mag.referentiel_pro_ape` a on trim(a.ape_code)=trim(b.ape)

            -- where  p.date_histo = date_trunc(current_date(), month)-- /!\ à changer
      ),

      actifs as (
                select
                    date_sub(date(metaseg.date_histo), interval 1 month) date_histo,
                    if(meta_segmentation = "NON_IDENTIFIES" and num_cliphy is null,-1, num_cliphy) client_id,
                    case when meta_segmentation in ("NOUVEAUX","ACTIFS_N") then 1
                         when meta_segmentation in ("ACTIFS_N-1","ACTIFS_N-2","ACTIFS_N-3") then 0
                         when meta_segmentation ="NON_IDENTIFIES" then -1
                    end as top_nouveau

                from `lmfr-ddp-dwh-prd.dwh_customer.Meta_segmentation_omnicanal_metaseg_foyer_v1` metaseg
                left join (select distinct num_cliphy, num_foy from `dfdp-teradata6y.CustomerCatalogLmfr.TD001_RCL_CLIPHY` where num_bu = 1 and COD_STA = 1) CP
                       on CP.NUM_FOY=metaseg.NUM_FOY

                where date_histo >= date_debut
      ),

      attract_magasin as (

          select date_trunc(customer_sale_date, month) mois,
                 entity_number,
                 count(distinct client_id) Nb_clients,
                 count(distinct if(cpt_tic = 1 and client_id is not null, customer_sale_id,null)) Nb_tickets_clients,
                 sum(cpt_tic) Nb_tickets,
                 sum(if(nb_Rayon > 1, cpt_tic,0)) Nb_tickets_multi_rayon,
                 sum(cpt_tic*nb_Rayon) Nb_rayon_tickets,

                 sum(if(client_id is not null, cpt_tic, 0)) Nb_tickets_id,
                 sum(if(client_id is null, cpt_tic, 0)) Nb_tickets_non_id,
                 sum(if(client_id is null, ca, 0)) ca_non_id,

                 sum(if(client_id is not null and (top_nouveau = 1 or top_nouveau is null), cpt_tic, 0)) Nb_tickets_nouveaux,
                 sum(if(client_id is not null and top_nouveau = 0, cpt_tic, 0)) Nb_tickets_anciens,

                 sum(if(client_id is not null and (typ_cli = "1 - Particuliers" or typ_pro is null), cpt_tic, 0)) Nb_tickets_parts,
                 sum(if(client_id is not null and typ_cli = "2 - Pros habitat", cpt_tic, 0)) Nb_tickets_pros,
                 sum(if(client_id is not null and typ_cli = "3 - Autres pros", cpt_tic, 0)) Nb_tickets_autres_pros,

                 sum(if(client_id is not null and (typ_cli = "1 - Particuliers" or typ_pro is null), ca, 0)) ca_parts,
                 sum(if(client_id is not null and typ_cli = "2 - Pros habitat", ca, 0)) ca_pros,
                 sum(if(client_id is not null and typ_cli = "3 - Autres pros", ca, 0)) ca_autres_pros,

                 sum(if(client_id is not null and (typ_pro = "1 - Particuliers" or typ_pro is null) and (top_nouveau = 1 or top_nouveau is null), cpt_tic,0)) Nb_tickets_parts_nouveaux,
                 sum(if(client_id is not null and typ_cli = "2 - Pros habitat" and (top_nouveau = 1 or top_nouveau is null), cpt_tic,0)) Nb_tickets_pros_nouveaux,

                 sum(if(client_id is not null and (typ_pro = "1 - Particuliers" or typ_pro is null) and top_nouveau = 0, cpt_tic, 0)) Nb_tickets_parts_anciens,
                 sum(if(client_id is not null and typ_cli = "2 - Pros habitat" and top_nouveau = 0, cpt_tic, 0)) Nb_tickets_pros_anciens,

          from (
                    select customer_sale_id,
                           entity_number,
                           sales.customer_sale_date,
                           max(flag_sale) flag_sale,
                           max(sales.client_id) client_id,
                           max(typ_pro) typ_pro,
                           max(typ_cli) typ_cli,
                           max(actifs.top_nouveau) top_nouveau,
                           sum(customer_sale_line_amount_vat_included) ca,
                           if(sum(customer_sale_line_amount_vat_included) >= 0, 1,-1) cpt_tic,
                           count( distinct product_nomenclature_department_number) as nb_Rayon
                    from sales
                    left join pros
                           on pros.num_cli = sales.client_id
                           and pros.date_histo = date_trunc(sales.customer_sale_date,month)
                    left join actifs
                           on sales.client_id = actifs.client_id
                           and actifs.date_histo = date_trunc(sales.customer_sale_date, month)
                    group by 1, 2, 3
          )

          group by 1,2
      )

      select * from attract_magasin
);
