INSERT INTO dw_report.mktg_fb_campaign_perf_f
(
 campaign_domain_id
,campaign_type_id
,vertical_id
,ext_customer_id
,acct_desc_nm
,acct_currency_cd
,acct_tz_nm
,campaign_id
,campaign_nm
,call_to_actn_clicks_ct
,dvc_type_cd
,clicks_ct
,cpm
,cpp
,ctr
,dw_eff_dt
,ad_serv_freq
,imprsn_ct
,inline_link_clicks_ct
,reach_ct
,social_clicks_ct
,social_imprsn_ct
,social_reach_ct
,spend_am
,total_actn_val_am
,total_actns_ct
,total_unique_actns_ct
,unique_clicks_ct
,unique_ctr
,unique_inline_link_clicks_ct
,unique_social_clicks_ct
,src_sys_id
,dw_campaign_perf_sk
,dw_load_ts
)
 SELECT
stg.campaign_domain_id
,stg.campaign_type_id
,stg.vertical_id
,stg.ext_customer_id
,stg.acct_desc_nm
,stg.acct_currency_cd
,stg.acct_tz_nm
,stg.campaign_id
,stg.campaign_nm
,stg.call_to_actn_clicks_ct
,stg.dvc_type_cd
,stg.clicks_ct
,stg.cpm
,stg.cpp
,stg.ctr
,stg.dw_eff_dt
,stg.ad_serv_freq
,stg.imprsn_ct
,stg.inline_link_clicks_ct
,stg.reach_ct
,stg.social_clicks_ct
,stg.social_imprsn_ct
,stg.social_reach_ct
,stg.spend_am
,stg.total_actn_val_am
,stg.total_actns_ct
,stg.total_unique_actns_ct
,stg.unique_clicks_ct
,stg.unique_ctr
,stg.unique_inline_link_clicks_ct
,stg.unique_social_clicks_ct
,stg.src_sys_id
,max_sk.MAX_dw_campaign_perf_sk + row_number()
  over (ORDER BY ext_customer_id, campaign_id, dw_eff_dt, dvc_type_cd)
  AS dw_campaign_perf_sk
,stg.dw_load_ts
FROM
dw_stage.mktg_fb_campaign_perf_w stg
LEFT JOIN
(  SELECT COALESCE(MAX(dw_campaign_perf_sk),0) AS MAX_dw_campaign_perf_sk FROM dw_report.mktg_fb_campaign_perf_f  ) max_sk
ON 1 = 1
WHERE
NOT EXISTS
(SELECT 1 FROM dw_report.mktg_fb_campaign_perf_f tgt WHERE
    stg.ext_customer_id = tgt.ext_customer_id
AND stg.campaign_id = tgt.campaign_id
AND stg.dw_eff_dt = tgt.dw_eff_dt
AND stg.dvc_type_cd = tgt.dvc_type_cd
);
