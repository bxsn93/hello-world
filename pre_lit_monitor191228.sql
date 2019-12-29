--Recovery calculation accompanied with outstanding
--1、要分为extraction_date,client和days_afer_gen三个层级计算；
--2、payment和clientjoin时要使用left join，才能保留没有回款的客户记录；
--3、outstanding在client层级上就不变了，recovery会监控30天内的回款，recovery和outstanding要分开在不同层级下计算；
--4、为了数据透视表对outstanding求和汇总时不重复加和，要将相应分类下除第一行外的outstanding设置为0。
-----------------------------1 Provision-------------------------------------------------
DROP TABLE pre_lit_daily_provision;
CREATE TABLE pre_lit_daily_provision TABLESPACE playground_data_temp AS 

WITH CLIENTS AS (
SELECT DISTINCT 
       a.SKP_CLIENT,
       '360+' AS property,
       a.PILOT, 
       a.date_decision,
       a.extraction_date,
       a.skp_credit_case，
       a.due_amount  
  FROM AP_COLL.PreL_LETTER_INFOR_4 a --360+
 WHERE a.extraction_date >= DATE'2019-10-31'
UNION ALL
SELECT DISTINCT 
       b.SKP_CLIENT,
       '360-' AS property,
       b.group_name, 
       b.date_decision,
       b.date_generated,
       b.skp_credit_case,
       b.due_amount        
  FROM ap_coll.preli_360_lttr_hist b  --360-
 WHERE b.date_generated >= DATE'2019-11-18'
UNION ALL
SELECT DISTINCT 
       c.SKP_CLIENT,
       'IEXA' AS property,
       c.pilot, 
       c.date_decision,
       c.extraction_date,
       c.skp_credit_case,
       c.due_amount        
  FROM ap_coll.prel_letter_infor_iexa c --IEXA
  WHERE c.extraction_date >= DATE'2019-11-13'
)
,PRODUCT AS (
SELECT DISTINCT pro.skp_product, pr.date_decision,
case when acqui.code_credit_acquisition_chnl='EXTREME' and ct.CODE_CREDIT_TYPE='SS'  then 'ESHOP'
     when acqui.code_credit_acquisition_chnl='EXTREME' and ct.CODE_CREDIT_TYPE='SC'  then 'MCL'
     --when pro.SKP_PRODUCT_CHANNEL in (1,201) THEN 'XCL'--SC001£ºCash loan - Xsell
     when pro.SKP_PRODUCT_CHANNEL=1 and pr.date_decision>date'2017-12-31' THEN 'XCL_STD_NEW'
     when pro.SKP_PRODUCT_CHANNEL=1 THEN 'XCL_STD_OLD'
     when pro.SKP_PRODUCT_CHANNEL=201 then 'XCL_STUP'
     when pro.CODE_PRODUCT like 'FCACL%' then 'ACL'
     when pro.SKP_PRODUCT_CHANNEL=2 THEN 'WCL' --SC002£ºCash loan - Walkin
     when gl.NAME_GOODS_CATEGORY_ENG='Motorbike'  then 'TW'
     else 'CD'
     end AS GOODS_TYPE
from clients pr

join owner_dwh.dc_credit_case creca
on pr.skp_credit_case=creca.SKP_CREDIT_CASE
AND pr.date_decision = creca.date_decision

join owner_dwh.dc_product pro
on creca.SKP_PRODUCT=pro.SKP_PRODUCT
--credit_type
join owner_dwh.cl_credit_type ct
on creca.SKP_CREDIT_TYPE=ct.SKP_CREDIT_TYPE

join owner_dwh.cl_credit_acquisition_chnl acqui
on creca.SKP_CREDIT_ACQUISITION_CHNL=acqui.skp_credit_acquisition_chnl
--goods_type
join OWNER_DWH.DC_GOODS_TYPE gl
on creca.SKP_GOODS_TYPE_EXP=gl.SKP_GOODS_TYPE
)
,RESULTS AS (
SELECT /*+FULL(c) NO_MERGE */ 
       rma.property,
       rma.extraction_date,
       rma.pilot,
       rma.skp_client,
       rma.due_amount,
       pro.GOODS_TYPE AS product_group,
       rma.skp_credit_case

  FROM CLIENTS RMA 
  JOIN owner_dwh.dc_contract c
    on rma.skp_client = c.skp_client
   and rma.skp_credit_case = c.skp_credit_case
   AND rma.date_decision = c.date_decision
  join PRODUCT pro
    on c.skp_product = pro.skp_product
)
,provision AS (
 select pp.property,
        pp.pilot,
        pp.extraction_date,
        SUM(pp.due_amount) AS amt_receivable,
        SUM(pp.due_amount*provi.provision_rate) AS amt_provision,         
        COUNT(DISTINCT pp.skp_credit_case) AS cnt_case,
        COUNT(DISTINCT pp.skp_client) AS cnt_client,
        buc.CODE_DPD_TOL_SUBBUCKET,
        provi.product_group,
        provi.provision_rate
        
  from RESULTS pp
  join owner_dwh.dc_dpd_tol_bucket buc
    on dp.skp_dpd_tol_bucket = buc.skp_dpd_tol_bucket
  join ap_coll.ft_coll_provision_rate provi --needs to be changed, missing join condition of dpd bucket!!!
    on pp.product_group = provi.product_group
   and buc.CODE_DPD_TOL_SUBBUCKET = provi.flag_bucket

 group BY pp.property, 
        pp.pilot,
        pp.extraction_date,
        buc.CODE_DPD_TOL_SUBBUCKET,
        provi.product_group,
        provi.provision_rate
)
,provision_rate AS (
SELECT pr.property,
       pr.extraction_date,
       pr.pilot,
       SUM(pr.amt_provision) AS amt_provision_total
FROM provision pr
GROUP BY pr.property,pr.extraction_date,pilot
)
,provision_rate_base AS (
SELECT pr.property,
       pr.extraction_date,
       pr.pilot,
       SUM(pr.amt_provision) AS amt_provision_total_base
FROM provision pr
WHERE pr.pilot = 'Chal1'
GROUP BY pr.property,pr.extraction_date,pilot
)
SELECT ra.property,
       ra.extraction_date,
       ra.pilot,
       rb.amt_provision_total_base/ra.amt_provision_total AS daily_provision_rate
FROM provision_rate ra
JOIN provision_rate_base rb
  ON ra.extraction_date = rb.extraction_date
 AND ra.property = rb.property
;
------------------------2 base information including debt----------------------------
DROP TABLE monitor_base;
CREATE TABLE monitor_base AS--include 360+,360- and iexa
SELECT '360+' AS property
       ,a.pilot
       ,a.skp_client
       ,a.extraction_date
       ,a.skp_credit_case
       ,a.date_assigned
       ,a.due_amount --old monitor report use data from ap_coll.PPL2_CHCH_CLIENT_LIST_TMP to calculate client_total outstanding, which doesn't make sense, 
                     --because column outstanding_total of that table represents only the highest CPD contract's outstanding, not total outstanding!
       ,b.cpd_ranges
       ,CASE WHEN a.extraction_date- a.date_assigned BETWEEN 0 AND 6 THEN '0-6'
             WHEN a.extraction_date - a.date_assigned BETWEEN 7 AND 13 THEN '7-13'
             WHEN a.extraction_date - a.date_assigned BETWEEN 14 AND 20 THEN '14-20'
             WHEN a.extraction_date - a.date_assigned BETWEEN 21 AND 27 THEN '21-27'
             WHEN a.extraction_date - a.date_assigned BETWEEN 28 AND 34 THEN '28-34'
             WHEN a.extraction_date - a.date_assigned BETWEEN 35 AND 41 THEN '35-41'
             WHEN a.extraction_date - a.date_assigned BETWEEN 42 AND 48 THEN '42-48'
             WHEN a.extraction_date - a.date_assigned BETWEEN 49 AND 55 THEN '49-55'
             WHEN a.extraction_date - a.date_assigned BETWEEN 56 AND 62 THEN '56-62'
             WHEN a.extraction_date - a.date_assigned >= 63 THEN '63-70'
        END AS DAYS_EXA_ASSIGNMENT_GROUP
       ,CASE WHEN a.pilot IN ('Cham1','Cham2') THEN 'Not sending'
             WHEN a.pilot IN ('Chal1','Chal2') THEN c.TEXT_DELIVERY_RESULT
        END AS TEXT_DELIVERY_RESULT_more
       ,CASE WHEN a.pilot IN ('Cham1','Cham2') THEN 'Not sending'
             WHEN a.pilot IN ('Chal1','Chal2') AND c.TEXT_DELIVERY_RESULT IN ('Customer request to delay posting','Delivered','Delivered to client','Delivered to others',
               'Delivered to self pick-up site','Customer will pickup') THEN 'Delivered'
             WHEN a.pilot IN ('Chal1','Chal2') AND c.TEXT_DELIVERY_RESULT IN ('退件','Delivery address and name is invalid','Return to Post office','Return to Post Office (assume)')
               THEN 'Un-deliverd'
             ELSE c.TEXT_DELIVERY_RESULT
        END AS TEXT_DELIVERY_RESULT
  FROM ap_coll.prel_letter_infor_4 a  --case level
  JOIN ap_coll.PPL2_CHCH_CLIENT_LIST_TMP b  --client level, to find CPD
    ON a.extraction_date = b.selection_date
   AND a.skp_client = b.skp_client
  JOIN owner_dwh.f_letter_result_tt c
    ON a.extraction_date = c.DATE_GENERATED
   AND a.skp_credit_case = c.SKP_CREDIT_CASE
   AND c.SKP_COLL_LETTER_TYPE = 13  --360+ letter (10,11,12 are very old pilot 360+ letters)
 WHERE a.extraction_date >= DATE'2019-10-31' --new pilot starts from DATE'2019-10-31'
 
UNION ALL

SELECT '360-' AS property
       ,aa.group_name
       ,aa.skp_client
       ,aa.date_generated
       ,aa.skp_credit_case
       ,aa.date_assignment
       ,aa.due_amount
       ,aa.cpd_range
       ,CASE WHEN aa.date_generated - aa.date_assignment BETWEEN 0 AND 6 THEN '0-6'
             WHEN aa.date_generated - aa.date_assignment BETWEEN 7 AND 13 THEN '7-13'
             WHEN aa.date_generated - aa.date_assignment BETWEEN 14 AND 20 THEN '14-20'
             WHEN aa.date_generated - aa.date_assignment BETWEEN 21 AND 27 THEN '21-27'
             WHEN aa.date_generated - aa.date_assignment BETWEEN 28 AND 34 THEN '28-34'
             WHEN aa.date_generated - aa.date_assignment BETWEEN 35 AND 41 THEN '35-41'
             WHEN aa.date_generated - aa.date_assignment BETWEEN 42 AND 48 THEN '42-48'
             WHEN aa.date_generated - aa.date_assignment BETWEEN 49 AND 55 THEN '49-55'
             WHEN aa.date_generated - aa.date_assignment BETWEEN 56 AND 62 THEN '56-62'
             WHEN aa.date_generated - aa.date_assignment >= 63 THEN '63-70'
        END AS DAYS_EXA_ASSIGNMENT_GROUP
       ,CASE WHEN aa.group_name IN ('Cham1','Cham2') THEN 'Not sending'
             WHEN aa.group_name IN ('Chal1','Chal2') THEN c.TEXT_DELIVERY_RESULT
        END AS TEXT_DELIVERY_RESULT_more
       ,CASE WHEN aa.group_name IN ('Cham1','Cham2') THEN 'Not sending'
             WHEN aa.group_name IN ('Chal1','Chal2') AND c.TEXT_DELIVERY_RESULT IN ('Customer request to delay posting','Delivered','Delivered to client','Delivered to others',
               'Delivered to self pick-up site','Customer will pickup') THEN 'Delivered'
             WHEN aa.group_name IN ('Chal1','Chal2') AND c.TEXT_DELIVERY_RESULT IN ('退件','Delivery address and name is invalid','Return to Post office','Return to Post Office (assume)')
               THEN 'Un-deliverd'
             ELSE c.TEXT_DELIVERY_RESULT
        END AS TEXT_DELIVERY_RESULT
  FROM ap_coll.preli_360_lttr_hist aa  --case level
  JOIN owner_dwh.f_letter_result_tt c
    ON aa.date_generated = c.DATE_GENERATED
   AND aa.skp_credit_case = c.SKP_CREDIT_CASE
   AND c.SKP_COLL_LETTER_TYPE = 121  --360- letter
 WHERE aa.date_generated >= DATE'2019-11-18' --new pilot starts from DATE'2019-10-18'
 
UNION ALL

SELECT 'IEXA' AS property
       ,ab.pilot
       ,ab.skp_client
       ,ab.extraction_date
       ,ab.skp_credit_case
       ,TRUNC(ab.extraction_date,'iw') AS date_assigned
       ,ab.due_amount
       ,bb.cpd_ranges
       ,'XNA' AS DAYS_EXA_ASSIGNMENT_GROUP
       ,CASE WHEN ab.pilot IN ('Cham1','Cham2') THEN 'Not sending'
             WHEN ab.pilot IN ('Chal1','Chal2') THEN c.TEXT_DELIVERY_RESULT
        END AS TEXT_DELIVERY_RESULT_more
       ,CASE WHEN ab.pilot IN ('Cham1','Cham2') THEN 'Not sending'
             WHEN ab.pilot IN ('Chal1','Chal2') AND c.TEXT_DELIVERY_RESULT IN ('Customer request to delay posting','Delivered','Delivered to client','Delivered to others',
               'Delivered to self pick-up site','Customer will pickup') THEN 'Delivered'
             WHEN ab.pilot IN ('Chal1','Chal2') AND c.TEXT_DELIVERY_RESULT IN ('退件','Delivery address and name is invalid','Return to Post office','Return to Post Office (assume)')
               THEN 'Un-deliverd'
             ELSE c.TEXT_DELIVERY_RESULT
        END AS TEXT_DELIVERY_RESULT
  FROM ap_coll.prel_letter_infor_iexa ab  --case level
  JOIN AP_COLL.PPL2_IEXA_CLIENT_LIST_TMP bb  --client level, to find CPD
    ON ab.extraction_date = bb.selection_date
   AND ab.skp_client = bb.skp_client
  JOIN owner_dwh.f_letter_result_tt c
    ON ab.extraction_date = c.DATE_GENERATED
   AND ab.skp_credit_case = c.SKP_CREDIT_CASE
   AND c.SKP_COLL_LETTER_TYPE = 221  --IEXA letter
 WHERE ab.extraction_date >= DATE'2019-11-13' --new pilot starts from DATE'2019-11-13'
;
CREATE TABLE monitor_base_client AS --client level
SELECT a.property
       ,a.pilot
       ,a.extraction_date
       ,a.skp_client
       ,a.text_delivery_result
       ,a.text_delivery_result_more
       ,a.cpd_ranges
       ,a.days_exa_assignment_group
       ,SUM(a.due_amount) AS due_amount
       ,COUNT(DISTINCT a.skp_credit_case) AS cnt_case
FROM monitor_base a
GROUP BY a.property
       ,a.pilot
       ,a.extraction_date
       ,a.skp_client
       ,a.text_delivery_result
       ,a.text_delivery_result_more
       ,a.cpd_ranges
       ,a.days_exa_assignment_group
;
-----------------------------3 payment----------------------------------------------
DROP TABLE monitor_payment;
CREATE TABLE monitor_payment AS --client level
SELECT a.property
       ,a.pilot
       ,a.extraction_date
       ,a.skp_client
       ,a.text_delivery_result
       ,a.text_delivery_result_more
       ,a.cpd_ranges
       ,a.days_exa_assignment_group
       ,b.date_payment_incoming
       ,b.date_payment_incoming - a.extraction_date AS days_after_gen
       ,SUM(b.amt_payment) AS amt_payment
FROM monitor_base a
JOIN owner_dwh.f_incoming_payment_tt b
  ON a.skp_client = b.skp_client
 AND a.skp_credit_case = b.skp_credit_case
 AND a.extraction_date <= b.date_payment_incoming
 AND a.extraction_date >= b.date_payment_incoming -30
 AND b.date_payment_incoming >= DATE'2019-10-31'
 AND b.flag_waive_payment = 'N'
 AND b.CODE_STATUS = 'a'
 AND b.skp_incoming_payment_type BETWEEN 1 AND 2
 AND b.flag_payment_overpay = 'N'
GROUP BY a.property
         ,a.pilot
         ,a.extraction_date
         ,a.skp_client
         ,a.text_delivery_result
         ,a.text_delivery_result_more
         ,a.cpd_ranges
         ,a.days_exa_assignment_group
         ,b.date_payment_incoming
;
CREATE TABLE monitor_payment_cum AS --client level
SELECT a.property
       ,a.pilot
       ,a.extraction_date
       ,a.skp_client     
       ,a.text_delivery_result
       ,a.text_delivery_result_more
       ,a.cpd_ranges
       ,a.days_exa_assignment_group
       ,a.days_after_gen
       ,SUM(b.amt_payment) AS amt_payment
FROM monitor_payment a
JOIN monitor_payment b
  ON a.property = b.property
 AND a.pilot = b.pilot
 AND a.extraction_date = b.extraction_date
 AND a.skp_client = b.skp_client
 AND a.days_after_gen >= b.days_after_gen
GROUP BY a.property
       ,a.pilot
       ,a.extraction_date
       ,a.skp_client 
       ,a.text_delivery_result
       ,a.text_delivery_result_more
       ,a.cpd_ranges
       ,a.days_exa_assignment_group
       ,a.days_after_gen
;
------------------------output1: daily recovery & outstanding--------------------------------
WITH base AS(  --letter base (sent & delivery & outstanding) data on extraction_date level
SELECT a.property
       ,a.pilot
       ,a.extraction_date
       ,a.text_delivery_result
       ,a.text_delivery_result_more
       ,a.cpd_ranges
       ,a.days_exa_assignment_group
       ,SUM(a.due_amount) AS due_amount
       ,COUNT(DISTINCT a.skp_client) AS cnt_client
       ,COUNT(DISTINCT a.skp_credit_case) AS cnt_case
FROM monitor_base a
GROUP BY a.property
       ,a.pilot
       ,a.extraction_date
       ,a.text_delivery_result
       ,a.text_delivery_result_more
       ,a.cpd_ranges
       ,a.days_exa_assignment_group
)  
,payment AS (   --daily recovery on days_after_gen level
SELECT a.property
       ,a.pilot
       ,a.extraction_date
       ,a.text_delivery_result
       ,a.text_delivery_result_more
       ,a.cpd_ranges
       ,a.days_exa_assignment_group
       ,a.days_after_gen  
       ,SUM(a.amt_payment) AS amt_payment
FROM monitor_payment a
GROUP BY a.property
       ,a.pilot
       ,a.extraction_date       
       ,a.text_delivery_result
       ,a.text_delivery_result_more
       ,a.cpd_ranges
       ,a.days_exa_assignment_group
       ,a.days_after_gen
)
,base_payment AS( --join base and recovery
SELECT a.*,b.days_after_gen,b.amt_payment,
       row_number() OVER (PARTITION BY a.property,a.pilot,a.extraction_date,a.text_delivery_result_more,
                          a.text_delivery_result,a.cpd_ranges,a.days_exa_assignment_group ORDER BY b.days_after_gen) AS rn
FROM base a
LEFT JOIN payment b  --left join can keep all data in base remaining
ON a.property = b.property
AND a.pilot = b.pilot
AND a.extraction_date = b.extraction_date
AND a.text_delivery_result = b.text_delivery_result
AND a.text_delivery_result_more = b.text_delivery_result_more
AND a.cpd_ranges = b.cpd_ranges
AND a.days_exa_assignment_group = b.days_exa_assignment_group
)
SELECT a.property 
       ,a.pilot
       ,a.extraction_date
       ,a.text_delivery_result
       ,a.text_delivery_result_more
       ,a.cpd_ranges
       ,a.days_exa_assignment_group
       ,a.days_after_gen
       ,a.amt_payment
       ,CASE WHEN a.rn = 1 THEN a.due_amount ELSE 0 END AS due_amount --keep original amount of first row, set other rows to 0, 
                                                                      --which can avoid double sum calculation on base data in pivot table in Excel
       ,CASE WHEN a.rn = 1 THEN a.cnt_client ELSE 0 END AS cnt_client
       ,CASE WHEN a.rn = 1 THEN a.cnt_case   ELSE 0 END AS cnt_case        
FROM base_payment a
;  
------------------------output2: number of repaid clients--------------------------------
with base AS( --extraction_date level
SELECT a.property                    ,a.pilot                      ,a.extraction_date  
       ,a.text_delivery_result       ,a.text_delivery_result_more  ,a.cpd_ranges
       ,a.days_exa_assignment_group
       --,SUM(a.due_amount) AS due_amount
       ,COUNT(DISTINCT a.skp_client) AS cnt_client
       --,SUM(a.cnt_case) AS cnt_case
FROM monitor_base_client a
GROUP BY a.property                    ,a.pilot                      ,a.extraction_date  
         ,a.text_delivery_result       ,a.text_delivery_result_more  ,a.cpd_ranges
         ,a.days_exa_assignment_group
)
,debt_paid AS(  --days_after_gen level
SELECT a.property                    ,a.pilot                      ,a.extraction_date  
       ,a.text_delivery_result       ,a.text_delivery_result_more  ,a.cpd_ranges
       ,a.days_exa_assignment_group  ,b.days_after_gen
       ,count(case when b.amt_payment>0 then a.skp_client ELSE NULL END) AS cnt_client_repaid
       ,count(case when b.amt_payment>=0.5*a.due_amount then a.skp_client ELSE NULL END) AS cnt_client_half
       ,count(case when b.amt_payment>=a.due_amount then a.skp_client ELSE NULL END) AS cnt_client_full
       ,sum(case when b.amt_payment>0 then b.amt_payment ELSE NULL END) AS amt_repaid
       ,sum(case when b.amt_payment>=0.5*a.due_amount then b.amt_payment ELSE NULL END) AS amt_half
       ,sum(case when b.amt_payment>=a.due_amount then b.amt_payment ELSE NULL END) AS amt_full
FROM monitor_base_client a
LEFT JOIN monitor_payment_cum b
  ON a.property = b.property
 AND a.pilot = b.pilot
 AND a.extraction_date = b.extraction_date
 AND a.skp_client = b.skp_client
GROUP BY a.property                    ,a.pilot                      ,a.extraction_date  
         ,a.text_delivery_result       ,a.text_delivery_result_more  ,a.cpd_ranges
         ,a.days_exa_assignment_group  ,b.days_after_gen
)
,adj_debt_paid AS( --extraction_date level
SELECT a.property                    ,a.pilot                      ,a.extraction_date  
       ,a.text_delivery_result       ,a.text_delivery_result_more  ,a.cpd_ranges
       ,a.days_exa_assignment_group  ,a.cnt_client                 ,b.days_after_gen
       ,b.cnt_client_repaid          ,b.cnt_client_half            ,b.cnt_client_full            
       ,b.amt_repaid                 ,b.amt_half                   ,b.amt_full                   
       ,c.daily_provision_rate       ,a.cnt_client * c.daily_provision_rate AS adj_cnt_client
       ,b.cnt_client_repaid * c.daily_provision_rate AS adj_cnt_client_repaid
       ,b.cnt_client_half * c.daily_provision_rate AS adj_cnt_client_half
       ,b.cnt_client_full * c.daily_provision_rate AS adj_cnt_client_full
       ,b.amt_repaid * c.daily_provision_rate AS adj_amt_repaid
       ,b.amt_half * c.daily_provision_rate AS adj_amt_half
       ,b.amt_full * c.daily_provision_rate AS adj_amt_full
       ,row_number() OVER (PARTITION BY a.property,a.pilot,a.extraction_date,a.text_delivery_result_more,
                          a.text_delivery_result,a.cpd_ranges,a.days_exa_assignment_group ORDER BY b.days_after_gen) AS rn
FROM base a
JOIN debt_paid b
  ON a.property = b.property         
 AND a.pilot = b.pilot
 AND a.extraction_date = b.extraction_date
 AND a.text_delivery_result = b.text_delivery_result
 AND a.text_delivery_result_more = b.text_delivery_result_more
 AND a.cpd_ranges = b.cpd_ranges
 AND a.days_exa_assignment_group = b.days_exa_assignment_group
JOIN pre_lit_daily_provision c
  ON a.property = c.property
 AND a.extraction_date = c.extraction_date
 AND a.pilot = c.pilot
)
SELECT a.property                    ,a.pilot                      ,a.extraction_date  
       ,a.text_delivery_result       ,a.text_delivery_result_more  ,a.cpd_ranges
       ,a.days_exa_assignment_group  ,a.days_after_gen             ,a.adj_cnt_client
       ,CASE WHEN a.rn = 1 THEN a.cnt_client ELSE 0 END AS cnt_client
       ,a.cnt_client_repaid          ,a.cnt_client_half            ,a.cnt_client_full
       ,a.adj_cnt_client_repai       ,a.adj_cnt_client_half        ,a.adj_cnt_client_full
       ,a.amt_repaid                 ,a.amt_half                   ,a.amt_full
       ,a.adj_amt_repaid             ,a.adj_amt_half               ,a.adj_amt_full
       ,a.daily_provision_rate
FROM adj_debt_paid a
ORDER BY a.property                    ,a.pilot                      ,a.extraction_date  
         ,a.text_delivery_result       ,a.text_delivery_result_more  ,a.cpd_ranges
         ,a.days_exa_assignment_group  ,a.days_after_gen
;
SELECT extraction_date,property,COUNT(*) FROM pre_lit_daily_provision GROUP BY extraction_date,property ORDER BY extraction_date;
SELECT * FROM pre_lit_daily_provision;
SELECT DISTINCT SKP_PRODUCT_CHANNEL FROM owner_dwh.dc_product;
SELECT DISTINCT CODE_PRODUCT FROM owner_dwh.dc_product;
