--@exclude_input=tmall_rec_project.dw_user_item_alipay_log
--@exclude_input=tmall_rec_project.dw_user_item_cart_log
--@exclude_input=tmall_rec_project.dw_user_item_collect_log
--@exclude_input=tmall_rec_project.dw_user_item_click_log
--odps sql 
--********************************************************************--
--author:aliyun5858408693
--create time:2024-11-06 08:44:40
--********************************************************************--
CREATE TABLE IF NOT EXISTS brand_stat_feature_ads(
    brand_id STRING ,
    click_num BIGINT ,
    collect_num BIGINT ,
    cart_num BIGINT ,
    alipay_num BIGINT 
)PARTITIONED BY (ds STRING) LIFECYCLE 60;


INSERT OVERWRITE TABLE brand_stat_feature_ads PARTITION (ds=${bizdate})
SELECT t1.brand_id, click_num
    ,IF(t2.collect_num IS NULL, 0, t2.collect_num)
    ,IF(t3.cart_num IS NULL, 0, t3.cart_num)
    ,IF(t4.alipay_num IS NULL, 0, t4.alipay_num)
FROM (
    SELECT brand_id, COUNT(DISTINCT user_id) AS click_num
    FROM dw_user_item_click_log 
    WHERE ds<='${bizdate}'  AND ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd') 
    GROUP BY brand_id
)t1 LEFT JOIN (
    SELECT brand_id, COUNT(DISTINCT user_id) AS collect_num
    FROM dw_user_item_collect_log 
    WHERE ds<='${bizdate}'  AND ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd') 
    GROUP BY brand_id
)t2 ON t1.brand_id=t2.brand_id LEFT JOIN (
    SELECT brand_id, COUNT(DISTINCT user_id) AS cart_num
    FROM dw_user_item_cart_log 
    WHERE ds<='${bizdate}'  AND ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd') 
    GROUP BY brand_id
)t3 ON t1.brand_id=t3.brand_id LEFT JOIN (
    SELECT brand_id, COUNT(DISTINCT user_id) AS alipay_num
    FROM dw_user_item_alipay_log 
    WHERE ds<='${bizdate}'  AND ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd') 
    GROUP BY brand_id
)t4 ON t1.brand_id=t4.brand_id;

SELECT * FROM brand_stat_feature_ads  WHERE ds='${bizdate}' LIMIT 100;