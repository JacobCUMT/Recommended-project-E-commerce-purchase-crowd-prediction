--@exclude_input=tmall_rec_project.dw_user_item_click_log
--@exclude_input=tmall_rec_project.dw_user_item_collect_log
--@exclude_input=tmall_rec_project.dw_user_item_cart_log
--@exclude_input=tmall_rec_project.dw_user_item_alipay_log
--odps sql 
--********************************************************************--
--author:aliyun5858408693
--create time:2024-11-06 09:29:22
--********************************************************************--
CREATE TABLE IF NOT EXISTS user_click_beh_feature_ads(
    user_id STRING,

    item_num_3d BIGINT,
    brand_num_3d BIGINT,
    seller_num_3d BIGINT,
    cate1_num_3d BIGINT,
    cate2_num_3d BIGINT,
    cnt_days_3d BIGINT,

    item_num_7d BIGINT,
    brand_num_7d BIGINT,
    seller_num_7d BIGINT,
    cate1_num_7d BIGINT,
    cate2_num_7d BIGINT,
    cnt_days_7d BIGINT,

    item_num_15d BIGINT,
    brand_num_15d BIGINT,
    seller_num_15d BIGINT,
    cate1_num_15d BIGINT,
    cate2_num_15d BIGINT,
    cnt_days_15d BIGINT,

    item_num_30d BIGINT,
    brand_num_30d BIGINT,
    seller_num_30d BIGINT,
    cate1_num_30d BIGINT,
    cate2_num_30d BIGINT,
    cnt_days_30d BIGINT,

    item_num_60d BIGINT,
    brand_num_60d BIGINT,
    seller_num_60d BIGINT,
    cate1_num_60d BIGINT,
    cate2_num_60d BIGINT,
    cnt_days_60d BIGINT,

    item_num_90d BIGINT,
    brand_num_90d BIGINT,
    seller_num_90d BIGINT,
    cate1_num_90d BIGINT,
    cate2_num_90d BIGINT,
    cnt_days_90d BIGINT
)PARTITIONED BY (ds STRING) LIFECYCLE 60;


INSERT OVERWRITE TABLE user_click_beh_feature_ads PARTITION(ds=${bizdate})
SELECT user_id
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), ds, NULL))
FROM dw_user_item_click_log 
WHERE ds<='${bizdate}' AND ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd')
GROUP BY user_id;

SELECT * FROM user_click_beh_feature_ads WHERE ds='${bizdate}' LIMIT 10;
SELECT COUNT(*) FROM user_click_beh_feature_ads WHERE ds='${bizdate}'; --9677464
SELECT DISTINCT ds FROM user_click_beh_feature_ads WHERE ds>='20130401' AND ds<='20131001';

CREATE TABLE IF NOT EXISTS user_collect_beh_feature_ads(
    user_id STRING,

    item_num_3d BIGINT,
    brand_num_3d BIGINT,
    seller_num_3d BIGINT,
    cate1_num_3d BIGINT,
    cate2_num_3d BIGINT,
    cnt_days_3d BIGINT,

    item_num_7d BIGINT,
    brand_num_7d BIGINT,
    seller_num_7d BIGINT,
    cate1_num_7d BIGINT,
    cate2_num_7d BIGINT,
    cnt_days_7d BIGINT,

    item_num_15d BIGINT,
    brand_num_15d BIGINT,
    seller_num_15d BIGINT,
    cate1_num_15d BIGINT,
    cate2_num_15d BIGINT,
    cnt_days_15d BIGINT,

    item_num_30d BIGINT,
    brand_num_30d BIGINT,
    seller_num_30d BIGINT,
    cate1_num_30d BIGINT,
    cate2_num_30d BIGINT,
    cnt_days_30d BIGINT,

    item_num_60d BIGINT,
    brand_num_60d BIGINT,
    seller_num_60d BIGINT,
    cate1_num_60d BIGINT,
    cate2_num_60d BIGINT,
    cnt_days_60d BIGINT,

    item_num_90d BIGINT,
    brand_num_90d BIGINT,
    seller_num_90d BIGINT,
    cate1_num_90d BIGINT,
    cate2_num_90d BIGINT,
    cnt_days_90d BIGINT
)PARTITIONED BY (ds STRING) LIFECYCLE 60;


INSERT OVERWRITE TABLE user_collect_beh_feature_ads PARTITION(ds=${bizdate})
SELECT user_id
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), ds, NULL))
FROM dw_user_item_collect_log 
WHERE ds<='${bizdate}' AND ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd')
GROUP BY user_id;

SELECT COUNT(*) FROM user_collect_beh_feature_ads WHERE ds='${bizdate}'; --3218285

CREATE TABLE IF NOT EXISTS user_cart_beh_feature_ads(
    user_id STRING,

    item_num_3d BIGINT,
    brand_num_3d BIGINT,
    seller_num_3d BIGINT,
    cate1_num_3d BIGINT,
    cate2_num_3d BIGINT,
    cnt_days_3d BIGINT,

    item_num_7d BIGINT,
    brand_num_7d BIGINT,
    seller_num_7d BIGINT,
    cate1_num_7d BIGINT,
    cate2_num_7d BIGINT,
    cnt_days_7d BIGINT,

    item_num_15d BIGINT,
    brand_num_15d BIGINT,
    seller_num_15d BIGINT,
    cate1_num_15d BIGINT,
    cate2_num_15d BIGINT,
    cnt_days_15d BIGINT,

    item_num_30d BIGINT,
    brand_num_30d BIGINT,
    seller_num_30d BIGINT,
    cate1_num_30d BIGINT,
    cate2_num_30d BIGINT,
    cnt_days_30d BIGINT,

    item_num_60d BIGINT,
    brand_num_60d BIGINT,
    seller_num_60d BIGINT,
    cate1_num_60d BIGINT,
    cate2_num_60d BIGINT,
    cnt_days_60d BIGINT,

    item_num_90d BIGINT,
    brand_num_90d BIGINT,
    seller_num_90d BIGINT,
    cate1_num_90d BIGINT,
    cate2_num_90d BIGINT,
    cnt_days_90d BIGINT
)PARTITIONED BY (ds STRING) LIFECYCLE 60;


INSERT OVERWRITE TABLE user_cart_beh_feature_ads PARTITION(ds=${bizdate})
SELECT user_id
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), ds, NULL))
FROM dw_user_item_cart_log 
WHERE ds<='${bizdate}' AND ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd')
GROUP BY user_id;

SELECT COUNT(*) FROM user_cart_beh_feature_ads WHERE ds='${bizdate}'; --4368359

CREATE TABLE IF NOT EXISTS user_alipay_beh_feature_ads(
    user_id STRING,

    item_num_3d BIGINT,
    brand_num_3d BIGINT,
    seller_num_3d BIGINT,
    cate1_num_3d BIGINT,
    cate2_num_3d BIGINT,
    cnt_days_3d BIGINT,

    item_num_7d BIGINT,
    brand_num_7d BIGINT,
    seller_num_7d BIGINT,
    cate1_num_7d BIGINT,
    cate2_num_7d BIGINT,
    cnt_days_7d BIGINT,

    item_num_15d BIGINT,
    brand_num_15d BIGINT,
    seller_num_15d BIGINT,
    cate1_num_15d BIGINT,
    cate2_num_15d BIGINT,
    cnt_days_15d BIGINT,

    item_num_30d BIGINT,
    brand_num_30d BIGINT,
    seller_num_30d BIGINT,
    cate1_num_30d BIGINT,
    cate2_num_30d BIGINT,
    cnt_days_30d BIGINT,

    item_num_60d BIGINT,
    brand_num_60d BIGINT,
    seller_num_60d BIGINT,
    cate1_num_60d BIGINT,
    cate2_num_60d BIGINT,
    cnt_days_60d BIGINT,

    item_num_90d BIGINT,
    brand_num_90d BIGINT,
    seller_num_90d BIGINT,
    cate1_num_90d BIGINT,
    cate2_num_90d BIGINT,
    cnt_days_90d BIGINT
)PARTITIONED BY (ds STRING) LIFECYCLE 60;


INSERT OVERWRITE TABLE user_alipay_beh_feature_ads PARTITION(ds=${bizdate})
SELECT user_id
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-3,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-7,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-15,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-30,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-60,'dd'),'yyyyMMdd'), ds, NULL))

    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), item_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), brand_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), seller_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), cate1_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), cate2_id, NULL))
    ,COUNT(DISTINCT IF(ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd'), ds, NULL))
FROM dw_user_item_alipay_log 
WHERE ds<='${bizdate}' AND ds>=to_char(dateadd(to_date('${bizdate}','yyyyMMdd'),-90,'dd'),'yyyyMMdd')
GROUP BY user_id;

SELECT COUNT(*) FROM user_alipay_beh_feature_ads WHERE ds='${bizdate}'; --2981366