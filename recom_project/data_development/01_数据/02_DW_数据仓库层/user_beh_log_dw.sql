--odps sql 
--********************************************************************--
--author:aliyun5858408693
--create time:2024-11-05 15:58:03
--********************************************************************--
CREATE TABLE IF  NOT EXISTS dw_user_item_click_log (
    user_id STRING COMMENT '用户id'
    ,item_id STRING COMMENT '商品id'
    ,brand_id STRING COMMENT '品牌id'
    ,seller_id STRING COMMENT '商家id'
    ,cate1_id STRING COMMENT '类目1id'
    ,cate2_id STRING COMMENT '类目2id'
    ,op_time STRING COMMENT '点击时间'
)PARTITIONED BY (ds STRING COMMENT '日期分区') LIFECYCLE 60;

CREATE TABLE IF  NOT EXISTS dw_user_item_collect_log (
    user_id STRING COMMENT '用户id'
    ,item_id STRING COMMENT '商品id'
    ,brand_id STRING COMMENT '品牌id'
    ,seller_id STRING COMMENT '商家id'
    ,cate1_id STRING COMMENT '类目1id'
    ,cate2_id STRING COMMENT '类目2id'
    ,op_time STRING COMMENT '点击时间'
)PARTITIONED BY (ds STRING COMMENT '日期分区') LIFECYCLE 60;

CREATE TABLE IF  NOT EXISTS dw_user_item_cart_log (
    user_id STRING COMMENT '用户id'
    ,item_id STRING COMMENT '商品id'
    ,brand_id STRING COMMENT '品牌id'
    ,seller_id STRING COMMENT '商家id'
    ,cate1_id STRING COMMENT '类目1id'
    ,cate2_id STRING COMMENT '类目2id'
    ,op_time STRING COMMENT '点击时间'
)PARTITIONED BY (ds STRING COMMENT '日期分区') LIFECYCLE 60;

CREATE TABLE IF  NOT EXISTS dw_user_item_alipay_log (
    user_id STRING COMMENT '用户id'
    ,item_id STRING COMMENT '商品id'
    ,brand_id STRING COMMENT '品牌id'
    ,seller_id STRING COMMENT '商家id'
    ,cate1_id STRING COMMENT '类目1id'
    ,cate2_id STRING COMMENT '类目2id'
    ,op_time STRING COMMENT '点击时间'
)PARTITIONED BY (ds STRING COMMENT '日期分区') LIFECYCLE 60;

ALTER TABLE dw_user_item_click_log SET lifecycle 180;
ALTER TABLE dw_user_item_collect_log SET lifecycle 180;
ALTER TABLE dw_user_item_cart_log SET lifecycle 180;
ALTER TABLE dw_user_item_alipay_log SET lifecycle 180;

insert overwrite table dw_user_item_click_log partition(ds) 
select user_id, t1.item_id, brand_id, seller_id, cate1, cate2, vtime, ds
from(
    select user_id, item_id, vtime, to_char(to_date(vtime, 'yyyy-mm-dd hh:mi:ss'), 'yyyymmdd') as ds
    from(user_item_beh_log)
    where action='click'
)t1 join(
    select item_id, brand_id, seller_id, split_part(category, '-', 1) as cate1, split_part(category, '-', 2) as cate2
    from item_dim
)t2 on t1.item_id=t2.item_id; 

SELECT * FROM dw_user_item_click_log WHERE ds='${bizdate}' LIMIT 100;
SHOW PARTITIONS dw_user_item_click_log; -- 20130401-20131001
SELECT count(user_id) FROM dw_user_item_click_log WHERE ds=${bizdate}; --10935708
SELECT count(DISTINCT user_id) FROM dw_user_item_click_log WHERE ds=${bizdate}; --2566225,故20130930平均每人点击四个商品

insert overwrite table dw_user_item_collect_log partition(ds) 
select user_id, t1.item_id, brand_id, seller_id, cate1, cate2, vtime, ds
from(
    select user_id, item_id, vtime, to_char(to_date(vtime, 'yyyy-mm-dd hh:mi:ss'), 'yyyymmdd') as ds
    from(user_item_beh_log)
    where action='collect'
)t1 join(
    select item_id, brand_id, seller_id, split_part(category, '-', 1) as cate1, split_part(category, '-', 2) as cate2
    from item_dim
)t2 on t1.item_id=t2.item_id; 

SELECT count(user_id) FROM dw_user_item_collect_log WHERE ds=${bizdate}; --292677
SELECT count(DISTINCT user_id) FROM dw_user_item_collect_log WHERE ds=${bizdate}; --187104

insert overwrite table dw_user_item_cart_log partition(ds) 
select user_id, t1.item_id, brand_id, seller_id, cate1, cate2, vtime, ds
from(
    select user_id, item_id, vtime, to_char(to_date(vtime, 'yyyy-mm-dd hh:mi:ss'), 'yyyymmdd') as ds
    from(user_item_beh_log)
    where action='cart'
)t1 join(
    select item_id, brand_id, seller_id, split_part(category, '-', 1) as cate1, split_part(category, '-', 2) as cate2
    from item_dim
)t2 on t1.item_id=t2.item_id; 

SELECT count(user_id) FROM dw_user_item_cart_log WHERE ds=${bizdate}; --336918
SELECT count(DISTINCT user_id) FROM dw_user_item_cart_log WHERE ds=${bizdate}; --202173

insert overwrite table dw_user_item_alipay_log partition(ds) 
select user_id, t1.item_id, brand_id, seller_id, cate1, cate2, vtime, ds
from(
    select user_id, item_id, vtime, to_char(to_date(vtime, 'yyyy-mm-dd hh:mi:ss'), 'yyyymmdd') as ds
    from(user_item_beh_log)
    where action='alipay'
)t1 join(
    select item_id, brand_id, seller_id, split_part(category, '-', 1) as cate1, split_part(category, '-', 2) as cate2
    from item_dim
)t2 on t1.item_id=t2.item_id; 

SELECT count(user_id) FROM dw_user_item_alipay_log  WHERE ds=${bizdate}; --155894
SELECT count(DISTINCT user_id) FROM dw_user_item_alipay_log  WHERE ds=${bizdate}; --126453
SELECT * FROM dw_user_item_alipay_log WHERE ds='20130701' LIMIT 100;