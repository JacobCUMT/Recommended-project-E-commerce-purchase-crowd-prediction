--odps sql 
--********************************************************************--
--author:aliyun5858408693
--create time:2024-11-05 09:00:37
--********************************************************************--
create table if not exists user_item_beh_log(
    item_id STRING,
    user_id STRING,
    action STRING,
    vtime STRING
)LIFECYCLE 30;
ALTER TABLE user_item_beh_log SET lifecycle 180;

select * from user_item_beh_log limit 100;

SELECT MAX(vtime),MIN(vtime),COUNT(DISTINCT item_id),COUNT(DISTINCT user_id),COUNT(*) FROM user_item_beh_log;
SELECT COUNT(DISTINCT user_id) FROM user_item_beh_log;  --9774184

SELECT COUNT(t1.item_id)
FROM(
    SELECT item_id
    FROM user_item_beh_log 
    WHERE action='alipay'
)t1 JOIN (
    SELECT item_id
    FROM item_dim 
    WHERE brand_id='b13402'
)t2 ON t1.item_id=t2.item_id;

SELECT COUNT(t1.item_id)
FROM(
    SELECT item_id
    FROM user_item_beh_log 
    WHERE action='click'
)t1 JOIN (
    SELECT item_id
    FROM item_dim 
    WHERE brand_id='b13402'
)t2 ON t1.item_id=t2.item_id;

SELECT user_id, vtime
FROM (
    SELECT item_id, user_id, vtime
    FROM user_item_beh_log
    WHERE action='click'
)t1 JOIN (
    SELECT item_id
    FROM item_dim 
    WHERE brand_id='b13402'
)t2 ON t1.item_id=t2.item_id;