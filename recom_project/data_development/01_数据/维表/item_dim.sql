--odps sql 
--********************************************************************--
--author:aliyun5858408693
--create time:2024-11-04 17:43:38
--********************************************************************--
create table if not exists item_dim(
    item_id string,
    title string,
    pict_url STRING,
    category STRING,
    brand_id STRING,
    seller_id STRING
)LIFECYCLE 90;

SELECT title from item_dim WHERE brand_id = 'b13402';