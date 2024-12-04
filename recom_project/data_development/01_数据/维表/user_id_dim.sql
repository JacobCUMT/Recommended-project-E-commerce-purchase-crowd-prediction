--odps sql 
--********************************************************************--
--author:aliyun5858408693
--create time:2024-11-07 11:43:27
--********************************************************************--
create table if not exists user_id_number(
    user_id string,
    number bigint
)lifecycle 60;

insert overwrite table user_id_number
select user_id, ROW_NUMBER() over(order by rnd desc) as number
from(
    select user_id, RAND() as rnd
    from(
        select distinct user_id
        from user_item_beh_log
    )
);

select * from user_id_number limit 100;