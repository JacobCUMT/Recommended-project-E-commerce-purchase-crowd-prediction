--odps sql 
--********************************************************************--
--author:aliyun5858408693
--create time:2024-11-06 13:28:59
--********************************************************************--
create table if not exists brand_top500_alipay_dim (
    brand_id string,
    alipay_num bigint
)LIFECYCLE 60;

insert OVERwrite table brand_top500_alipay_dim
select brand_id, alipay_num
from (
    select brand_id, count(DISTINCT user_id) as alipay_num
    from dw_user_item_alipay_log
    where ds<=${bizdate} and ds>to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
        and brand_id is not null
    group by brand_id
)t1 order by alipay_num desc limit 500
;

create table if not exists brand_cate2_dim (
    brand_id string,
    cate2 string
)lifecycle 60;

insert overwrite table brand_cate2_dim
select brand_id, cate2_id
from (
    select brand_id, cate2_id, ROW_NUMBER() OVER(PARTITION BY brand_id ORDER BY num desc) AS number
    from (
        select brand_id, cate2_id, count(distinct user_id) as num
        from(
            select t1.brand_id, user_id, cate2_id
            from (
                select user_id, brand_id, cate2_id
                from dw_user_item_alipay_log
                where ds<${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
            )t1 join (
                select brand_id
                from brand_top500_alipay_dim
            )t2 on t1.brand_id=t2.brand_id
        )
        group by brand_id, cate2_id
    )
)where number <=2;


create table if not exists brand_top1w_alipay_dim (
    brand_id string,
    alipay_num bigint
)LIFECYCLE 60;

insert OVERwrite table brand_top1w_alipay_dim
select brand_id, alipay_num
from (
    select brand_id, count(DISTINCT user_id) as alipay_num
    from dw_user_item_alipay_log
    where ds<=${bizdate} and ds>to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
        and brand_id is not null
    group by brand_id
)t1 order by alipay_num desc limit 10000
;
