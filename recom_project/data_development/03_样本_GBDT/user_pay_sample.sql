--@exclude_input=tmall_rec_project.user_id_number
--@exclude_input=tmall_rec_project.brand_top500_alipay_dim
--@exclude_input=tmall_rec_project.dw_user_item_alipay_log
--odps sql 
--********************************************************************--
--author:aliyun5858408693
--create time:2024-11-06 16:43:31
--********************************************************************--
create table if not exists user_pay_sample_pos(
    user_id string,
    brand_id string
)partitioned by (ds string) lifecycle 60;

insert overwrite table user_pay_sample_pos partition(ds=${bizdate})
select t1.user_id, t1.brand_id
from(
    select distinct user_id, brand_id
    from dw_user_item_alipay_log 
    where ds>${bizdate} and ds<=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),+7, 'dd'),'yyyymmdd')
)t1 join(
    select brand_id
    from brand_top500_alipay_dim 
)t2 ON t1.brand_id=t2.brand_id;

SELECT count(brand_id)
from user_pay_sample_pos 
WHERE ds=${bizdate};

create table if not exists user_pay_sample(
    user_id string,
    brand_id string,
    label bigint
)partitioned by(ds string) lifecycle 60;

insert overwrite table user_pay_sample partition(ds=${bizdate})
select t1.neg_user_id as user_id, t1.brand_id, 0 as label
from(
    select distinct brand_id, t2.user_id as neg_user_id
    from(
        select trans_array(2, ',', user_id, brand_id, rand_neg) as (user_id, brand_id, rand_neg)
        from(
            select user_id, brand_id, concat(
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint),',',
                cast(rand()*10000000 as bigint)
            )as rand_neg
            from user_pay_sample_pos
            where ds=${bizdate}
        )
    )t1 join(
        select user_id, number
        from user_id_number 
    )t2 on t1.rand_neg=t2.number
)t1 left anti join(
    select user_id, brand_id
    from user_pay_sample_pos
    where ds=${bizdate}
)t2 on t1.neg_user_id=t2.user_id and t1.brand_id=t2.brand_id
union all
select user_id, brand_id, 1 as label
from user_pay_sample_pos
where ds=${bizdate};

SELECT COUNT(*) from user_pay_sample WHERE ds='20130701' GROUP BY brand_id;
SELECT COUNT(*) from user_pay_sample WHERE ds='20130701' and label=0;
SELECT * from user_pay_sample_pos  WHERE ds='20130701' LIMIT 100;