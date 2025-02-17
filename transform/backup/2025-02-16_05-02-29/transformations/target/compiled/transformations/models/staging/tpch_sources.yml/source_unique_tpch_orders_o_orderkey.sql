
    
    

select
    o_orderkey as unique_field,
    count(*) as n_records

from snowflake_sample_data.tpch_sf1.orders
where o_orderkey is not null
group by o_orderkey
having count(*) > 1


