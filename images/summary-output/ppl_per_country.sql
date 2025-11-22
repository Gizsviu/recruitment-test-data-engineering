select
    ct.country_name as Country,
    count(p.personal_id) as Population

from people as p

left join cities as c
    on c.city_id = p.city_id

left join regions as r
    on r.region_id = c.region_id

left join countries as ct
    on ct.country_id = r.country_id

group by 
    ct.country_name