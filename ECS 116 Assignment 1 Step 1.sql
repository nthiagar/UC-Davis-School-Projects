set search_path to food_sec;

create table gdp_stunting_overweight (
area_code_m49 varchar(3),
area varchar(50),
year_code varchar(8),
childhood_overweight numeric,
anemia numeric,
gdp_pc_ppp numeric,
childhood_stunting numeric,
primary key (area_code_m49, year_code))



insert into gdp_stunting_overweight
(area_code_m49,
area,
year_code,
childhood_overweight,
anemia,
gdp_pc_ppp,
childhood_stunting
)
select o.area_code_m49, o.area, o.year_code,
o.value as childhood_overweight,
a.value as anemia,
g.value as gdp_pc_ppp,
s.value as childhood_stunting
from africa_fs_ac o, africa_fs_ac a, africa_fs_ac g, africa_fs_ac s
where s.area_code_m49 = o.area_code_m49
and g.area_code_m49 = o.area_code_m49
and a.area_code_m49 = o.area_code_m49
--
and s.year_code = o.year_code
and g.year_code = o.year_code
and a.year_code = o.year_code
--
and o.item_code = '21041'
and a.item_code = '21043'
and g.item_code = '22013'
and s.item_code = '21025'
order by area, year_code;





