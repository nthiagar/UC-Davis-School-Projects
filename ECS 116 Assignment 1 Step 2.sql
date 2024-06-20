set search_path to food_sec;


create table energy_undernourished (
area_code_m49 varchar(3),
area varchar(50),
year_code varchar(8),
dietary_energy numeric,
undernourished numeric,
derived_year varchar(4),
primary key (area_code_m49, year_code))



insert into energy_undernourished
(area_code_m49,
area,
year_code,
dietary_energy,
undernourished
)

select d.area_code_m49, d.area, d.year_code,
d.value as dietary_energy,
u.value as anemia
from africa_fs_ac d, africa_fs_ac u
where d.area_code_m49 = u.area_code_m49
--
and d.year_code = u.year_code
--
and d.item_code = '21010'
and u.item_code = '210041'
order by area, year_code;




update energy_undernourished
set derived_year = ((cast(substring(year_code, 1, 4) as int)) + (cast(substring(year_code, 5, 4) as int)))/2

select * from energy_undernourished;

