set search_path to food_sec;


create table gdp_energy_with_fs_indicators (
area_code_m49 varchar(3),
area varchar(50),
year_code varchar(8),
gdp_pc_ppp numeric,
childhood_stunting numeric,
childhood_overweight numeric,
anemia numeric,
dietary_energy numeric,
undernourished numeric,
primary key (area_code_m49, year_code))



insert into gdp_energy_with_fs_indicators
(area_code_m49,
area,
year_code,
gdp_pc_ppp,
childhood_stunting,
childhood_overweight,
anemia,
dietary_energy,
undernourished
)
select e.area_code_m49, e.area, g.year_code, g.gdp_pc_ppp,
g.childhood_stunting, g.childhood_overweight, g.anemia, e.dietary_energy, e.undernourished 
from gdp_stunting_overweight g
join energy_undernourished e on 
g.area_code_m49 = e.area_code_m49 and g.area = e.area and
g.year_code = e.derived_year 
order by e.area, e.year_code;

select count(*) from gdp_energy_with_fs_indicators



create table gdp_energy_fs_aggs (
area_code_m49 varchar(3),
area varchar(50),
avg_gdp_pc_ppp numeric,
avg_childhood_stunting numeric,
avg_childhood_overweight numeric,
avg_anemia numeric,
avg_dietary_energy numeric,
avg_undernourished numeric);


insert into gdp_energy_fs_aggs
(area_code_m49,
area,
avg_gdp_pc_ppp,
avg_childhood_stunting,
avg_childhood_overweight,
avg_anemia,
avg_dietary_energy,
avg_undernourished
)
select area_code_m49, area, round(avg(gdp_pc_ppp)::numeric, 2),
round(avg(childhood_stunting)::numeric, 2), round(avg(childhood_overweight)::numeric, 2), round(avg(anemia)::numeric, 2),
round(avg(dietary_energy)::numeric, 2), round(avg(undernourished)::numeric, 2)
from gdp_energy_with_fs_indicators
group by area_code_m49, area
order by area;
