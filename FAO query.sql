/*TARGETED TECHNOLOGY INSTITUTE
  UN-FAO PROJECT 
By :- LENSA SHASHO

--RELESE 1
-------SPRINT 1
------======story 1 */
create database WorldFoodProduction;

use WorldFoodProduction

select*from [dbo].[CO2_Data]
select*from [dbo].[Surface_Temperature_Anomaly]
select*from [dbo].[Maize_Production]
select*from [dbo].[Maize_Yields]

-----======story 2
select*from [dbo].[Rice_Production]
select*from [dbo].[Rice_Yields]
select*from [dbo].[Wheat_Production]
select*from [dbo].[Wheat_Yields]

----SPRINT 2

create view vw_Countries
as (
select Distinct Country
from 
(
select Country from [dbo].[CO2_Data]
INTERSECT 
select Country from [dbo].[Rice_Production]
INTERSECT 
select Country from [dbo].[Rice_Yields]
INTERSECT
select Country from [dbo].[Maize_Production]
INTERSECT
select Country from [dbo].[Maize_Yields]
INTERSECT
select Country from [dbo].[Wheat_Production]
INTERSECT
select Country from [dbo].[Wheat_Yields]
INTERSECT 
select Country from [dbo].[Surface_Temperature_Anomaly]
) a )


select *from [dbo].[vw_Countries]
order by Country

--==========DELETE COUNTRIES NOT IN vw_Countries=======
delete from [dbo].[CO2_Data]
where Country not in (select Country from [dbo].[vw_Countries])

delete from [dbo].[Maize_Production]
where Country not in (select Country from [dbo].[vw_Countries])

delete from [dbo].[Maize_Yields]
where Country not in (select Country from [dbo].[vw_Countries])

delete from [dbo].[Rice_Production]
where Country not in (select Country from [dbo].[vw_Countries])

delete from [dbo].[Rice_Yields]
where Country not in (select Country from [dbo].[vw_Countries])

delete from [dbo].[Surface_Temperature_Anomaly]
where Country not in (select Country from [dbo].[vw_Countries])

delete from [dbo].[Wheat_Production]
where Country not in (select Country from [dbo].[vw_Countries])

delete from [dbo].[Wheat_Yields]
where Country not in (select Country from [dbo].[vw_Countries])

---=======GET RANGE OF YEAR COMOMON TO ALL TABLES=========
create view vw_CommonYear
as(
select Year from [dbo].[CO2_Data]
INTERSECT 
select Year from [dbo].[Rice_Production]
INTERSECT 
select Year from [dbo].[Rice_Yields]
INTERSECT
select Year from [dbo].[Maize_Production]
INTERSECT
select Year from [dbo].[Maize_Yields]
INTERSECT
select Year from [dbo].[Wheat_Production]
INTERSECT
select Year from [dbo].[Wheat_Yields]
INTERSECT 
select Year from [dbo].[Surface_Temperature_Anomaly]
)

select min (year)[MIN], max(year)[MAX] from [dbo].[vw_CommonYear]

----=====DELETE DATA NOT BETWEEN (1961 AND 2017)==========
delete from [dbo].[CO2_Data]
where year not between 1961 and 2017

delete from [dbo].[Surface_Temperature_Anomaly]
where year not between 1961 and 2017

delete from [dbo].[Maize_Production]
where year not between 1961 and 2017

delete from [dbo].[Rice_Production]
where year not between 1961 and 2017

delete from [dbo].[Rice_Yields]
where year not between 1961 and 2017

delete from [dbo].[Wheat_Production]
where year not between 1961 and 2017

delete from [dbo].[Wheat_Yields]
where year not between 1961 and 2017

-----=====FUNCTION TO GROUP DATA BEFORRE AND AFTER THE YEAR 2000====================

CREATE FUNCTION udf_yearRange (@year int) 
RETURNS varchar(5)
AS 
	BEGIN
	Declare @Range varchar (5)
	set @Range=(select case when @year >2000 then 'Late' 
	else 'Early' end Range)
	
		RETURN @Range
	END

----=====SELECT COUNTRIES HAVING DATA ONLY AFTER THE YEAR 2000===============

select * from
(select distinct Country, [dbo].[udf_yearRange](year) [Range] from [dbo].[Maize_Production] where year <2000) MY
right outer join 
(select distinct Country, [dbo].[udf_yearRange](year) [Range] from [dbo].[Maize_Production] where year >2000) MP
on MY.Country=MP.Country
where MY.Country is null

union

select * from
(select distinct Country, [dbo].[udf_yearRange](year) [Range] from [dbo].[Maize_Yields] where year <2000) MY
right outer join 
(select distinct Country, [dbo].[udf_yearRange](year) [Range] from [dbo].[Maize_Yields] where year >2000) MP
on MY.Country=MP.Country
where MY.Country is null

union

select * from
(select distinct Country, [dbo].[udf_yearRange](year) [Range] from [dbo].[Rice_Production] where year <2000) MY
right outer join 
(select distinct Country, [dbo].[udf_yearRange](year) [Range] from [dbo].[Rice_Yields] where year >2000) MP
on MY.Country=MP.Country
where MY.Country is null

union

select * from
(select distinct Country, [dbo].[udf_yearRange](year) [Range] from [dbo].[Wheat_Production]  where year <2000) MY
right outer join 
(select distinct Country, [dbo].[udf_yearRange](year) [Range] from [dbo].[Wheat_Yields] where year >2000) MP
on MY.Country=MP.Country
where MY.Country is null

----=====DELETE COUNTRIES HAVING DATA ONLY AFTER THE YEAR 2000===============

delete from [dbo].[CO2_Data]
where country in ('Sudan','Denmark','Serbia','Montenegro', 'Luxembourg','Belgium')

delete from [dbo].[Maize_Production]
where country in ('Sudan','Denmark','Serbia','Montenegro', 'Luxembourg','Belgium')

delete from [dbo].[Maize_Yields]
where country in ('Sudan','Denmark','Serbia','Montenegro', 'Luxembourg','Belgium')

delete from [dbo].[Surface_Temperature_Anomaly]
where country in ('Sudan','Denmark','Serbia','Montenegro', 'Luxembourg','Belgium')

delete from [dbo].[Wheat_Production]
where country in ('Sudan','Denmark','Serbia','Montenegro', 'Luxembourg','Belgium')

delete from [dbo].[Wheat_Yields]
where country in ('Sudan','Denmark','Serbia','Montenegro', 'Luxembourg','Belgium')

create table WFP_FAO (
iso_code varchar(3), Country varchar (50),year int, co2 float,[population] float,Surface_Temp_Anomaly float,Maize_Prod float,
Maize_yield float,Rice_Prod float,Rice_yield float,Wheat_Prod float,Wheat_yield float )


insert into WFP_FAO (iso_code,Country,year,co2,[population],Surface_Temp_Anomaly,Maize_Prod,Maize_yield,
Rice_Prod,Rice_yield,Wheat_Prod,Wheat_yield)
select CO.[iso_code],CO.[Country],CO.[year],CO.[co2],CO.[population],
ST.[Surface_Temp_Anomaly],MP.Production_Tones[Maize_Prod],
MY.Yield_Hectogram_Per_Hectare[Maize_yield],RP.Production_Tones[Rice_Prod],
RY.Yield_Hectogram_Per_Hectare[Rice_yield],
WP.Production_Tones[Wheat_Prod],WY.Yield_Hectogram_Per_Hectare[Wheat_yield] from [dbo].[CO2_Data] CO 
 inner join [dbo].[Surface_Temperature_Anomaly] ST
   on CO.Country=ST.Country
  and CO.year=ST.Year   
inner join [dbo].[Maize_Production] MP
   on CO.Country= MP.Country 
  and CO.year=MP.Year
inner join [dbo].[Maize_Yields] MY
    on CO.Country=MY.Country
   and CO.year=MY.Year
inner join [dbo].[Rice_Production] RP
    on CO.Country=RP.Country
   and CO.year=RP.Year
 inner join [dbo].[Rice_Yields] RY
    on CO.Country=RY.Country
   and CO.year=RY.Year
  inner join [dbo].[Wheat_Production] WP
    on CO.Country=WP.Country
   and CO.year=WP.Year
  inner join [dbo].[Wheat_Yields] WY
    on CO.Country=WY.Country
   and CO.year=WY.Year
   order by iso_code,year
   ;


   select*from WFP_FAO


