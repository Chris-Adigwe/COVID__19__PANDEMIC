-- Question 1 -- Retrieve the total confirmed, death, and recovered cases
SELECT SUM(confirmed)total_confirmed, SUM(deaths)total_death, 
	   SUM(recovered)recovered_cases 
FROM public.covid_19_data

-- Question 2 -- Retrieve the total confirmed, deaths and recovered cases for the first quarter 
-- of each year of observation

SELECT 
		EXTRACT(QUARTER FROM observationdate) quarter,
		SUM(confirmed)total_confirmed, 
		SUM(deaths)total_death, 
		SUM(recovered)recovered_cases 
		
FROM public.covid_19_data

WHERE EXTRACT(QUARTER FROM observationdate) = 1


GROUP BY EXTRACT(QUARTER FROM observationdate)


-- Question 3 -- Retrieve a summary of all the records. This should include the following 
-- information for each country:
-- ● The total number of confirmed cases 
-- ● The total number of deaths
-- ● The total number of recoveries

SELECT 
		country,
		SUM(confirmed)total_confirmed, 
		SUM(deaths)total_death, 
		SUM(recovered)recovered_cases 
		
FROM public.covid_19_data


GROUP BY country

-- Question 4  Retrieve the percentage increase in the number of death cases from 2019 to 
-- 2020.

WITH A AS (
SELECT 
		EXTRACT(YEAR FROM observationdate) AS year,
		SUM(deaths) AS "2019_deaths"
		
FROM public.covid_19_data
WHERE EXTRACT(YEAR FROM observationdate) = 2019
GROUP BY EXTRACT(YEAR FROM observationdate)),

B AS (
SELECT 
		EXTRACT(YEAR FROM observationdate) AS year,
		SUM(deaths) AS "2020_deaths"
		
FROM public.covid_19_data
WHERE EXTRACT(YEAR FROM observationdate) = 2020
GROUP BY EXTRACT(YEAR FROM observationdate))

SELECT  
		B."2020_deaths",
		A."2019_deaths",
		ROUND(((CAST(B."2020_deaths" AS NUMERIC) - CAST(A."2019_deaths" AS NUMERIC))/CAST(A."2019_deaths" AS NUMERIC))*100,2) AS percentage_increase 
FROM A
INNER JOIN B
ON A.year + 1 = B.year


-- Question  5- Retrieve information for the top 5 countries with the highest confirmed cases.

SELECT  country, 
		sum(confirmed) AS confirmed_cases
		
FROM    covid_19_data
GROUP BY country
ORDER BY 2 DESC
LIMIT 5


-- Question 6 -- Compute the total number of drop (decrease) or increase in the confirmed 
-- cases from month to month in the 2 years of observation

SELECT  EXTRACT(YEAR FROM observationdate) AS YEAR,
 		EXTRACT(MONTH FROM observationdate) AS Month,
		sum(confirmed) AS PREV,
		LEAD(sum(confirmed::NUMERIC), 1) OVER(ORDER BY  EXTRACT(YEAR FROM observationdate) ASC) CURRENT,
		ROUND(((LEAD(sum(confirmed::NUMERIC), 1) OVER(ORDER BY  EXTRACT(YEAR FROM observationdate) ASC) - sum(confirmed::NUMERIC))/sum(confirmed::NUMERIC))*100,2) AS NEW
		
FROM    covid_19_data
GROUP BY EXTRACT(YEAR FROM observationdate), EXTRACT(MONTH FROM observationdate)
ORDER BY 1 ASC, 2 ASC
		





