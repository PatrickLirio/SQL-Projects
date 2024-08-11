-- Exploratory Data Analysis

SELECT *
FROM world_layoffs.layoffs_staging2;

-- Let's get to know what is the max  total_laid_off and percentage_laid_off
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM world_layoffs.layoffs_staging2;

-- Let's check what companies who has 100% percentage_laid_off
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
-- ORDER BY total_laid_off DESC;
ORDER BY funds_raised_millions DESC;

-- Let's look for company with combining all the laid_offs
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Since amazon is the top 1 based on the first query let's check
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Amazon%';

-- Let's try to know what industry have the most laid off
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Let's try to know what country have the most laid off
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Let's try to know what year have the most laid off
SELECT YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Let's try to know what stage have the most laid off
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Lets' look the date range
SELECT MIN(`date`),  MAX(`date`)
FROM world_layoffs.layoffs_staging2;

-- Let's try to know what month have the most laid off
SELECT SUBSTRING(`date`,1,7) AS `MONTH` , SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- Let's try to know the rolling total of laid off employee every month by using CTE common table expression.
-- we will be able to see how many employee were laid off from the march 2020 to the specific month we want to know 
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH` , SUM(total_laid_off)  AS Total_off
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, Total_off, SUM(Total_off) OVER(ORDER BY `MONTH`) AS Rolling_Total--  basically you want to know the month, number of laid_off to that month, summation of previous month rolling total to next month
FROM Rolling_Total;

-- Let's see how many were laid off per year to these companies 
SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC; -- sorted it by the SUM(total_laid_off) in descending order

-- Let's try to do it by using CTE
-- we put partition per year and a ranking per year, 
-- so example these are the top 1 companies that had most laid_off employees per year
-- what we next do is to get the 5 companies per year 
WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
( 
SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
-- ORDER BY Ranking ASC
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;

-- What I did here is add another column to total all the total_laid_off column that is at the top 5 
-- and I put it as total_laid_off_per year. This is to know which year has the most laid off 
-- employees based from the top 5 companies of each years.
WITH Company_Year AS (
    SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_laid_off
    FROM world_layoffs.layoffs_staging2
    GROUP BY company, YEAR(`date`)
),
Company_Year_Rank AS (
    SELECT company, years, total_laid_off, 
    DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
    FROM Company_Year
    WHERE years IS NOT NULL
),
Total_Laid_Off_Per_Year AS (
    SELECT years, SUM(total_laid_off) AS total_laid_off_per_year
    FROM Company_Year
    GROUP BY years
)
SELECT cyr.company, cyr.years, cyr.total_laid_off, cyr.Ranking, tlo.total_laid_off_per_year
FROM Company_Year_Rank cyr
JOIN Total_Laid_Off_Per_Year tlo 
	ON cyr.years = tlo.years
WHERE cyr.Ranking <= 5;














