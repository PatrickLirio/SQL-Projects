-- Data Cleaning

-- steps in data cleaning
-- 1. Remove Duplicate
-- 2.Standardize the Data
-- 3.Null values or Blank values
-- 4. Remove any columns or rows

SELECT COUNT(*) 
FROM world_layoffs.layoffs;


-- Copy the raw data into staging table to ensure that the raw data is safe for future use
CREATE TABLE layoffs_staging
LIKE world_layoffs.layoffs;

SELECT COUNT(*) 
FROM world_layoffs.layoffs_staging;

-- insert all the data to the new table
INSERT layoffs_staging
SELECT * 
FROM world_layoffs.layoffs;

-- REMOVING OF DUPLICATE DATA 
-- used window function to ensure we are seing the unique rows
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM world_layoffs.layoffs_staging;

-- used Common table Expression to see the duplicated rows
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Confirming if it was really a duppplicate
SELECT * 
FROM world_layoffs.layoffs_staging
WHERE company = 'Cazoo';

-- Now that we found the duplicates we need to REMOVE it now
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;
-- since the target table duplicate_cte of the DELETE is not updatable, we need to create another staging table
-- where we can put all the dupplicate data into that table 

-- Heres how to do it 
	-- right click the "layoffs_staging" table and click "Copy to clipboard" and choose the "create statement"
    -- and paste it here by doing CTRL+V 
    -- change the name of the table into layoffs_staging2
    -- make sure the name and datatypes are correct and add "row_num" column
    -- now run it.
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- check table
SELECT * 
FROM world_layoffs.layoffs_staging2;
	 -- Since it's empty INSERT the data from "layoffs_staging" to "layoffs_staging2" by using the windows function
     -- to ensure the column count will match value. 
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY 
company, location, industry, 
total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging;

 -- Get again the duplicate rows 
SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

-- Now DELETE it
DELETE 
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

-- Check if it is successfully deleted
SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

-- Check how many rows are left after the dupplicate rows are deleted. its 2536 
SELECT COUNT(*) 
FROM world_layoffs.layoffs_staging2;


-- STANDARDIZING DATA
-- For company column  
SELECT company, TRIM(company)
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);-- just taking off the white space 

-- For industry column  
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- Now that we found out that the "Crypto", "Crypto Currency", and "Crypto Currency" are the same
-- we need to update it so that it will be putted into one column as "Crypto Currency"

-- let's see all the data of "Crypto", "Crypto Currency", and "Crypto Currency"
SELECT *-- COUNT(*)
FROM world_layoffs.layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- let's update it now
UPDATE  world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- check
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY 1; 

-- For location column
SELECT DISTINCT location
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- For country column
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- update the error
UPDATE  world_layoffs.layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- For Date
SELECT `date`
FROM world_layoffs.layoffs_staging2;

-- to format the date into date from text 
SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')  -- to transform date into date format use this and put the parameters(date column which is set as string, Year/month/date)
FROM world_layoffs.layoffs_staging2;

-- Now lets update it
UPDATE  world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y') ;

-- Check
SELECT * 
FROM world_layoffs.layoffs_staging2;

-- since when you check the data type of check at the navigator information is still text but it is now in a date format
-- we need  to change it to date
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;


-- For total_laid_off
-- check the nulls in total_laid_off column and percentage_laid_off
SELECT COUNT(*)
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- if we wanted to delete the total_laid_off column and percentage_laid_off that are nuill
DELETE
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- check as well the null and no value in the industry 
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- we need to put a label in the industry column of air bnb because it is blank
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Carvana';

-- update the industry of Airbnb that has a 30 total_laid_off
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Travel'
WHERE company = 'Airbnb' 
AND industry = '';

-- Show all the company where industry are blank or null 
-- t1.industry are all columns that are blank or null and is related to t1.company, 
-- while t2.industry are all columns that are not blank and related to t2.company
-- Basically what this code does is that it shows the comparison of 2 tables (self join is used)
SELECT t1.company, t2.company, t1.industry, t2.industry
FROM world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2 -- Here we are doing a self join
	ON t1.company = t2.company -- this is where the 2 columns have their values and common to them
WHERE(t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Let's convert all the blank spaces into null first
UPDATE  world_layoffs.layoffs_staging2 
SET industry = NULL
WHERE industry = '';

-- this is how to update it all in one without manually updating it per row
UPDATE  world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2 
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- we need to remove the row_num we added earlier
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

-- FINAL CHECK
SELECT *
FROM world_layoffs.layoffs_staging2; 
SELECT COUNT(*)
FROM world_layoffs.layoffs_staging2; 
 




