-- Data Cleaning

-- steps in data cleaning
-- 1. Remove Duplicate
-- 2.Standardize the Data
-- 3.Null values or Blank values
-- 4. Remove any columns

SELECT * 
FROM world_layoffs.layoffs;


-- Copy the raw data into staging table
CREATE TABLE layoffs_staging
LIKE world_layoffs.layoffs;

SELECT * 
FROM world_layoffs.layoffs_staging;

-- insert all the data to the new table
INSERT layoffs_staging
SELECT * 
FROM world_layoffs.layoffs;

-- LET'S DO THE REMOVING OF DUPLICATE DATA 
-- used window function 
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM world_layoffs.layoffs_staging;

-- used Common table Expression
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

