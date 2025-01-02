
-- In this project, we will start with data cleaning, followed by data analysis on the cleaned data to uncover valuable insights.

-- Part 1
-- Data Cleaning

SELECT * 
FROM global_job_layoffs.layoffs;

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens.

CREATE TABLE global_job_layoffs.layoffs_staging 
LIKE global_job_layoffs.layoffs;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways


-- 1. Remove Duplicates

-- First let's check for duplicates
SELECT *
FROM global_job_layoffs.layoffs_staging
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		global_job_layoffs.layoffs_staging;
        
SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- these are our real duplicates 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		global_job_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
    
-- let's just look at oda to confirm
SELECT *
FROM global_job_layoffs.layoffs_staging
WHERE company = 'Casper';

-- To delete the duplicates in MySQL we need to create another table. we will create table sateing2 with an extra row and then we can filty on row nums and delete which are greater than 1 or are duplicated. 

ALTER TABLE global_job_layoffs.layoffs_staging ADD row_num INT;

SELECT *
FROM global_job_layoffs.layoffs_staging;


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
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM layoffs;

select * 
from layoffs_staging2
where  row_num > 1; 
-- now that we have this we can delete rows were row_num is greater than 2

DELETE FROM global_job_layoffs.layoffs_staging2
WHERE row_num > 1;


-- 2. Standardize Data
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company); 


-- We noticed that Crypto has multiple different variations. We need to standardize that - let's say all to Crypto

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');


-- We have some "United States" and some "United States." with a period at the end. Let's standardize this.

SELECT DISTINCT country
FROM global_job_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM global_job_layoffs.layoffs_staging2
ORDER BY country;

-- Let's also fix the date columns:
SELECT *
FROM global_job_layoffs.layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM global_job_layoffs.layoffs_staging2;

-- We also have some null and empty rows, let's take a look at these

SELECT DISTINCT industry
FROM global_job_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM global_job_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM global_job_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

-- nothing wrong here
SELECT *
FROM global_job_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE global_job_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM global_job_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row. To populate this null values
SELECT *
FROM global_job_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM global_job_layoffs.layoffs_staging2
ORDER BY industry;



-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values

-- 4. remove any columns and rows we need to

SELECT *
FROM global_job_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM global_job_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM global_job_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM global_job_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM global_job_layoffs.layoffs_staging2;


-- Part 2
-- Now we are good with our data cleaning and our data is ready for analysis.  

-- Let's start our EDA ( Eplanatory Data Analyis). So explore the data and find trends or patterns or anything interesting like outliers

-- normally when we start the EDA process we should have some idea of what we're looking for
-- A- Some easy quiries 

SELECT *
FROM global_job_layoffs.layoffs_staging2;

SELECT MAX(total_laid_off)
FROM global_job_layoffs. layoffs_staging2; 

-- Looking at Percentage to see how big these layoffs were

SELECT MAX(percentage_laid_off), MIN(percentage_laid_off)
From global_job_layoffs.layoffs_staging2
WHERE percentage_laid_off IS NOT NULL; 

-- Which companies had 1 which is basically 100 percent of the company laid off
SELECT *
FROM global_job_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;
-- It appears that most of these are startups that went out of business during this period.

-- By ordering the data by funds_raised_millions, we can observe the scale of some of these companies.
SELECT *
FROM global_job_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- BritishVolt appears to be an EV company. Quibi raised about 2 billion dollars and still went under—that’s rough.



-- SOMEWHAT TOUGHER AND MOSTLY USING GROUP BY-----------------------------------------------------------------------------------

-- Companies with the largest individual layoffs

SELECT company, total_laid_off
FROM global_job_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 10;
-- This is just on a single day

-- Companies with the highest total layoffs
SELECT company, SUM(total_laid_off)
FROM global_job_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;


-- by location
SELECT location, SUM(total_laid_off)
FROM global_job_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- this it total in the past 3 years or in the dataset

SELECT country, SUM(total_laid_off)
FROM global_job_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM global_job_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 DESC;

SELECT industry, SUM(total_laid_off)
FROM global_job_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off)
FROM global_job_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;



-- More challenging queries------------------------------------------------------------------------------
 -- Previously, we examined companies with the most layoffs. Now, let's analyze this on a yearly basis. It's a bit more challenging.

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- We now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;

SELECT * 
FROM global_job_layoffs.layoffs_staging2; 
