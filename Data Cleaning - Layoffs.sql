-- DATA CLEANING
-- Copy the Raw data into the Staging table
-- Always work on the Staging data and not on the Raw data.

 -- 1) Remove Duplicates
 -- 2) Standardize the Data - Eg: Make sure spellings are same across the board
 -- 3) Null or Blank Values - Try Populating data based on values avail
 -- 4) Remove any not needed columns & rows - BUT usually we don't do this in real world proj
 -- 5) Standardizing the Data Formats - Correct any wrong data type assigned to a column
 
SELECT *
FROM layoffs_staging;
 
-- 1)
-- *) Find & Remove Duplicates
-- *) Find Duplicates USE WINDOW FUNC - ROW_NUMBER() AND PARTITION IT BY ALL COLUMNS 

-- Find Duplicates using ROW_NUMBER & CTE
 
WITH CTE1 AS
 (
 SELECT *,
 ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as RN
 FROM layoffs_staging
 ) 
SELECT * FROM CTE1
WHERE RN > 1;

-- Create Staging Table using the CTE data with Row Number Column 
 
DROP TABLE IF EXISTS layoffs_staging2;

CREATE TABLE layoffs_staging2 
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as RN
FROM layoffs_staging;

-- Check the Duplicates in Staging 2 Table

SELECT * FROM
layoffs_staging2
WHERE RN > 1;

-- Delete the Duplicate records

DELETE FROM
layoffs_staging2
WHERE RN > 1;
 
-- 2) Standardization is something that has to be done across the whole table - So check all columns
-- Standardizing the Data - Eg: Make sure spellings are same across the board
-- 1) Check For any spaces in Columns front & Back -> Use TRIM()
-- 2) Check For any duplicates with Spelling mistakes 
--    Make sure its same across board else when EDA they all will be a separate entity

-- Remove Spaces front and Back from column Company

UPDATE layoffs_staging2
SET company=TRIM(company);
 
-- There are entries in Industry column as Crypto | Crypto Currency | CryptoCurrency
-- Update all as Crypto so it is same across board
 
Update layoffs_staging2
SET industry='Crypto'
WHERE industry LIKE 'Crypto%';

SELECT Distinct(industry)
FROM
layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- There are entries in Country column as United States | United States.
-- Update all as United States so it is same across board

UPDATE layoffs_staging2
SET country='United States'
WHERE country LIKE 'United%' AND country NOT IN ('United Kingdom','United Arab Emirates');

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United%' AND country NOT IN ('United Kingdom','United Arab Emirates');

-- Date format -> Convert it in the format we need for EDA ( Column Date is in text data type over here )
-- Any date related column in String Data type like text use STR_TO_DATE() func

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y') 'Format Date'
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET date=str_to_date(`date`, '%m/%d/%Y');

-- CONVERT date column datatype
ALTER TABLE layoffs_staging2 MODIFY `date` date;

-- 3) NULL or BLANK Values - Decide what has to be done - Totally Null it or Add some other Values

-- Check Industry column - Null & Blank
-- Bally's Interactive will be Null bcas of Only one row

SELECT * FROM 
layoffs_staging2
WHERE company IN
(
SELECT company
FROM layoffs_staging2
WHERE industry IS NULL OR industry = ''
);

-- Always Update Blank Values to Null Before Populating with Values

UPDATE layoffs_staging2 
SET industry = NULL
WHERE 
industry IS NULL 
OR
industry ='';

-- Airbnb, Carvana & Juul has Industry populated in Few rows - So Populate the same in  empty or Null row
-- Only for Same location

-- Select & See company industries empty & not empty by default

SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2 
ON
	t1.company=t2.company
AND
	t2.location=t2.location
WHERE 
(
t1.industry IS NULL
AND
t2.industry IS NOT NULL);

-- Update Not Null Industry Values into the Null Industry rows for same Company & Location

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON
	t1.company = t2.company
AND
	t2.location = t2.location
SET 
	t1.industry = t2.industry
WHERE 
(
t1.industry IS NULL
AND
t2.industry IS NOT NULL);

-- Check changes for Airbnb, Carvana, Juul

SELECT *
FROM layoffs_staging2
WHERE company IN ('Airbnb','Carvana','Juul');

-- 4) Remove Rows / Columns from the Staging Data 
-- But We have to be sure 100 % When removing data from the Data base table

DELETE FROM
layoffs_staging2
WHERE 
	total_laid_off IS NULL
AND
	percentage_laid_off IS NULL;

-- Drop Additional Column after data cleaning is done if Any
-- In our case RN is an additional column that we created for removing Duplicates

ALTER TABLE layoffs_staging2 DROP COLUMN RN;

SELECT * FROM 
layoffs_staging2;