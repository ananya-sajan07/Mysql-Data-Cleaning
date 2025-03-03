-- Data Cleaning

-- 1. Make a copy of the raw data
-- 2. Remove Duplicates
-- 3. Standardize data - like make spellings and all same throughout
-- 4. Remove null values or blank values
-- 5. Remove any unnecessary columns.

SELECT *
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
	ROW_NUMBER() 
    OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
	ROW_NUMBER() 
    OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	-- give all column name above as we want duplicates to have all columns same 
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1; -- now all the results we get are the duplicates
-- if row_num =3, it means that row is the 3rd occurrence of the same row. That means
-- there is 1 original and 2 duplicates of the row.
SELECT *
FROM layoffs_staging
WHERE company = ' E Inc.'; -- checking a random company from previous output

WITH duplicate_cte AS
(
SELECT *,
	ROW_NUMBER() 
    OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
    'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

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

SELECT *
FROM layoffs_staging2
WHERE row_num>1;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
	ROW_NUMBER() 
    OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
    'date', stage, country, funds_raised_millions) AS row_num	
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1; -- deleting duplicates

SELECT*
FROM layoffs_staging2
WHERE row_num > 1;

SELECT*
FROM layoffs_staging2; -- now this table doesn't have any duplicates.

-- -------------------------------------------------------------------------------------
-- 3. Standardizing Data
-- Finding issues and fixing it.

-- Removing extra whitespaces at beginning or end
SELECT company, TRIM(company) 
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);
-- --------------------------------------------------------------------------------------
-- making Crypto, Crypto Currency, CryptoCurrency to Crypto
SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;
-- In `ORDER BY 1`, the **`1` refers to the first column in the SELECT clause**, which 
-- in this case is `industry`.

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2;
-- -------------------------------------------------------------------------------------

-- Cheking Country column for issues
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1; -- there is a 'United States.' instead of 'United States', see code below

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';
-- -------------------------------------------------------------------------------------

-- Change date format from TEXT to DATE
SELECT `date`,
STR_TO_DATE (`date`, '%m/%d/%Y') -- conversion (col_name, 'format of existing date')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE (`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- -------------------------------------------------------------------------------------
-- 4. Remove null values or blank values
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL; 
		-- if only one (total_laid_off) Null still useful.

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';
-- going to populate from filled to null value. Eg. in industry column, one is 'travel' 
-- and the other is null. So we populate 'travel' to null

-- Finding which companies can be populated
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '') AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- -------------------------------------------------------------------------------------
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%'; -- doesn't have an other value to populate it to null cell. 

-- -------------------------------------------------------------------------------------
-- 5. Remove any unnecessary columns.
SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL; 

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL; 