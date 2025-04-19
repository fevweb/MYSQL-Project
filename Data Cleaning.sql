create database world_layoffs
use world_layoffs

SELECT * from layoffs

--1.Remove duplicates 
--2. Standardize the Data
--3. Null Values
--4. Remove Any Columns or Rows


CREATE table layoffs_staging
like layoffs



insert layoffs_staging
SELECT * from layoffs

select *,
row_number() over
(PARTITION BY company, industry, total_laid_off,percentage_laid_off,'date') AS row_num
from layoffs_staging

--checks duplicates if 2 then it is
with duplicate_cte AS(
    select *,
row_number() over
(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
from layoffs_staging
)
SELECT * from duplicate_cte
where row_num > 1


SELECT * FROM
layoffs_staging
where company ='Casper'


CREATE TABLE `layoffs_staging2` (
  `company` text DEFAULT NULL,
  `location` text DEFAULT NULL,
  `industry` text DEFAULT NULL,
  `total_laid_off` int(14) DEFAULT NULL,
  `percentage_laid_off` text DEFAULT NULL,
  `date` text DEFAULT NULL,
  `stage` text DEFAULT NULL,
  `country` text DEFAULT NULL,
  `funds_raised_millions` int(21) DEFAULT NULL,
  `row_num` int(14) DEFAULT NULL
);




INSERT into layoffs_staging2
  select *,
row_number() over
(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
from layoffs_staging

DELETE
FROM layoffs_staging2
where row_num > 1

SELECT *
FROM layoffs_staging2

-- Standardizing data

SELECT company, TRIM(company)
FROM layoffs_staging2

UPDATE layoffs_staging2
set company = TRIM(company)

SELECT DISTINCT(industry)
from layoffs_staging2
order by 1

SELECT * from 
layoffs_staging2
where industry like 'Crypto%'

UPDATE layoffs_staging2
set industry = 'Crypto'
where industry = 'Crypto Currency'
OR
industry = 'CryptoCurrency'


SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER by 1

UPDATE layoffs_staging2
set country = 'United States'
where country = 'United States.'

select `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
from layoffs_staging2


UPDATE layoffs_staging2
set  `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
from layoffs_staging2
ORDER by 1;


ALTER table layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT * FROM layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging2
where industry is NULL
OR industry ='' 


SELECT *
FROM layoffs_staging2
where company = 'Airbnb'


SELECT t1.industry,t2.industry
from layoffs_staging2 t1
  join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL


UPDATE layoffs_staging2
set industry = NULL
WHERE industry =''

UPDATE layoffs_staging2 t1
 join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry IS NULL
AND t2.industry IS NOT NULL;



SELECT *
FROM layoffs_staging2
where company = "Bally's Interactive"


DELETE FROM
layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL


ALTER TABLE layoffs_staging2
DROP COLUMN row_num

SELECT * FROM
layoffs_staging2