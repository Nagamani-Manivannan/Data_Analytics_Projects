use layoffs_db;
select *
from layoffs; 
create table layoffs_staging
like layoffs;

Insert layoffs_staging
select *
from layoffs;

with duplicate_cte as 
(
select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off,'date', stage, country,funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num >1;


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

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off,'date', stage, country,funds_raised_millions) as row_num
from layoffs_staging;

DELETE FROM layoffs_staging2
WHERE row_num >= 2;

SET SQL_SAFE_UPDATES = 0; 

SELECT * 
FROM layoffs_staging2;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';
-- nothing wrong here
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

update layoffs_staging2
set industry = "NULL"
where industry = '';

select *
from layoffs_staging2
where industry = "null"
or industry = ""
order by industry;

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry=t2.industry
where t1.industry is null
and t2.industry is not null;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

SELECT *
FROM layoffs_staging2;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_staging2
modify column `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

alter table layoffs_staging2
drop row_num;

select *
from layoffs_staging2;







