-- 1. Remove Duplicates
-- 2. Standardize tha Data
-- 3. Null values or blank values
-- 4. Remove any columns or Rows

select*
from layoffs;

create table layoffs_stagging
like layoffs;

insert into layoffs_stagging
select*
from layoffs;

select*
from layoffs_stagging;

select*,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date',
stage,country,funds_raised_millions) as row_num
from layoffs_stagging;

with duplicate_cte as(
select*,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date',
stage,country,funds_raised_millions) as row_num
from layoffs_stagging
)
select*
from duplicate_cte
where row_num > 1;

select*
from layoffs_stagging
where company = '&Open';

-- create another table like the original one
-- create a cte where we use row_number partition by all the cases from the second table then use select from the second table 
-- when you finish u use select* from example_cte where row_num > 1  to see duplicates
 -- to delete the duplicates use another table in this example it's layoffs_stagging2

CREATE TABLE layoffs_stagging2 (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_stagging2
select*,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date',
stage,country,funds_raised_millions) as row_num
from layoffs_stagging;

select*
from layoffs_stagging2
where row_num > 1;

delete
from layoffs_stagging2
where row_num > 1;

select*
from layoffs_stagging2;

-- standardizing data

select distinct(company)
from layoffs_stagging2;

select company, trim(company) -- trim to remove the blanks
from layoffs_stagging2;

update layoffs_stagging2
set company = trim(company);

select distinct(industry)
from layoffs_stagging2;

update layoffs_stagging2
set industry = 'Crypto'
where industry like 'Crypto%';

select*
from layoffs_stagging2
where industry like 'Crypto%';

select distinct(country)
from layoffs_stagging2
order by 1;

select*
from layoffs_stagging2
where country like 'united states%';

update layoffs_stagging2
set country = 'United states'
where country like 'USA%';

select `date`
from layoffs_stagging2
order by 1;

update layoffs_stagging2
set `date` = str_to_date (`date`,'%m/%d/%Y');

alter table layoffs_stagging2
modify column `date` date;

select*
from layoffs_stagging2
where total_laid_off IS NULL;

update layoffs_stagging2
set industry = NULL
where industry = '';

select*
from layoffs_stagging2
where company like 'Airbnb%';

select t1.industry, t2.industry
from layoffs_stagging2 as t1
join layoffs_stagging2 as t2
   on t1.company = t2.company
where (t1.industry IS NULL or t1.industry = '')
and t2.industry IS NOT NULL;

update layoffs_stagging2 as t1
join layoffs_stagging2 as t2
    on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry IS NULL
and t2.industry IS NOT NULL;

select*
from layoffs_stagging2;

select*
from layoffs_stagging2
where total_laid_off IS NULL AND percentage_laid_off IS NULL;

delete
from layoffs_stagging2
where total_laid_off IS NULL AND percentage_laid_off IS NULL;

select*
from layoffs_stagging2;

alter table layoffs_stagging2
drop column row_num;

select distinct(industry)
from layoffs_stagging2;

-- exploratory Data Analysis

select*
from layoffs_stagging2;

select YEAR(`date`) , sum(total_laid_off)
from layoffs_stagging2
group by YEAR(`date`) 
order by 1 DESC;

select* from layoffs_stagging2;

select company , YEAR(`date`) as `Month`,sum(total_laid_off) as total_off,
dense_rank() over(partition by YEAR(`date`) order by sum(total_laid_off) DESC) AS RANKING
from layoffs_stagging2
where YEAR(`date`) IS NOT NULL
group by company, YEAR(`date`);

with company_ranking as(
select company , YEAR(`date`) as `Month`,sum(total_laid_off) as total_off,
dense_rank() over(partition by YEAR(`date`) order by sum(total_laid_off) DESC) AS RANKING
from layoffs_stagging2
where YEAR(`date`) IS NOT NULL
group by company, YEAR(`date`)
)
select*
from company_ranking
where RANKING <=5;





