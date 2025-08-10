SELECT 
    *
FROM
    layoffs;



/*  Goals:
>> Remove duplicates
>> standerdize the data
>> null values or blank values
>> remove any unnecessary columns

*/
CREATE TABLE layoffs_stagging LIKE layoffs;

SELECT 
    *
FROM
    layoffs_stagging;
insert layoffs_stagging
select * from layoffs;


SELECT 
    *
FROM
    layoffs_stagging;
    
/*so far a duplicate of raw data table is ready 
 for removing duplicates, we create a columns 'row_num' partition by every columns 
 and look for the duplicates in them, row_num give us unique ID for each row, which makes it easier to identify duplicates*/


select * ,
row_number() over (partition by company,location,industry,total_laid_off,'date') as row_num
from layoffs_stagging;

-- here we create a CTE to filter on,if row_num>1


with cte_dulicate as
(
select * ,
row_number() over (partition by company,location,industry,total_laid_off,'date',stage,country,funds_raised_millions) as row_num
from layoffs_stagging
)
select * from cte_dulicate where row_num >1;

SELECT 
    *
FROM
    layoffs_stagging
WHERE
    company = 'ola';

SELECT 
    *
FROM
    layoffs_stagging
WHERE
    company = 'carta';


-- we can not update or delete data using CTE in mysql, but its possible in MS sql server
-- so for removing the duplicates , we create another table,

CREATE TABLE `layoffs_stagging_2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `row_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;




SELECT 
    *
FROM
    layoffs_stagging_2;

-- insert CTE data (duplicates) into it
insert into layoffs_stagging_2
select * ,
row_number() over (partition by company,location,industry,total_laid_off,'date',stage,country,funds_raised_millions) 
as row_num
from layoffs_stagging;
SELECT 
    *
FROM
    layoffs_stagging_2;
SELECT 
    *
FROM
    layoffs_stagging_2
WHERE
    row_num > 1;

-- now delete the duplicates

DELETE FROM layoffs_stagging_2 
WHERE
    row_num > 1;

SELECT 
    *
FROM
    layoffs_stagging_2
WHERE
    row_num > 1; -- all the duplicate data is deleted now
SELECT 
    *
FROM
    layoffs_stagging_2;


-- standerdizing the data --> removing the spaces , fixing lowercases and uppercases

SELECT company, TRIM(company) from layoffs_stagging_2;
UPDATE layoffs_stagging_2 
SET 
    company = TRIM(company);

-- >> similar companies with different names, giving them one name >>( crypto=crypto currency)
SELECT 
    *
FROM
    layoffs_stagging_2;
SELECT DISTINCT
    industry
FROM
    layoffs_stagging_2
-- ORDER BY 1; -- order by 1st column
;
SELECT 
    industry
FROM
    layoffs_stagging_2
WHERE
    industry LIKE '%crypto%';

UPDATE layoffs_stagging_2 
SET 
    industry = 'crypto'
WHERE
    industry LIKE '%crypto%';
SELECT DISTINCT
    industry
FROM
    layoffs_stagging_2;


SELECT 
    *
FROM
    layoffs_stagging_2;-- >> checking on the table, so far so good, all the changes made so far

SELECT DISTINCT
    country
FROM
    layoffs_stagging_2;

SELECT DISTINCT
    country, TRIM(TRAILING '.' FROM country) as cleaned_data_country
FROM
    layoffs_stagging_2
ORDER BY 1;

-- update the column
UPDATE layoffs_stagging_2 
SET 
    country = TRIM(TRAILING '.' FROM country)
WHERE
    country LIKE 'United States%';
-- now  check the column
SELECT DISTINCT
    country
FROM
    layoffs_stagging_2;
 
-- changing date from text formate to date formate

SELECT 
    `date`
FROM
    layoffs_stagging_2;
SELECT 
    `date`, STR_TO_DATE(`date`, '%m/%d/%Y') AS new_date
FROM
    layoffs_stagging_2;

UPDATE layoffs_stagging_2 
SET 
    `date` = STR_TO_DATE(`date`, '%m/%d/%Y');


alter table layoffs_stagging_2
modify column `date` date;

SELECT 
    *
FROM
    layoffs_stagging_2;-- > checking on the table...so far so good

SELECT 
    *
FROM
    layoffs_stagging_2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

SELECT 
    total_laid_off, percentage_laid_off
FROM
    layoffs_stagging_2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

SELECT 
    *
FROM
    layoffs_stagging_2
WHERE
    industry IS NULL OR industry = '';

SELECT 
    *
FROM
    layoffs_stagging_2
WHERE
   -- company = 'Airbnb'
    industry='travel';

update  layoffs_stagging_2
set industry=null
where industry='';

/*select t1.industry,t2.industry from layoffs_stagging_2 t1 join layoffs_stagging_2 t2 on 
t1.company=t2.company
where(t1.industry is null or t1.industry='' )
and t2.industry is not null; 

update layoffs_stagging_2 t1 join layoffs_stagging_2 t2 on 
t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null  
and t2.industry is not null; */

-- now deleting all the rows where total_laid_off, percentage_laid_off is Null

delete
FROM
    layoffs_stagging_2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

SELECT 
    *
FROM
    layoffs_stagging_2;
    
-- now in the last delete the row_num table, we dont need that anymore    
  Alter table  layoffs_stagging_2
  drop column row_num;
  -- here we have clean data, we can work further more on it for more exploratory analysis.
             
             /* ======== All Done, End of this Project ============ */
   
   -- PROJECT 2 >> === Exploratory Data Analysis ====
   
   
   
   SELECT 
    *
FROM
    layoffs_stagging_2;
    -- total max
  SELECT max(total_laid_off ), max(percentage_laid_off )  FROM layoffs_stagging_2;  
  
  select * from layoffs_stagging_2 where percentage_laid_off=1
 order by total_laid_off desc;
-- by company
   select company,sum(total_laid_off) from layoffs_stagging_2
   group by company order by 2;
   -- by country
  select country,sum(total_laid_off) from layoffs_stagging_2
   group by country ; 
  -- by year 
   select sum(total_laid_off), year(`date`) from layoffs_stagging_2
   group by year(`date`);
   
 /*  select sum(total_laid_off), month(`date`) from layoffs_stagging_2
   group by month(`date`); */
   
   -- by Month
 select substring(`date`, 1,7) monthly, sum(total_laid_off) from layoffs_stagging_2
 group by substring(`date`, 1,7);
 
   
   select company,sum(total_laid_off), year(`date`) as yearly from layoffs_stagging_2
   group by  company, year(`date`);
  
  -- ranking companies based on laid off, highest will be rank #1
   -- using CTE
   
   with company_ranking as(
    select company,sum(total_laid_off) total_off, year(`date`) as yearly from layoffs_stagging_2
   group by  company, year(`date`)
   )
   select * , dense_rank() over (partition by yearly order by total_off) com_ranking
   from company_ranking
   where total_off is not null
   order by com_ranking;
   
   
   
   
   
   
   
   