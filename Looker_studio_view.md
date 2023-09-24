Year-wise view: Seeing if there is a trend of popularity increase/decrease over the years
```sql
select 
    start as year,
    total_votes
from dtc-de-course-388001.onepiece.year_wise_summary
order by start asc;
```

We are seeing that Popularity dipped from 2000 to 2003 and then has stayed consistent over the years till 2020
![Year Wise Trend](./images/Popularity_by_year.png)


