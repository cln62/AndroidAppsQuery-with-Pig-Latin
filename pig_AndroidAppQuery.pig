
reviews = LOAD 'directory of the JSON file' USING JsonLoader('reviewerID:chararray, asin:chararray, reviewerName:chararray,helpful:{(list:int)}, reviewText:chararray, overall:float, summary:chararray, unixReviewTime:int, reviewTime:chararray');
reviews_final = FOREACH reviews GENERATE *, GetYear(ToDate(reviewTime,'MM DD, YYYY')) AS reviewYear;

store reviews_final into 'directory of the file you want to save';


--query 1. To find top 100k, 3* or above rated products

top_reviewers = FILTER reviews_final BY overall >= 3.0;
top_100k_reviewers = LIMIT top_reviewers 100000;
top_100k_reviews = FOREACH top_100k_reviewers GENERATE reviewerID, asin, overall;
store top_100k_reviews into 'directory of the file you want to save';
 
-- query to check if the count of reviews = 100000

group_top_100k_reviews = GROUP top_100k_reviews ALL;
count_top_100k_reviews = FOREACH group_top_100k_reviews GENERATE COUNT(top_100k_reviews);
dump count_top_100k_reviews;


--query 2. To find reviews of distinct reviewers with 1* ratings

one_star_ratings = FILTER reviews_final BY overall == 1.0;
one_star_reviews = FOREACH one_star_ratings GENERATE reviewerName, reviewText;
group_one_star_reviews = GROUP one_star_reviews BY reviewerName;

--dump group_one_star_reviews;
store group_one_star_reviews into '/home/cloudera/Downloads/PIG/pig-output-files/group_one_star_reviews';


--query 3. To find reviews in range of years 2XXX to 2017.

records_2XXX_2017 = FILTER reviews_final BY reviewYear >= $year AND reviewYear <= 2017;
reviews_2XXX_2017 = FOREACH records_2XXX_2017 GENERATE reviewerID, asin, reviewText, reviewTime;
store reviews_2XXX_2017 into '/home/cloudera/Downloads/PIG/pig-output-files/reviews_2XXX_2017';


--query 4. To find reviews with average ratings above 3* in last X years

records_last_year = FILTER top_reviewers BY (2018 - reviewYear) <= $X;
reviews_last__X_year = FOREACH records_last_year GENERATE reviewerID, asin, reviewText, reviewTime;
store reviews_last__X_year into '/home/cloudera/Downloads/PIG/pig-output-files/reviews_last__X_year';


--query 5. To find top 100k summaries with 1* ratings. 

top_100k_onestar_ratings = LIMIT one_star_ratings 100000;
top_100k_onestar_summaries = FOREACH top_100k_onestar_ratings GENERATE reviewerID, asin, summary, overall;
store top_100k_onestar_summaries into '/home/cloudera/Downloads/PIG/pig-output-files/top_100k_onestar_summaries';


--query 6. To display reviewers ID of customers who have reviewed more than 20 products

revID_products = FOREACH reviews GENERATE reviewerID,asin;
group_revID = GROUP revID_products BY reviewerID;
revID_prod_count = FOREACH group_revID GENERATE group AS revID, COUNT(revID_products.asin) AS prodcnt;
revID_greater_20_prod = FILTER revID_prod_count BY prodcnt > 20;
revID_20 = FOREACH revID_greater_20_prod GENERATE revID;
store revID_20 into '/home/cloudera/Downloads/PIG/pig-output-files/revID_20';


--query 7. To find number of products bought in any given year

reviews_given_year = FILTER reviews_final BY reviewYear == $givenYear;
group_products_that_year = GROUP reviews_given_year ALL;
product_count_year = FOREACH group_products_that_year GENERATE COUNT(reviews_given_year);
store product_count_year into '/home/cloudera/Downloads/PIG/pig-output-files/product_count_year';

--query 8. To find top 100k summaries with substring 'quality'

all_summaries = FOREACH reviews GENERATE summary;
summary_with_quality = FILTER all_summaries BY (summary matches '.*quality.*');
top_100k_summary_quality = LIMIT summary_with_quality 100000;
store top_100k_summary_quality  into '/home/cloudera/Downloads/PIG/pig-output-files/top_100k_summary_quality ';






