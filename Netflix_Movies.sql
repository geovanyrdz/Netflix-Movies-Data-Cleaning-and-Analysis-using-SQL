-- Original Table
SELECT *
FROM "Netflix_Movies"
ORDER BY title;

-- Copy the Table
CREATE TABLE "Netflix_Movies_2" AS
SELECT *
FROM "Netflix_Movies";

-- Find ID duplicates
SELECT show_id, count(*)
FROM "Netflix_Movies_2"
GROUP BY show_id
HAVING COUNT(*) > 1;

-- Delete duplicates
DELETE FROM "Netflix_Movies_2"
WHERE ctid NOT IN (
    SELECT MIN(ctid)
    FROM "Netflix_Movies_2"
    GROUP BY show_id
);

-- Find Movie titles and its types with duplicates (case-insensitive)

SELECT *
FROM "Netflix_Movies_2"
WHERE (upper(title), type) IN ( 
	SELECT upper(title), type 
	FROM "Netflix_Movies_2"
	GROUP BY upper(title), type
	HAVING COUNT(*) > 1
)
ORDER BY title;

-- Delete duplicates titles and its types

WITH CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY upper(title), type ORDER BY show_id) AS row_num
    FROM "Netflix_Movies_2"
)
-- Delete rows where row_num is greater than 1, meaning they are duplicates
DELETE FROM "Netflix_Movies_2"
WHERE show_id IN (
    SELECT show_id
    FROM CTE
    WHERE row_num > 1
);
 	

--New tables for  director, cast,  country, listed in, 

SELECT show_id, trim(unnest(string_to_array(director, ','))) AS director
INTO Netflix_director
FROM "Netflix_Movies_2";

SELECT show_id, trim(unnest(string_to_array("cast", ','))) AS cast
INTO Netflix_cast
FROM "Netflix_Movies_2";

SELECT show_id, trim(unnest(string_to_array(country, ','))) AS country
INTO Netflix_country
FROM "Netflix_Movies_2";

SELECT show_id, trim(unnest(string_to_array(listed_in, ','))) AS listed_in
INTO Netflix_listed_in
FROM "Netflix_Movies_2";

--Date type conversion for date added


SELECT show_id, type, title, CAST(date_added AS DATE) AS date_added
FROM "Netflix_Movies_2";

SELECT *
FROM "netflix_country"


--Populate missing values in country column.

INSERT INTO netflix_country
SELECT show_id, m.country
	FROM "Netflix_Movies_2" nm
	INNER JOIN (
	SELECT director, country
	FROM "netflix_country" nc
	INNER JOIN netflix_director nd
	ON nc.show_id = nd.show_id
	GROUP BY director, country
	ORDER BY director ) m
	ON nm.director = m.director
	WHERE nm.country is null;


--Populate missing values in duration and date_added columns and create a new table with the clean data.


WITH CTE AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY title, "type" ORDER BY show_id) AS row_num
    FROM "Netflix_Movies_2"
)
SELECT show_id, "type", title, COALESCE(CAST(date_added AS DATE), '2020-01-01') AS date_added, release_year,
rating, CASE WHEN duration is null THEN rating ELSE duration END AS duration, description
INTO Netflix_clean
FROM CTE




-- DATA ANALYSIS SECTION

/* 1. For each director, count the number of movies and tv shows created by them in separate columns
for directors who have created tv shows and movies both. */

/*Count movies and TV shows for each director:*/

WITH DirectorCounts AS (
    SELECT 
        ND.director,
        SUM(CASE WHEN NC."type" = 'Movie' THEN 1 ELSE 0 END) AS total_movies,
        SUM(CASE WHEN NC."type" = 'TV Show' THEN 1 ELSE 0 END) AS total_tv_shows
    FROM Netflix_clean NC
    INNER JOIN Netflix_director ND
    ON NC.show_id = ND.show_id
    GROUP BY ND.director
)
SELECT *
FROM DirectorCounts
WHERE total_movies > 0 AND total_tv_shows > 0
ORDER BY director;

--2. which country has highest number of comedy movies 


-- Triple Inner join to get the data from country, listed_in and clean tables.
	
SELECT NC.country, N.type, listed_in, COUNT(listed_in) as count_genre
FROM netflix_country NC
INNER JOIN netflix_listed_in LI ON NC.show_id = LI.show_id
INNER JOIN netflix_clean N ON LI.show_id = N.show_id
WHERE listed_in LIKE '%Comedies%' AND "type" = 'Movie'
GROUP BY NC.country, N.type, listed_in
ORDER BY count_genre DESC
LIMIT 1;

--3. for each year (as per date added to netflix), which director has maximum number of movies released

WITH DirectorMovieCount AS (
SELECT EXTRACT(YEAR FROM NC.date_added) AS year_added, ND.director, COUNT(NC.show_id) AS movie_count
FROM netflix_clean NC
INNER JOIN netflix_director ND ON NC.show_id = ND.show_id
WHERE NC.type = 'Movie'
GROUP BY year_added, ND.director
ORDER BY MOVIE_COUNT DESC
),

RankedDirectors AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY year_added ORDER BY movie_count DESC) AS rank_num  -- Rank directors per year by movie count
    FROM DirectorMovieCount
)

-- Get the top director for each year
SELECT year_added, director, movie_count
FROM RankedDirectors
WHERE rank_num = 1  -- Filter to get only the top-ranked director per year
ORDER BY year_added;



--4. what is average duration of movies in each genre

SELECT NL.listed_in, ROUND(AVG(CAST(REPLACE(duration, 'min', '') AS INTEGER))) AS avg_duration_minutes
FROM netflix_listed_in NL
INNER JOIN netflix_clean NC ON NL.show_id = NC.show_id
WHERE type = 'Movie'
GROUP BY NL.listed_in
ORDER BY avg_duration_minutes DESC

--5.  find the list of directors who have created horror and comedy movies both.
-- display director names along with number of comedy and horror movies directed by them 

SELECT ND.director,
    SUM(CASE WHEN NL.listed_in = 'Comedies' THEN 1 ELSE 0 END) AS total_comedy_movies,
    SUM(CASE WHEN NL.listed_in = 'Horror Movies' THEN 1 ELSE 0 END) AS total_horror_movies
FROM netflix_clean NC
INNER JOIN netflix_director ND ON NC.show_id = ND.show_id
INNER JOIN netflix_listed_in NL ON ND.show_id = NL.show_id
WHERE NC.type = 'Movie'
GROUP BY ND.director
HAVING SUM(CASE WHEN NL.listed_in = 'Comedies' THEN 1 ELSE 0 END) > 0
AND SUM(CASE WHEN NL.listed_in = 'Horror Movies' THEN 1 ELSE 0 END) > 0;
 	
