# Netflix Movies Data Cleaning and Analysis using SQL

## Overview

This project involves extracting, cleaning, and analyzing a dataset of Netflix movies and shows. The dataset was obtained from Kaggle and loaded into a PostgreSQL database using Python for further analysis using SQL.

## Data Source

- **Dataset**: [Netflix Movies and TV Shows](https://www.kaggle.com/datasets/shivamb/netflix-shows) (CSV format).
- **Tools Used**: Python, Pandas, JupyterNotebooks, Visual Studio Code, pgAdmin 4 for PostgreSQL, SQL.

## Table of Contents

- [Data Preparation](#Data-Preparation)
- [Data Cleaning and Transformation](#Data-Cleaning-and-Transformation)
- [Data Analysis](#Data-Analysis)
- [Insights](#Insights)

## Data Preparation

1. **CSV to PostgreSQL**: The data was initially in CSV format and was loaded into a PostgreSQL database using Python.

    ```python
    # Load CSV data
    import pandas as pd
    df = pd.read_csv("Data/netflix_titles.csv")
    df.head()

    #Connect to PostgreSQL and Insert Data
    import sqlalchemy as sal
    # PostgreSQL database credentials
    db_user = 'Gio'
    db_password = 'Gio4everful.'
    db_host = 'localhost'  # or your database host
    db_port = '5432'  # or your database port
    db_name = 'Netflix_Movies'
    
    # Create SQLAlchemy engine to connect to PostgreSQL database
    engine = sal.create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    
    # Save DataFrame to PostgreSQL
    try:
        df.to_sql('Netflix_Movies', engine, if_exists='append', index=False)
        print("Data saved to PostgreSQL database successfully!")
    except Exception as e:
        print(f"Error saving data to PostgreSQL: {str(e)}")
    ```

## Data Cleaning and Transformation

The data cleaning and transformation process involved several steps, including handling duplicates, splitting columns, and type conversion.

1. **Original Table Exploration**

    ```sql
    SELECT *
    FROM "Netflix_Movies"
    ORDER BY title;
    ```

2. **Create a Copy of the Table**

    ```sql
    CREATE TABLE "Netflix_Movies_2" AS
    SELECT *
    FROM "Netflix_Movies";
    ```

3. **Find and Delete Duplicates**

    ```sql
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
    ```

4. **Find and Delete Duplicate Titles (Case-insensitive)**

    ```sql
    SELECT *
    FROM "Netflix_Movies_2"
    WHERE (upper(title), type) IN ( 
        SELECT upper(title), type 
        FROM "Netflix_Movies_2"
        GROUP BY upper(title), type
        HAVING COUNT(*) > 1
    )
    ORDER BY title;

    WITH CTE AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY upper(title), type ORDER BY show_id) AS row_num
        FROM "Netflix_Movies_2"
    )
    DELETE FROM "Netflix_Movies_2"
    WHERE show_id IN (
        SELECT show_id
        FROM CTE
        WHERE row_num > 1
    );
    ```

5. **Create New Tables for Director, Cast, Country, and Listed In**

    ```sql
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
    ```

6. **Date Type Conversion and Clean Data Creation**

    ```sql
    SELECT show_id, type, title, CAST(date_added AS DATE) AS date_added
    FROM "Netflix_Movies_2";

    WITH CTE AS (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY title, "type" ORDER BY show_id) AS row_num
        FROM "Netflix_Movies_2"
    )
    SELECT show_id, "type", title, COALESCE(CAST(date_added AS DATE), '2020-01-01') AS date_added, release_year,
    rating, CASE WHEN duration is null THEN rating ELSE duration END AS duration, description
    INTO Netflix_clean
    FROM CTE;
    ```

## Data Analysis

1. **Count of Movies and TV Shows by Director**

    ```sql
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
    ```

2. **Country with Highest Number of Comedy Movies**

    ```sql
    SELECT NC.country, N.type, listed_in, COUNT(listed_in) as count_genre
    FROM netflix_country NC
    INNER JOIN netflix_listed_in LI ON NC.show_id = LI.show_id
    INNER JOIN netflix_clean N ON LI.show_id = N.show_id
    WHERE listed_in LIKE '%Comedies%' AND "type" = 'Movie'
    GROUP BY NC.country, N.type, listed_in
    ORDER BY count_genre DESC
    LIMIT 1;
    ```

3. **Director with Maximum Number of Movies Released Each Year**

    ```sql
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
            ROW_NUMBER() OVER (PARTITION BY year_added ORDER BY movie_count DESC) AS rank_num
        FROM DirectorMovieCount
    )
    SELECT year_added, director, movie_count
    FROM RankedDirectors
    WHERE rank_num = 1
    ORDER BY year_added;
    ```

4. **Average Duration of Movies in Each Genre**

    ```sql
    SELECT NL.listed_in, ROUND(AVG(CAST(REPLACE(duration, 'min', '') AS INTEGER))) AS avg_duration_minutes
    FROM netflix_listed_in NL
    INNER JOIN netflix_clean NC ON NL.show_id = NC.show_id
    WHERE type = 'Movie'
    GROUP BY NL.listed_in
    ORDER BY avg_duration_minutes DESC;
    ```

5. **Directors Who Have Created Both Horror and Comedy Movies**

    ```sql
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
    ```

## Insights

- **Directors Creating Both Movies and TV Shows**: 83 directors are creating both Movies and TV Shows among them are: Abhishek Chaubey, Thomas Astruc, Weica Wang.
- **Top Country for Comedy Movies**: Uniteed States, with 685 Comedy Movies.
- **Yearly Director Performance**:
- ![image](https://github.com/user-attachments/assets/2705809b-39b1-4b2a-ba29-ca2b87319b88)
- **Average Duration by Genre**:
- ![image](https://github.com/user-attachments/assets/db047bae-af73-4c7d-a3f7-3b8f74362729)

- **Directors Creating Horror and Comedy Movies**: 55 directors are creating Horror and Comedy Movies among them are: Hardik Mehta, Anggy Umbara, Adam Egypt Mortimer.
