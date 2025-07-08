// 4.1 Create Stage & Load Raw (JSON)
  
// First step: Load Raw JSON
CREATE OR REPLACE stage MANAGE_DB.EXTERNAL_STAGES.JSONSTAGE
     url='s3://bucketsnowflake-jsondemo';

CREATE OR REPLACE file format MANAGE_DB.FILE_FORMATS.JSONFORMAT
    TYPE = JSON;
        
CREATE OR REPLACE table OUR_FIRST_DB.PUBLIC.JSON_RAW (
    raw_file variant);
    
COPY INTO OUR_FIRST_DB.PUBLIC.JSON_RAW
    FROM @MANAGE_DB.EXTERNAL_STAGES.JSONSTAGE
    file_format= MANAGE_DB.FILE_FORMATS.JSONFORMAT
    files = ('HR_data.json');
    
SELECT * FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


// 4.2 Parsing & Analyze (JSON)

// Selecting attribute/column
SELECT RAW_FILE:city FROM OUR_FIRST_DB.PUBLIC.JSON_RAW

SELECT $1:first_name FROM OUR_FIRST_DB.PUBLIC.JSON_RAW

// Selecting attribute/column - formattted
SELECT RAW_FILE:first_name::string AS first_name  FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT RAW_FILE:id::int AS id  FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:id::int AS id,  
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:last_name::STRING AS last_name,
    RAW_FILE:gender::STRING AS gender

FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

// Handling nested data
SELECT RAW_FILE:job AS job  FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


// 4.3 Handling Nested Data (JSON)

// Handling nested data  
SELECT RAW_FILE:job AS job  FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
      RAW_FILE:job.salary::INT AS salary
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:job.salary::INT AS salary,
    RAW_FILE:job.title::STRING AS title
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

// Handling arreys

SELECT
    RAW_FILE:prev_company AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT
    RAW_FILE:prev_company[1]::STRING AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT
    ARRAY_SIZE(RAW_FILE:prev_company) AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:id::int AS id,  
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:prev_company[0]::STRING AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
UNION ALL 
SELECT 
    RAW_FILE:id::int AS id,  
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:prev_company[1]::STRING AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
ORDER BY id

  
// 4.4 Dealing with Hierarchy (JSON)

SELECT 
    RAW_FILE:spoken_languages AS spoken_languages
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT * FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
     array_size(RAW_FILE:spoken_languages) AS spoken_languages
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
     RAW_FILE:first_name::STRING AS first_name,
     array_size(RAW_FILE:spoken_languages) AS spoken_languages
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:spoken_languages[0] AS First_language
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:spoken_languages[0] AS First_language
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:first_name::STRING AS First_name,
    RAW_FILE:spoken_languages[0].LANGUAGE::STRING AS First_language,
    RAW_FILE:spoken_languages[0].LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW

SELECT 
    RAW_FILE:id::int AS id,
    RAW_FILE:first_name::STRING AS First_name,
    RAW_FILE:spoken_languages[0].LANGUAGE::STRING AS First_language,
    RAW_FILE:spoken_languages[0].LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
UNION ALL 
SELECT 
    RAW_FILE:id::int AS id,
    RAW_FILE:first_name::STRING AS First_name,
    RAW_FILE:spoken_languages[1].LANGUAGE::STRING AS First_language,
    RAW_FILE:spoken_languages[1].LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
UNION ALL 
SELECT 
    RAW_FILE:id::int AS id,
    RAW_FILE:first_name::STRING AS First_name,
    RAW_FILE:spoken_languages[2].LANGUAGE::STRING AS First_language,
    RAW_FILE:spoken_languages[2].LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
ORDER BY ID

SELECT
    RAW_FILE:first_name::STRING AS First_name,
    f.VALUE:LANGUAGE::STRING AS First_language,
    f.VALUE:LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW, TABLE(flatten(RAW_FILE:spoken_languages)) f;


// 4.5 Insert the Final Data

// Option 1: CREATE TABLE AS
CREATE OR REPLACE TABLE Languages AS
SELECT
    RAW_FILE:first_name::STRING AS First_name,
    f.VALUE:LANGUAGE::STRING AS First_language,
    f.VALUE:LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW, TABLE(flatten(RAW_FILE:spoken_languages)) f;

SELECT * FROM Languages;

TRUNCATE TABLE languages;

// Option 2: INSERT INTO
INSERT INTO Languages
SELECT
    RAW_FILE:first_name::STRING AS First_name,
    f.VALUE:LANGUAGE::STRING AS First_language,
    f.VALUE:LEVEL::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW, TABLE(flatten(RAW_FILE:spoken_languages)) f;

SELECT * FROM Languages;
