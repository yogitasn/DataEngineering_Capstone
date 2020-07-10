class SqlQueries:
    
    create_table_d_country= ("""
        CREATE TABLE IF NOT EXISTS public.d_country (
            country_id int4 SERIAL NOT NULL,
            country_name varchar(4),
            CONSTRAINT country_pkey PRIMARY KEY (country_id)
            );
    """)
    
    create_table_d_state= ("""
        CREATE TABLE IF NOT EXISTS public.d_state (
            state_id int4 SERIAL NOT NULL,
            state_name varchar(4),
            CONSTRAINT state_pkey PRIMARY KEY (state_id)
            );
    """)
    
    create_table_d_port= ("""
        CREATE TABLE IF NOT EXISTS public.d_port (
            port_id int4 SERIAL NOT NULL,
            port_name varchar(4),
            CONSTRAINT port_pkey PRIMARY KEY (port_id)
            );
    """)
    create_table_final_immigration = ("""
        CREATE TABLE IF NOT EXISTS public.final_immigration (      
            cicid int8,
            i94yr int4,
            i94mon int4,
            i94cit int4,
            i94res int4,
            i94port varchar(4),
            arrdate int4,
            i94mode int4,
            i94addr varchar(4),
            depdate int4,
            i94bir int4,
            i94visa char,
            count int4,
            dtadfile varchar(4),
            visapost varchar(4),
            occup varchar(128),
            entdepa char,
            entdepd char,
            entdepu char,
            matflag varchar(128),
            biryear int4,
            dtadto varchar(128),
            gender char,
            insnum varchar(128),
            airline varchar(128),
            admnum numeric(18,0),
            fltno int4,
            visatype char
            CONSTRAINT final_immigration_airport_pkey PRIMARY KEY (cicid)
            
            
      );
    """)
    
    
    create_table_staging_immigration = ("""
         CREATE TABLE IF NOT EXISTS public.staging_immigration (
            cicid int8,
            i94yr int4,
            i94mon int4,
            i94cit int4,
            i94res int4,
            i94port varchar(4),
            arrdate int4,
            i94mode int4,
            i94addr varchar(4),
            depdate int4,
            i94bir int4,
            i94visa char,
            count int4,
            dtadfile varchar(4),
            visapost varchar(4),
            occup varchar(128),
            entdepa char,
            entdepd char,
            entdepu char,
            matflag varchar(128),
            biryear int4,
            dtadto varchar(128),
            gender char,
            insnum varchar(128),
            airline varchar(128),
            admnum numeric(18,0),
            fltno int4,
            visatype char
            );
          """)
    
    create_table_staging_airport = ("""
        CREATE TABLE IF NOT EXISTS public.staging_airport (
                ident varchar(10),
                type varchar(100),
                name varchar(100),
                elevation_ft int4,
                continent varchar(4),
                iso_country varchar(4),
                iso_region  varchar(4),
                municipality varchar(100),
                gps_code varchar(10),
                iata_code varchar(10),
                local_code varchar(10),
                coordinates  varchar(256)
                );
               """)
    
    create_table_staging_us_cities_demographics = ("""
        CREATE TABLE IF NOT EXISTS public.staging_us_cities_demographics(
                City varchar(100),
                State varchar(100),
                Median_Age float4,
                Male_Population int4,
                Female_Population int4,
                Total_Population int4,
                Number_of_Veterans int4,
                Foreign-born int4,
                Average_Household Size int4,
                State_Code varchar(2),
                Race varchar(20),
                Count int4
                 );
               """)
        
    final_immigration_airport_insert = ("""
        SELECT
            immigr.cicid AS CCCID,
            immigr.i94yr AS YEAR,
            immigr.i94mon AS MONTH,
            immigr.i94yr AS YEAR,
            immi_country.country_name as COUNTRY_NAME,
            immigr.i94mode,
              CASE
                WHEN i94mode = 1 THEN 'Air'
                WHEN i94mode = 2 THEN 'Sea'
                WHEN i94mode = 3 THEN 'Land'
              ELSE
              'Not reported'
            END AS I94MODE,
            immi_state.state_name as STATE_NAME,
            immigr.i94bir AS AGE,
              CASE
              WHEN I94VISA = 1 THEN 'Business'
              WHEN i94visa = 2 THEN 'Pleasure'
              WHEN i94visa = 3 THEN 'Student'
             END AS VISA_CATEGORY,
            immigr.biryear AS BIRTH_YEAR,
            immigr.gender,
            immigr.airline,
            immigr.visatype AS VISA_TYPE,
            immigr.arrival_date,
            immigr.departure_date,
            immigr.no_of_days_stayed
            FROM staging_immigration immigr
            LEFT JOIN immigration_d_country immi_country
            ON immigr.i94res=immi_country.country_id
            LEFT JOIN immigration_d_state immi_state
            ON immigr.i94addr=immi_state.state_id
            LEFT JOIN immigration_d_port immi_port
            ON immigr.i94port=immi_state.port_id
     """)

      d_airport_insert=("""
            CREATE OR REPLACE TABLE D_AIRPORT AS
            SELECT DISTINCT
              substr(iso_region, 4,2) STATE_ID,
              name AIRPORT_NAME,
              iata_code IATA_CODE,
              local_code LOCAL_CODE,
              COORDINATES,
              ELEVATION_FT
            FROM staging_airport
              """)

        D_CITY_DEMO_INSERT=("""
                CREATE OR REPLACE TABLE D_CITY_DEMO AS
                SELECT DISTINCT
                  CITY CITY_NAME,
                  STATE STATE_NAME,
                  STATE_CODE STATE_ID,
                  MEDIAN_AGE,
                  MALE_POPULATION,
                  FEMALE_POPULATION,
                  TOTAL_POPULATION,
                  NUMBER_OF_VETERANS,
                  FOREIGN_BORN,
                  AVERAGE_HOUSEHOLD_SIZE,
                  AVG(
                  IF
                    (RACE = 'White',
                      COUNT,
                      NULL)) WHITE_POPULATION,
                  AVG(
                  IF
                    (RACE = 'Black or African-American',
                      COUNT,
                      NULL)) BLACK_POPULATION,
                  AVG(
                  IF
                    (RACE = 'Asian',
                      COUNT,
                      NULL)) ASIAN_POPULATION,
                  AVG(
                  IF
                    (RACE = 'Hispanic or Latino',
                      COUNT,
                      NULL)) LATINO_POPULATION,
                  AVG(
                  IF
                    (RACE = 'American Indian and Alaska Native',
                      COUNT,
                      NULL)) NATIVE_POPULATION
                FROM
                  staging_us_cities_demographics
                GROUP BY
                  1,
                  2,
                  3,
                  4,
                  5,
                  6,
                  7,
                  8,
                  9,
                  10 
  """)

        
        D_TIME_INSERT=("""  
        CREATE OR REPLACE TABLE D_TIME AS

        SELECT
          ARRIVAL_DATE DATE,
          EXTRACT(DAY FROM ARRIVAL_DATE) DAY,
          EXTRACT(MONTH FROM ARRIVAL_DATE) MONTH,
          EXTRACT(YEAR FROM ARRIVAL_DATE) YEAR,
          EXTRACT(QUARTER FROM ARRIVAL_DATE) QUARTER,
          EXTRACT(DAYOFWEEK FROM ARRIVAL_DATE) DAYOFWEEK,
          EXTRACT(WEEK FROM ARRIVAL_DATE) WEEKOFYEAR
        FROM (
          SELECT
            DISTINCT ARRIVAL_DATE
          FROM
            final_immigration
          UNION DISTINCT
          SELECT
            DISTINCT DEPARTURE_DATE
          FROM
            final_immigration
         WHERE ARRIVAL_DATE IS NOT NULL
 """)
        
     insert_table_d_country= ("""
       SELECT country_id,
            country_name
            FROM d_country
            );
    """)
    
    insert_table_d_state= ("""
        CREATE 
            state_id,
            state_name
            FROM d_state
            );
    """)
    
    insert_table_d_port= ("""
        CREATE port_id,
            port_name 
            FROM d_port);
            """)
 
      
   