library(RODBC)

# Establish database connection
conn <- dbConnect(RSQLite::SQLite(),"FinalDB_lab4.sqlite")
dsn_driver <- "{IBM DB2 ODBC Driver}"
dsn_database <- "bludb"
dsn_hostname <- "..."
dsn_port <- "..."
dsn_protocol <- "TCPIP"
dsn_uid <- "..."
dsn_pwd <- "..." 
dsn_security <- "ssl"

conn_path <- paste("DRIVER=",dsn_driver,
                ";DATABASE=",dsn_database,
                ";HOSTNAME=",dsn_hostname,
                ";PORT=",dsn_port,
                ";PROTOCOL=",dsn_protocol,
                ";UID=",dsn_uid,
                ";PWD=",dsn_pwd,
                ";SECURITY=",dsn_security,       
                    sep="")
conn <- odbcDriverConnect(conn_path)
conn
sql.info <- sqlTypeInfo(conn)
conn.info <- odbcGetInfo(conn)
conn.info["DBMS_Name"]
conn.info["DBMS_Ver"]
conn.info["Driver_ODBC_Ver"]


# CROP_DATA:
df1 <- dbConnect (conn, 
                    "CREATE TABLE CROP_DATA (
                                      CD_ID INTEGER NOT NULL,
                                      YEAR DATE NOT NULL,
                                      CROP_TYPE VARCHAR(20) NOT NULL,
                                      GEO VARCHAR(20) NOT NULL, 
                                      SEEDED_AREA INTEGER NOT NULL,
                                      HARVESTED_AREA INTEGER NOT NULL,
                                      PRODUCTION INTEGER NOT NULL,
                                      AVG_YIELD INTEGER NOT NULL,
                                      PRIMARY KEY (CD_ID)
                                      )", 
                    errors=FALSE
                    )

    if (df1 == -1){
        cat ("An error has occurred.\n")
        msg <- odbcGetErrMsg(conn)
        print (msg)
    } else {
        cat ("Table was created successfully.\n")
    }

# FARM_PRICES:
df1 <- sqlQuery (conn, 
                    "CREATE TABLE FARM_PRICES (
                                      CD_ID INTEGER NOT NULL,
                                      YEAR DATE NOT NULL,
                                      CROP_TYPE VARCHAR(20) NOT NULL,
                                      GEO VARCHAR(20) NOT NULL, 
                                      PRICE_PRERMT INTEGER NOT NULL,
                                      PRIMARY KEY (CD_ID)
                                      )", 
                    errors=FALSE
                    )

    if (df1 == -1){
        cat ("An error has occurred.\n")
        msg <- odbcGetErrMsg(conn)
        print (msg)
    } else {
        cat ("Table was created successfully.\n")
    }

# DAILY_FX:
df1 <- sqlQuery (conn, 
                    "CREATE TABLE DAILY_FX (
                                      DFX_ID INTEGER NOT NULL,
                                      YEAR DATE NOT NULL,
                                      FXUSDCAD INTEGER NOT NULL,
                                      PRIMARY KEY (DFX_ID)
                                      )", 
                    errors=FALSE
                    )

    if (df1 == -1){
        cat ("An error has occurred.\n")
        msg <- odbcGetErrMsg(conn)
        print (msg)
    } else {
        cat ("Table was created successfully.\n")
    }

# MONTHLY_FX:
df1 <- sqlQuery (conn, 
                    "CREATE TABLE MONTHLY_FX (
                                      DFX_ID INTEGER NOT NULL,
                                      YEAR DATE NOT NULL,
                                      FXUSDCAD INTEGER NOT NULL,
                                      PRIMARY KEY (DFX_ID)
                                      )", 
                    errors=FALSE
                    )

    if (df1 == -1){
        cat ("An error has occurred.\n")
        msg <- odbcGetErrMsg(conn)
        print (msg)
    } else {
        cat ("Table was created successfully.\n")
    }

crop_df <- read.csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Annual_Crop_Data.csv')
farm_prices_df <- read.csv ('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Monthly_Farm_Prices.csv')
daily_fx_df <- read.csv ('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Daily_FX.csv')
monthly_fx_df <- read.csv ('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Monthly_FX.csv')
head(crop_df)
head(farm_prices_df)
head(daily_fx_df)
head(monthly_fx_df)

query = "SELECT COUNT(CD_ID) FROM FARM_PRICES"
    sqlQuery(conn,query)

query <- "SELECT DISTINCT(GEO) FROM FARM_PRICES;"
    view <- sqlQuery(conn,query)
    view

query = "SELECT SUM(HARVESTED_AREA) 
         WHERE GEO = 'Canada' AND CROP_TYPE = 'Rye' AND YEAR(YEAR) = 1968"
sqlQuery(conn,query)

query = "SELECT * FROM FARM_PRICES 
WHERE CROP_TYPE ='Rye' LIMIT 6"
sqlQuery(conn,query)

query <- 
    "SELECT DISTINCT(GEO) 
    FROM CROP_DATA 
    WHERE CROP_TYPE ='Barley';"
    view <- sqlQuery(conn,query)
    view

query <-
    "SELECT min(DATE) FIRST_DATE, max(DATE) LAST_DATE
    FROM FARM_PRICES;
    "
    view <- sqlQuery(conn,query)
    view

query = "SELECT DISTINCT (CROP_TYPE), 
PRICE_PRERMT, YEAR(DATE) 
WHERE PRICE_PRERMT >= 350 ORDER BY PRICE_PRERMT LIMIT 10"
sqlQuery(conn,query)

query = "SELECT CROP_TYPE AS CROP_RANK, MAX(AVG_YIELD)AS AVG_YIELD_YEAR_2000 
FROM CROP_DATA 
WHERE YEAR(YEAR)=2000 AND GEO='Saskatchewan' 
GROUP BY CROP_TYPE 
ORDER BY AVG_YIELD_YEAR_2000 desc"
sqlQuery(conn,query)

query = "SELECT CROP_TYPE, GEO, AVG_YIELD 
FROM CROP_DATA
WHERE YEAR(YEAR) >=2000
ORDER BY AVG_YIELD DESC LIMIT 10"
sqlQuery(conn,query)

query = "SELECT MAX(YEAR) AS MOST_RECENT_YEAR, 
SUM(HARVESTED_AREA) FROM CROP_DATA
WHERE YEAR(YEAR)=(SELECT MAX(YEAR(YEAR))FROM CROP_DATA 
WHERE GEO = 'Canada' AND CROP_TYPE  = 'Wheat')"
sqlQuery(conn,query)

query = "SELECT A.DATE,A.CROP_TYPE,A.GEO,A.PRICE_PRERMT AS CANADIAN_DOLLARS, 
(A.PRICE_PRERMT / B.FXUSDCAD) AS US_DOLLARS, B.FXUSDCAD 
FROM FARM_PRICES A , MONTHLY_FX B 
WHERE YEAR(A.DATE) = YEAR(B.DATE) 
AND MONTH(A.DATE) = MONTH(B.DATE) 
AND CROP_TYPE ='Canola' 
AND GEO = 'Saskatchewan' 
ORDER BY DATE DESC LIMIT 6"
sqlQuery(conn,query)


