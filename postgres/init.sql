-- Create the database
CREATE DATABASE stocks;

-- Connect to the database
\c stocks;

-- Create the comments table
CREATE TABLE raw_stock_prices (
    ticker varchar(255),
    date timestamp,
    open decimal,
    close decimal,
    high decimal,
    low decimal
);

CREATE TABLE stock_prices (
    id serial PRIMARY KEY,
    ticker varchar(255),
    date timestamp,
    open decimal,
    close decimal,
    high decimal,
    low decimal
);

CREATE TABLE raw_company_profile( 
    ticker varchar(255),
    county varchar(255),
    exchange varchar(255),
    ipo date,
    marketCapitalization integer,
    name varchar(255),
    phone varchar,
    shareOutstanding decimal,
    weburl varchar(255),
    finnhubIndustry varchar(255)
);

CREATE TABLE company_profile( 
    ticker varchar(255) PRIMARY KEY,
    county varchar(255),
    exchange varchar(255),
    ipo date,
    marketCapitalization integer,
    name varchar(255),
    phone varchar,
    shareOutstanding decimal,
    weburl varchar(255),
    finnhubIndustry varchar(255),
    load_date timestamp
);

CREATE OR REPLACE PROCEDURE scd1_company_profile_update()
AS
$$

BEGIN

WITH updated_records AS (
        SELECT
            rcp.ticker,
            rcp.county,
            rcp.exchange,
            rcp.ipo,
            rcp.marketcapitalization,
            rcp.name,
            rcp.phone,
            rcp.shareoutstanding,
            rcp.weburl,
            rcp.finnhubindustry,
            current_timestamp AS load_date
        FROM
            raw_company_profile rcp
        JOIN
            company_profile cp ON rcp.ticker = cp.ticker
        WHERE
            cp.load_date = (SELECT MAX(load_date) FROM company_profile WHERE cp.ticker = rcp.ticker)
            AND (
                rcp.county <> cp.county OR
                rcp.exchange <> cp.exchange OR
                rcp.ipo <> cp.ipo OR
                rcp.marketcapitalization <> cp.marketcapitalization OR
                rcp.name <> cp.name OR
                rcp.phone <> cp.phone OR
                rcp.shareoutstanding <> cp.shareoutstanding OR
                rcp.weburl <> cp.weburl OR
                rcp.finnhubindustry <> cp.finnhubindustry
            )
    )
    
    
    
    
    
    -- Perform the update
    UPDATE company_profile cp
    SET
        county = ur.county,
        exchange = ur.exchange ,
        ipo = ur.ipo ,
        marketcapitalization = ur.marketcapitalization ,
        name = ur.name ,
        phone = ur.phone ,
        shareoutstanding = ur.shareoutstanding ,
        weburl = ur.weburl ,
        finnhubindustry = ur.finnhubindustry,
        load_date = ur.load_date
    FROM updated_records ur
    WHERE
        cp.ticker = ur.ticker
        AND cp.load_date = (SELECT MAX(load_date) FROM company_profile WHERE ticker = ur.ticker);
	
    -- Create a CTE for inserted records   
    
    with inserted_records AS (
        SELECT
            rcp.ticker,
            rcp.county,
            rcp.exchange,
            rcp.ipo,
            rcp.marketcapitalization,
            rcp.name,
            rcp.phone,
            rcp.shareoutstanding,
            rcp.weburl,
            rcp.finnhubindustry,
            current_timestamp AS load_date
        FROM
            raw_company_profile rcp
        WHERE
            rcp.ticker NOT IN (SELECT ticker FROM company_profile)
    )
       
    -- Perform the insert
    INSERT INTO company_profile (
        ticker, county, exchange, ipo, marketcapitalization,
        name, phone, shareoutstanding, weburl, finnhubindustry,
        load_date
    )
   
    SELECT
        ir.ticker, ir.county, ir.exchange, ir.ipo, ir.marketcapitalization,
        ir.name, ir.phone, ir.shareoutstanding, ir.weburl, ir.finnhubindustry,
        ir.load_date
    FROM inserted_records ir;

END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE stock_price_insert()
AS
$$

BEGIN

    -- Create a CTE for inserted records   
    
    with inserted_records AS (
        select DISTINCT
       		rsp.ticker, 
       		rsp.date, 
       		rsp.open, 
       		rsp.close, 
       		rsp.high, 
       		rsp.low
        FROM
            raw_stock_prices rsp
            left join stock_prices sp on rsp.ticker = sp.ticker and rsp.date = sp.date
        WHERE
            sp.ticker is NULL
    )
       
    -- Perform the insert
    INSERT INTO stock_prices  (
            ticker,
       		date, 
       		open, 
       		close, 
       		high, 
       		low
    )
   
    SELECT
       		ir.ticker, 
       		ir.date, 
       		ir.open, 
       		ir.close, 
       		ir.high, 
       		ir.low
    FROM inserted_records ir;

END;
$$
LANGUAGE plpgsql;

