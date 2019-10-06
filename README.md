# Crawler ads.txt

--Table creation SQLs

CREATE TABLE website(
    website_id SERIAL PRIMARY KEY,
    name varchar(100) UNIQUE NOT NULL,
    last_crawled_at timestamp
);

CREATE TABLE advertiser(
    advertiser_id SERIAL PRIMARY KEY,
    name varchar(100) UNIQUE NOT NULL,
    tag varchar(100)
);

CREATE TABLE website_advertiser_relation(
	website_advertiser_relation_id SERIAL PRIMARY KEY,
	website_id INTEGER NOT NULL REFERENCES website(website_id) ON DELETE CASCADE,
	advertiser_id INTEGER NOT NULL REFERENCES advertiser(advertiser_id) ON DELETE CASCADE
);

CREATE TABLE publisher(
    publisher_id SERIAL PRIMARY KEY,
    website_advertiser_relation_id INTEGER NOT NULL REFERENCES website_advertiser_relation(website_advertiser_relation_id) ON DELETE CASCADE,
    account_id varchar(100) NOT NULL,
    account_type varchar(200) NOT NULL,
    UNIQUE (website_advertiser_relation_id, account_id)
);


--Indexes

CREATE INDEX ON advertiser (website_id);




