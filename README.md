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


--Indexes (Explicitly created ones)

CREATE INDEX ON website_advertiser_relation (website_id);

CREATE INDEX ON website_advertiser_relation (advertiser_id);

CREATE INDEX ON publisher (account_id);



--Queries

// List of unique advertisers on a website.
Select name from advertiser where advertiser_id IN
(Select advertiser_id from website_advertiser_relation where website_id =
(Select website_id from website where name='steadyhealth.com' limit 1));


// List of websites that contain a given advertiser.
Select name from website where website_id IN
(Select website_id from website_advertiser_relation where advertiser_id =
(Select advertiser_id from advertiser where name='google.com' limit 1));


// List of websites that contain a given advertiser id.
Select name from website where website_id IN
(Select website_id from publisher where account_id = 'pub-2051007210431666');


// List of all unique advertisers.
Select name from advertiser;