# main.Crawler ads.txt

<b>--Implementation and assumptions

Referring to the Ads.txt specification v1.0.1, certain assumptions have been taken for the implementation and hence, account for the same. 
</b>
<br>
<br>
<img src="img/1.png">

<i><b>The code follows the above to parse ads.txt responses for success (2xx) and redirect statuses (3xx) for as many times the root domain is maintained with at max 1 domain change hop delegation. Also, there were a few websites that returned ads.txt content with status of 4xx. However, 4xx status responses have been marked as failures and not saved to db.</b> </i>
<br>
<br>
<br>


<img src="img/2.png">
<br>
<br>
<i><b>For ads.txt that had content-type other than text/plain are ignored. However, as per my observation, there were a few websites that had content-type text/html for their valid ads.txt file. Regarding the format, the format in green has been followed to store records and since the second format - Variable format(marked in red) wasn't mentioned in the requirement document, it has been ignored.</b></i>

<br>
<br>

# Running the project

<b>Requirements: </b><br>
1. Java<br>
2. PostgreSQL 9.6 or above<br>
3. Postgres driver JAR for JDBC<br>

<b>Process: </b><br>
1. Configure in main.Setup.java file, the settings for your db.<br>
2. Run Init.java file once to create tables and indexes.<br>
3. Run main.Crawler.java to start crawling ads.txt files of domains mentioned in txt file configured in Setup.java variable<b>DOMAIN_LIST_FILE</b><br>



<b>--Table creation SQLs</b>

CREATE TABLE website(<br>
    website_id SERIAL PRIMARY KEY,<br>
    name varchar(100) UNIQUE NOT NULL,<br>
    last_crawled_at timestamp<br>
);<br>
<br>
CREATE TABLE advertiser(<br>
    advertiser_id SERIAL PRIMARY KEY,<br>
    name varchar(100) UNIQUE NOT NULL,<br>
    tag varchar(100)  //I think this should be unique too, but wasn't sure of the same!<br>
);<br>
<br>
CREATE TABLE website_advertiser_relation(<br>
	website_advertiser_relation_id SERIAL PRIMARY KEY,<br>
	website_id INTEGER NOT NULL REFERENCES website(website_id) ON DELETE CASCADE,<br>
	advertiser_id INTEGER NOT NULL REFERENCES advertiser(advertiser_id) ON DELETE CASCADE<br>
);<br>
<br>
CREATE TABLE publisher(<br>
    publisher_id SERIAL PRIMARY KEY,<br>
    website_advertiser_relation_id INTEGER NOT NULL REFERENCES website_advertiser_relation(website_advertiser_relation_id) ON DELETE CASCADE,<br>
    account_id varchar(100) NOT NULL,<br>
    account_type varchar(200) NOT NULL,<br>
    UNIQUE (website_advertiser_relation_id, account_id)<br>
);<br>


<b>--Indexes (Explicitly created ones)</b>

CREATE INDEX ON website_advertiser_relation (website_id);<br>

CREATE INDEX ON website_advertiser_relation (advertiser_id);<br>

CREATE INDEX ON publisher (account_id);<br>



<b>--Queries</b>

1. List of unique advertisers on a website.<br>
Select name from advertiser where advertiser_id IN
(Select advertiser_id from website_advertiser_relation where website_id =
(Select website_id from website where name='steadyhealth.com' limit 1));


2. List of websites that contain a given advertiser. <br>
Select name from website where website_id IN
(Select website_id from website_advertiser_relation where advertiser_id =
(Select advertiser_id from advertiser where name='google.com' limit 1));


3. List of websites that contain a given advertiser id. <br>
Select name from website where website_id IN
(Select website_id from publisher where account_id = 'pub-2051007210431666');


4. List of all unique advertisers. <br>
Select name from advertiser;
