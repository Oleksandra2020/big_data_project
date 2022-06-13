
CREATE KEYSPACE big_data_project WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE big_data_project;

CREATE TABLE page_domain (domain text, page_id int, PRIMARY KEY (domain));

CREATE TABLE pages (page_id int, page_title text, PRIMARY KEY (page_id));

CREATE TABLE info_per_date (review_date timestamp, page_title text, page_id int, user_id int, PRIMARY KEY ((review_date), user_id));

CREATE TABLE general (review_date timestamp, domain text, page_id int, user_is_bot boolean, page_title text, user_id int, user_text text, PRIMARY KEY((domain), page_id));

CREATE TABLE statistics1 (time_start timestamp, time_end timestamp, statistics text, PRIMARY KEY(time_start));

CREATE TABLE statistics2 (time_start timestamp, time_end timestamp, statistics text, PRIMARY KEY(time_start));

CREATE TABLE statistics3 (time_start timestamp, time_end timestamp, statistics text, PRIMARY KEY(time_start));