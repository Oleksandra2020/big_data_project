docker exec -it cassandra-node1 cqlsh -e "
CREATE KEYSPACE big_data_project WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE big_data_project;

CREATE TABLE page_domain (domain text, page_id int, PRIMARY KEY (domain));

CREATE TABLE pages (page_id int, page_title text, PRIMARY KEY (page_id));

CREATE TABLE page_user (user_id int, page_title text, PRIMARY KEY (user_id));

CREATE TABLE info_per_date (review_date timestamp, page_title text, page_id int, user_id int, PRIMARY KEY ((review_date), user_id));

CREATE TABLE general (review_date timestamp, domain text, page_id int, user_is_bot boolean, page_title text, user_id int, user_text text, PRIMARY KEY((domain), page_id));"

# docker run -it --network general-network --rm cassandra cqlsh cassandra-node1
