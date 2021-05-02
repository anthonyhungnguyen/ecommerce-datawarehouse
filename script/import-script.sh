#! /bin/sh`
# Import csv to OLTP table 

docker exec sad_mysql mysql --local-infile -uroot -padmin OLTP -e "LOAD DATA LOCAL INFILE '/data/olist_customers_dataset.csv' INTO TABLE OLTP_CUSTOMERS FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS"

docker exec sad_mysql mysql --local-infile -uroot -padmin OLTP -e "LOAD DATA LOCAL INFILE '/data/product_category_name_translation.csv' INTO TABLE OLTP_CATEGORIES_TRANSLATION FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS"

docker exec sad_mysql mysql --local-infile -uroot -padmin OLTP -e "LOAD DATA LOCAL INFILE '/data/olist_geolocation_dataset.csv' INTO TABLE OLTP_GEOLOCATION FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS"

docker exec sad_mysql mysql --local-infile -uroot -padmin OLTP -e "LOAD DATA LOCAL INFILE '/data/olist_order_items_dataset.csv' INTO TABLE OLTP_ORDER_ITEMS FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS"

docker exec sad_mysql mysql --local-infile -uroot -padmin OLTP -e "LOAD DATA LOCAL INFILE '/data/olist_order_payments_dataset.csv' INTO TABLE OLTP_ORDER_PAYMENTS FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS"

docker exec sad_mysql mysql --local-infile -uroot -padmin OLTP -e "LOAD DATA LOCAL INFILE '/data/olist_order_reviews_dataset.csv' INTO TABLE OLTP_ORDER_REVIEWS FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS"

docker exec sad_mysql mysql --local-infile -uroot -padmin OLTP -e "LOAD DATA LOCAL INFILE '/data/olist_orders_dataset.csv' INTO TABLE OLTP_ORDERS FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS"

docker exec sad_mysql mysql --local-infile -uroot -padmin OLTP -e "LOAD DATA LOCAL INFILE '/data/olist_products_dataset.csv' INTO TABLE OLTP_PRODUCTS FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS"

docker exec sad_mysql mysql --local-infile -uroot -padmin OLTP -e "LOAD DATA LOCAL INFILE '/data/olist_sellers_dataset.csv' INTO TABLE OLTP_SELLERS FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS"