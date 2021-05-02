#! /bin/sh
sudo docker exec -i sad_mysql mysql -uroot -padmin OLTP < ./etl-data/oltp.sql
sudo docker exec -i sad_mysql mysql -uroot -padmin STAGING < ./etl-data/staging.sql
sudo docker exec -i sad_mysql mysql -uroot -padmin DWH < ./etl-data/dwh.sql