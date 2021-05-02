CREATE DATABASE DWH;
USE DWH;

CREATE TABLE DWH.CUSTOMERS
(
    CUSTOMER_KEY BIGINT NOT NULL AUTO_INCREMENT,
    CUSTOMER_UNIQUE_ID VARCHAR(50) NOT NULL,
    CUSTOMER_ZIPCODE VARCHAR(5) NOT NULL,
    CUSTOMER_CITY VARCHAR(50) NOT NULL,
    CUSTOMER_STATE VARCHAR(50) NOT NULL,
    CUSTOMER_GEO_LAT DOUBLE PRECISION NOT NULL,
    CUSTOMER_GEO_LONG DOUBLE PRECISION NOT NULL,
    CONSTRAINT CUSTOMERS_PK PRIMARY KEY (CUSTOMER_KEY)
);

CREATE TABLE DWH.ORDER_PAYMENTS
(
    PAYMENT_KEY BIGINT NOT NULL AUTO_INCREMENT,
    ORDER_ID VARCHAR(50) NOT NULL,
    PAYMENT_SEQUENTIAL SMALLINT NOT NULL,
    PAYMENT_TYPE VARCHAR(20) NOT NULL,
    PAYMENT_INSTALLMENTS SMALLINT NOT NULL,
    PAYMENT_VALUE REAL NOT NULL,
    CONSTRAINT ORDER_PAYMENTS_PK PRIMARY KEY (PAYMENT_KEY)
);

CREATE TABLE DWH.PRODUCTS
(
    PRODUCT_KEY BIGINT NOT NULL AUTO_INCREMENT,
    PRODUCT_ID VARCHAR(50) NOT NULL,
    PRODUCT_CATEGORY_NAME VARCHAR(100) NOT NULL,
    PRODUCT_CATEGORY_NAME_ENGLISH VARCHAR(100) NOT NULL,
    PRODUCT_NAME_LENGTH SMALLINT NOT NULL,
    PRODUCT_DESCRIPTION_LENGTH INTEGER NOT NULL,
    PRODUCT_PHOTOS_QTY SMALLINT NOT NULL,
    PRODUCT_WEIGHT_GRAMS SMALLINT NOT NULL,
    PRODUCT_LENGTH_CM SMALLINT NOT NULL,
    PRODUCT_HEIGHT_CM SMALLINT NOT NULL,
    PRODUCT_WIDTH_CM SMALLINT NOT NULL,
    CONSTRAINT PRODUCTS_PK PRIMARY KEY (PRODUCT_KEY)
);

CREATE TABLE DWH.SELLERS
(
    SELLER_KEY BIGINT NOT NULL AUTO_INCREMENT,
    SELLER_ID VARCHAR(50) NOT NULL,
    SELLER_ZIPCODE VARCHAR(5) NOT NULL,
    SELLER_CITY VARCHAR(50) NOT NULL,
    SELLER_STATE VARCHAR(50) NOT NULL,
    SELLER_GEO_LAT DOUBLE PRECISION NOT NULL,
    SELLER_GEO_LNG DOUBLE PRECISION NOT NULL,
    CONSTRAINT SELLERS_PK PRIMARY KEY (SELLER_KEY)
);

CREATE TABLE DWH.ORDERS
(
    ORDER_ID VARCHAR(50) NOT NULL,
    CUSTOMER_KEY BIGINT NOT NULL,
    PRODUCT_KEY BIGINT NOT NULL,
    SELLER_KEY BIGINT NOT NULL,
    PAYMENT_KEY BIGINT NOT NULL,
    ORDER_ITEM_QTA SMALLINT NOT NULL,
    PRICE REAL NOT NULL,
    FREIGHT_VALUE REAL NOT NULL,
    ORDER_STATUS VARCHAR(30) NOT NULL,
    REVIEW_SCORE SMALLINT NOT NULL,
    ORDER_PURCHASE_TIMESTAMP TIMESTAMP NOT NULL,
    ORDER_APPROVAL_TIMESTAMP TIMESTAMP NOT NULL,
    ORDER_DELIVERED_CARRIER_TIMESTAMP TIMESTAMP NOT NULL,
    ORDER_DELIVERED_CUSTOMER_TIMESTAMP TIMESTAMP NOT NULL,
    ORDER_ESTIMATED_DELIVERY_TIMESTAMP TIMESTAMP NOT NULL
);

ALTER TABLE DWH.ORDERS
ADD CONSTRAINT ORDERS_CUSTOMERS_FK FOREIGN KEY (CUSTOMER_KEY)
REFERENCES DWH.CUSTOMERS (CUSTOMER_KEY)
MATCH SIMPLE
ON UPDATE NO ACTION
ON DELETE NO ACTION;

ALTER TABLE DWH.ORDERS
ADD CONSTRAINT ORDERS_ORDER_PAYMENTS_FK FOREIGN KEY (PAYMENT_KEY)
REFERENCES DWH.ORDER_PAYMENTS (PAYMENT_KEY)
MATCH SIMPLE
ON UPDATE NO ACTION
ON DELETE NO ACTION;


ALTER TABLE DWH.ORDERS
ADD CONSTRAINT ORDERS_PRODUCTS_FK FOREIGN KEY (PRODUCT_KEY)
REFERENCES DWH.PRODUCTS (PRODUCT_KEY)
MATCH SIMPLE
ON UPDATE NO ACTION
ON DELETE NO ACTION;

ALTER TABLE DWH.ORDERS
ADD CONSTRAINT ORDERS_SELLERS_FK FOREIGN KEY (SELLER_KEY)
REFERENCES DWH.SELLERS (SELLER_KEY)
MATCH SIMPLE
ON UPDATE NO ACTION
ON DELETE NO ACTION;

CREATE INDEX FKI_ORDERS_CUSTOMERS_FK
ON DWH.ORDERS(CUSTOMER_KEY);

CREATE INDEX FKI_ORDERS_ORDER_PAYMENTS_FK
ON DWH.ORDERS(PAYMENT_KEY);

CREATE INDEX FKI_ORDERS_PRODUCTS_FK
ON DWH.ORDERS(PRODUCT_KEY);

CREATE INDEX FKI_ORDERS_SELLERS_FK
ON DWH.ORDERS(SELLER_KEY);
