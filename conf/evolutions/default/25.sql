# --- !Ups

UPDATE
    data_source_products
SET
    class_name = 'com.teradata.jdbc.TeraDriver'
WHERE
    product_root = 'teradata';
