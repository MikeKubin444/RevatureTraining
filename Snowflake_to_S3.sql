-- ============================================================
-- Snowflake -> Amazon S3 using Apache Iceberg
-- External Catalog: AWS Glue
-- Student Demo
-- ============================================================

---------------------------------------------------------------
-- 1. Create Warehouse
---------------------------------------------------------------

-- CREATE OR REPLACE WAREHOUSE demo_wh
--     WAREHOUSE_SIZE = XSMALL
--     AUTO_SUSPEND = 60
--     AUTO_RESUME = TRUE
--     INITIALLY_SUSPENDED = TRUE;

-- USE WAREHOUSE demo_wh;

---------------------------------------------------------------
-- 2. Create Database
---------------------------------------------------------------

CREATE OR REPLACE DATABASE proj2;


---------------------------------------------------------------
-- 3. Create Schema
---------------------------------------------------------------

CREATE OR REPLACE SCHEMA proj2;



---------------------------------------------------------------
-- 4. Create External Volume + AWS Role: Iceburg
---------------------------------------------------------------

-- Create an AWS IAM role named 'Iceburg'.
--
-- Initial Trust Policy (temporary):
-- Replace the account ID below with YOUR Snowflake account ID
-- shown in the CREATE EXTERNAL VOLUME documentation.
--
-- {
--   "Version": "2012-10-17",
--   "Statement": [
--     {
--       "Effect": "Allow",
--       "Principal": {
--         "AWS": "arn:aws:iam::<YOUR_SNOWFLAKE_ACCOUNT_ID>:root"
--       },
--       "Action": "sts:AssumeRole"
--     }
--   ]
-- }
--
-- Attach an inline S3 policy giving the role access to YOUR bucket.
--
-- NOTE:
-- Our Iceberg metadata lives under the /warehouse/ prefix,
-- NOT the /iceberg/ prefix.
--
-- Bucket:
-- <YOUR_BUCKET_NAME>-<YOUR_AWS_ACCOUNT_ID>-us-east-2-an
--
-- {
--   "Version": "2012-10-17",
--   "Statement": [
--     {
--       "Effect": "Allow",
--       "Action": [
--         "s3:GetBucketLocation",
--         "s3:ListBucket"
--       ],
--       "Resource": "arn:aws:s3:::<YOUR_BUCKET_NAME>-<YOUR_AWS_ACCOUNT_ID>-us-east-2-an"
--     },
--     {
--       "Effect": "Allow",
--       "Action": [
--         "s3:GetObject",
--         "s3:PutObject",
--         "s3:DeleteObject"
--       ],
--       "Resource": "arn:aws:s3:::<YOUR_BUCKET_NAME>-<YOUR_AWS_ACCOUNT_ID>-us-east-2-an/warehouse/*"
--     }
--   ]
-- }

CREATE OR REPLACE EXTERNAL VOLUME my_external_volume
STORAGE_LOCATIONS =
(
    (
        NAME='s3_location'
        STORAGE_PROVIDER='S3'
        STORAGE_BASE_URL='s3://<YOUR_BUCKET_NAME>-<YOUR_AWS_ACCOUNT_ID>-us-east-2-an/warehouse/'
        STORAGE_AWS_ROLE_ARN='arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/Iceburg'
    )
)
ALLOW_WRITES = TRUE;

-- get the STORAGE_AWS_IAM_USER_ARN & STORAGE_AWS_EXTERNAL_ID
-- we will use these to update our trust policy in the role
-- this would be a best practice in prod
-- we will have to create a role with a generic trust policy first, 
-- then we can update with the information from this describe call 
DESC EXTERNAL VOLUME my_external_volume;

-- DESC EXTERNAL VOLUME returns:
--
-- STORAGE_AWS_IAM_USER_ARN
-- STORAGE_AWS_EXTERNAL_ID
--
-- Copy these values into the IAM Trust Policy
-- for the Iceburg role.
--
-- This replaces the temporary trust policy with one
-- that only allows YOUR Snowflake account to assume
-- the role.

---------------------------------------------------------------
-- 5. Create Glue Catalog Integration + AWS Role: snowglue
---------------------------------------------------------------

-- Create an IAM role named:
--
-- snowglue
--
-- Use a temporary trust policy first.
-- After running DESC CATALOG INTEGRATION,
-- update the trust policy using the values
-- returned by Snowflake.
--
-- Attach an inline policy giving the role
-- permission to read and manage the Glue Catalog.
--
-- The Glue Catalog in this project is:
--
-- AWS Account: <YOUR_AWS_ACCOUNT_ID>
-- Region: us-east-2
-- Glue Database (Namespace):
-- iceberg_catalog_db
--
-- This is NOT your S3 folder.
-- The namespace refers to the Glue database
-- containing your Iceberg tables.

CREATE OR REPLACE CATALOG INTEGRATION my_catalog
CATALOG_SOURCE = GLUE
TABLE_FORMAT = ICEBERG
CATALOG_NAMESPACE = 'iceberg_catalog_db'
GLUE_AWS_ROLE_ARN = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/<role>'
GLUE_CATALOG_ID = '<YOUR_AWS_ACCOUNT_ID>'
GLUE_REGION = 'us-east-2'
ENABLED = TRUE;

-- much like with our external volume, we will get values 
-- from this so that we can update our trust policy in our relevant AWS role 
-- output like this will be used in our trust policy:
-- GLUE_AWS_IAM_USER_ARN	String	arn:aws:iam::<YOUR_SNOWFLAKE_ACCOUNT_ID>:user/<key>
-- GLUE_AWS_EXTERNAL_ID	String	<key>
DESC CATALOG INTEGRATION my_catalog;
-- Copy the following values from the output:
--
-- GLUE_AWS_IAM_USER_ARN
-- GLUE_AWS_EXTERNAL_ID
--
-- Update the IAM Trust Policy for the
-- snowglue role using those values.
--
-- This is the production-safe trust policy.

---------------------------------------------------------------
-- 6. Create Iceberg Table
---------------------------------------------------------------

-- Snowflake expects the Glue catalog to contain
-- a table named "customers" inside the
-- Glue database "iceberg_catalog_db".
--
-- The Iceberg metadata for this table must
-- reside under the warehouse directory:
--
-- <YOUR_BUCKET_NAME>-<YOUR_AWS_ACCOUNT_ID>-us-east-2-an/warehouse/

CREATE OR REPLACE ICEBERG TABLE customers
    EXTERNAL_VOLUME = 'my_external_volume'
    CATALOG = 'my_catalog'
    CATALOG_TABLE_NAME = 'customers';

CREATE OR REPLACE ICEBERG TABLE products
    EXTERNAL_VOLUME = 'my_external_volume'
    CATALOG = 'my_catalog'
    CATALOG_TABLE_NAME = 'products';

CREATE OR REPLACE ICEBERG TABLE orders
    EXTERNAL_VOLUME = 'my_external_volume'
    CATALOG = 'my_catalog'
    CATALOG_TABLE_NAME = 'orders';

---------------------------------------------------------------
-- 7. Query Data
---------------------------------------------------------------

SELECT *
FROM customers;

SELECT *
FROM orders;

SELECT *
FROM products;

---------------------------------------------------------------
-- 8. Verify Metadata
---------------------------------------------------------------

SHOW ICEBERG TABLES;

DESCRIBE ICEBERG TABLE customers;
DESCRIBE ICEBERG TABLE orders;
DESCRIBE ICEBERG TABLE products;


SELECT COUNT(*)
FROM customers;

SELECT COUNT(*)
FROM orders;

SELECT COUNT(*)
FROM products;


---------------------------------------------------------------
-- 9. Views
---------------------------------------------------------------


-- Customer Summary
CREATE OR REPLACE VIEW customer_summary AS
SELECT
    customer_id,
    CONCAT(first_name, ' ', last_name) AS customer_name,
    email,
    phone,
    country,
    state,
    loyalty_points,
    is_active
FROM customers;


-- order details (3 table join)
CREATE OR REPLACE VIEW order_details AS
SELECT
    o.order_id,
    o.order_date,
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    p.product_name,
    p.category,
    o.quantity,
    o.total_amount,
    o.payment_method,
    o.order_status
FROM orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id
LEFT JOIN products p
    ON o.product_id = p.product_id;

-- sales summary

CREATE OR REPLACE VIEW sales_summary AS
SELECT
    p.category,
    COUNT(o.order_id) AS total_orders,
    SUM(o.quantity) AS units_sold,
    SUM(o.total_amount) AS total_sales,
    AVG(o.total_amount) AS average_order_value
FROM orders o
JOIN products p
    ON o.product_id = p.product_id
GROUP BY p.category;

-- Active Products
CREATE OR REPLACE VIEW active_products AS
SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    stock_quantity
FROM products
WHERE is_active = TRUE;

-- High Value Custys 
CREATE OR REPLACE VIEW high_value_customers AS
SELECT
    customer_id,
    CONCAT(first_name, ' ', last_name) AS customer_name,
    email,
    country,
    loyalty_points
FROM customers
WHERE loyalty_points >= 500;


-- Display the Views!

SELECT * FROM customer_summary;

SELECT * FROM order_details;

SELECT * FROM sales_summary;

SELECT * FROM active_products;

SELECT * FROM high_value_customers;