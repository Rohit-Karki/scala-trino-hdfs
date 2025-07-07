-- File: init.sql

CREATE DATABASE IF NOT EXISTS exampledb;

USE exampledb;

DROP TABLE IF EXISTS iceberg_namespace_properties;
DROP TABLE IF EXISTS iceberg_tables;

CREATE TABLE iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(255),
    property_value VARCHAR(5500),
    PRIMARY KEY (catalog_name(100), namespace(100), property_key(100))
);

CREATE TABLE iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(5500),
    previous_metadata_location VARCHAR(5500),
    PRIMARY KEY (catalog_name(100), table_namespace(100), table_name(100))
);
