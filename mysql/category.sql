CREATE DATABASE flink;

USE flink;

CREATE TABLE category (
  id BIGINT NOT NULL PRIMARY KEY,
  name VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

INSERT INTO category (id, name)
VALUES  
(1, 'Books'),
(2, 'Cell Phones'),
(3, 'Clothing & Shoes'),
(4, 'Drinks'),
(5, 'Electronics'),
(6, 'Foods'), 
(7, 'Home & Kitchens'),
(8, 'Others'),
(9, 'Sports & Outdoors'),
(10, 'Tools');
