-- Base de datos
CREATE DATABASE IF NOT EXISTS cursobsg
COMMENT 'This is an external database' 
LOCATION '/cursobsg/database/cursobsg';

-- Tablas
CREATE EXTERNAL TABLE eventos (
    dispositivo STRING, 
    tipoinfraccion STRING, 
    imagen STRING, 
    ubicacion STRING, 
    zonainteres STRING, 
    fechahora STRING
    ) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS TEXTFILE 
    LOCATION '/cursobsg/tables/eventos';