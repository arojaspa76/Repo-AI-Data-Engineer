-- Base de datos
CREATE DATABASE IF NOT EXISTS tesis 
COMMENT 'This is an external database' 
LOCATION '/maestria/database/tesis';

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
    LOCATION '/maestria/tables/eventos';