-- CREATE EXTERNAL TABLE carros_usados(year INT, modelo STRING, precio INT, millaje INT, color STRING, transmision STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/tesismaestria/pruebasnvidia/tables/carros_usados';

CREATE EXTERNAL TABLE carros_usados (year INT,modelo STRING,precio INT,millaje INT,color STRING,transmision STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/tesismaestria/pruebasnvidia/tables/carros_usados';


--CREATE EXTERNAL TABLE carros_usados_external (year INT,modelo STRING,precio DOUBLE,millaje INT,color STRING,transmision STRING) STORED AS ORC LOCATION '/tesismaestria/pruebasnvidia/tables/carros_usados_external' TBLPROPERTIES ('transactional'='false', 'insert.only'='true')
--CREATE TABLE carros_usados_acid STORED AS ORC AS SELECT * FROM carros_usados_external;

-- Base de datos
CREATE DATABASE IF NOT EXISTS CURSOETLWORKLOADAI
COMMENT 'This is an external database' 
LOCATION '/cursobsg/database/cursoetlworkloadai';

-- Tablas
CREATE EXTERNAL TABLE eventosdetectados (
    dispositivo STRING, 
    tipoinfraccion STRING, 
    nombreimagencapturada STRING, 
    fechahora TIMESTAMP, 
    ubicacion STRING, 
    zonainteres STRING
    ) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS TEXTFILE 
    LOCATION '/cursobsg/tables/eventosdetectados';
