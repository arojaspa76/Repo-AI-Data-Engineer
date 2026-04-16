# Repo AI Data Engineer - Session 5

Information about the course `Curso Procesos ETL para Workloads de AI` part of the `Programa Certified AI Data Engineer`

## Instalación de Apache HDFS + Apache HiveQL compatibles con Java 17 en WSL Ubuntu 24.04

* Instalación de Apache Hadoop
* Instalación de Apache HiveSQL
* Archivos de Computer Vision


## Objetivo

Instalar y dejar funcionando un stack local de laboratorio con:

- **Ubuntu 24.04 en WSL**
- **Java 17**
- **Apache Hadoop 3.5.0** para HDFS
- **Apache Hive 4.1.0** para HiveQL
- **HiveServer2 + Beeline**
- **Metastore Derby embebido**

Esta guía incluye:

- instalación paso a paso
- configuración base
- validaciones
- troubleshooting real de los errores encontrados

---

## Stack recomendado

La combinación estable para este caso fue:

- **Java 17**
- **Apache Hadoop 3.5.0**
- **Apache Hive 4.1.0**

Motivo práctico:

- Hadoop 3.5.0 es compatible con Java 17
- Hive 4.1.0 funciona con Java 17
- Hive 4.2.x ya se mueve a Java 21, por lo que no conviene con este entorno

---
# 0. Si existe alguna instalacion, ejecutar estos pasos previos

## Primero detén todo lo que esté corriendo
```bash
pkill -f HiveServer2
stop-yarn.sh
stop-dfs.sh
```

## Luego validar
```bash
jps -l
ps -ef | grep -E "HiveServer2|NameNode|DataNode|SecondaryNameNode|ResourceManager|NodeManager" | grep -v grep
```

## Si todavía quedan procesos Java de Hadoop/Hive hay que cerrarlos
```bash
pkill -f NameNode
pkill -f DataNode
pkill -f SecondaryNameNode
pkill -f ResourceManager
pkill -f NodeManager
pkill -f HiveServer2
```

## borrado completo y reinstalación limpia
```bash
# si el directorio de trabajo era bigdatadl, en las instrucciones siguientes cambiar bigdata por bigdatadl
rm -rf ~/bigdata/hadoop
rm -rf ~/bigdata/hive
rm -rf ~/bigdata/hadoopdata
rm -rf ~/bigdata/logs

#Si además quieres dejar limpio cualquier metastore o archivos de prueba:
rm -f ~/bigdata/personas.csv
```

## Después revisar el ~/.bashrc ya que seguramente quedaron rutas viejas duplicadas
```bash
nano ~/.bashrc
```
## eliminr o comentar líneas como estas si están repetidas o apuntan a instalaciones viejas
```bash
# si el directorio de trabajo era bigdatadl, en las instrucciones siguientes cambiar bigdata por bigdatadl
export HADOOP_HOME=$HOME/bigdata/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HIVE_HOME=$HOME/bigdata/hive
export PATH=$HIVE_HOME/bin:$PATH
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
```
## Eliminar rutas obsoletas
```bash
/home/arojaspa/bigdatadl/hadoop/bin
/home/arojaspa/bigdatadl/hadoop/sbin
/home/arojaspa/bigdatadl/hive/bin
/opt/spark/bin
/opt/spark/sbin
```

## Recargar el perfil
```bash
source ~/.bashrc
hash -r
```

## Validar variales de entorno
```bash
# si el directorio de trabajo era bigdatadl, en las instrucciones siguientes cambiar bigdata por bigdatadl
jps -l
ls -la ~/bigdata
echo $HADOOP_HOME
echo $HIVE_HOME
echo $PATH
which hadoop
which hive
which beeline
```
> Si hiciste borrado completo, idealmente which hadoop y which beeline no deberían apuntar a ninguna instalación previa, deberian quedar en blanco.

---
# 1. Preparación del sistema

Actualizar Ubuntu e instalar dependencias básicas:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y wget curl tar ssh rsync net-tools procps openssh-server
```

Verificar Java 17:

```bash
java -version
readlink -f "$(which java)"
dirname "$(dirname "$(readlink -f "$(which java)")")"
```

Normalmente:

```bash
/usr/lib/jvm/java-17-openjdk-amd64
```

---

# 2. Variables de entorno

Editar `~/.bashrc`:

```bash
nano ~/.bashrc
```

Agregar al final:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

export WORKDIR=$HOME/bigdata
export HADOOP_HOME=$WORKDIR/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

export HIVE_HOME=$WORKDIR/hive

export PATH=$PATH:$HIVE_HOME/bin:$PATH
```

Recargar:

```bash
source ~/.bashrc
hash -r
```

Validar:

```bash
echo $JAVA_HOME
echo $HADOOP_HOME
echo $HIVE_HOME
which beeline
beeline --version
```

> Importante: `which beeline` debe apuntar a `$HIVE_HOME/bin/beeline`, no al de Spark.

---

# 3. Crear estructura de trabajo

```bash
mkdir -p ~/bigdata
cd ~/bigdata
```

---

# 4. Instalar Hadoop 3.5.0

```bash
cd ~/bigdata
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz
tar -xzf hadoop-3.5.0.tar.gz
mv hadoop-3.5.0 hadoop
```

Validar:

```bash
source ~/.bashrc
hadoop version
```

---

# 5. Configurar SSH local

```bash
sudo service ssh start
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
ssh localhost
```

Si entra sin pedir contraseña, salir con:

```bash
exit
```

---

# 6. Crear directorios de datos HDFS

```bash
mkdir -p ~/bigdata/hadoopdata/hdfs/namenode
mkdir -p ~/bigdata/hadoopdata/hdfs/datanode
```

---

# 7. Configuración de Hadoop

## 7.1 hadoop-env.sh

Editar:

```bash
nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```

Asegurar:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

---

## 7.2 core-site.xml

```bash
nano $HADOOP_HOME/etc/hadoop/core-site.xml
```

Contenido:

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

---

## 7.3 hdfs-site.xml

```bash
nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
```

Contenido:

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>

  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///home/arojaspa/bigdata/hadoopdata/hdfs/namenode</value>
  </property>

  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///home/arojaspa/bigdata/hadoopdata/hdfs/datanode</value>
  </property>
</configuration>
```

> Cambia `arojaspa` por tu usuario si aplica.

---

## 7.4 mapred-site.xml

```bash
cp $HADOOP_HOME/etc/hadoop/mapred-site.xml.template $HADOOP_HOME/etc/hadoop/mapred-site.xml
nano $HADOOP_HOME/etc/hadoop/mapred-site.xml
```

Contenido:

```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
```

---

## 7.5 yarn-site.xml

```bash
nano $HADOOP_HOME/etc/hadoop/yarn-site.xml
```

Contenido:

```xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
```

---

# 8. Formatear y arrancar HDFS

Formatear NameNode una sola vez:

```bash
hdfs namenode -format
```

## 8.1 Start process
1. para iniciar hdfs & yarn: 
```bash
start-dfs.sh && start-yarn.sh
```

Validar:

```bash
jps -l
```

Deberían verse al menos:

- NameNode
- DataNode
- SecondaryNameNode
- ResourceManager
- NodeManager

Validar HDFS:

```bash
hdfs dfs -mkdir -p /user/$USER
hdfs dfs -mkdir -p /tmp
hdfs dfs -ls /
```

---

# 9. Instalar Hive 4.1.0

```bash
cd ~/bigdata
wget https://dlcdn.apache.org/hive/hive-4.1.0/apache-hive-4.1.0-bin.tar.gz
tar -xzf apache-hive-4.1.0-bin.tar.gz
mv apache-hive-4.1.0-bin hive
source ~/.bashrc
```

---

# 10. Configurar Hive

Crear archivo limpio `hive-site.xml`:

```bash
cat > /home/arojaspa/bigdata/hive/conf/hive-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>

  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=/home/arojaspa/bigdata/hive/metastore_db;create=true</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.EmbeddedDriver</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>APP</value>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>mine</value>
  </property>

  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>

  <property>
    <name>hive.exec.scratchdir</name>
    <value>/tmp/hive</value>
  </property>

  <property>
    <name>hive.server2.thrift.port</name>
    <value>10000</value>
  </property>

  <property>
    <name>hive.server2.thrift.bind.host</name>
    <value>127.0.0.1</value>
  </property>

  <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
  </property>

  <property>
    <name>hive.root.logger</name>
    <value>DEBUG,console</value>
  </property>

</configuration>
EOF
```

---

# 11. Preparar HDFS para Hive

```bash
hdfs dfs -mkdir -p /tmp
hdfs dfs -chmod 1777 /tmp

hdfs dfs -mkdir -p /tmp/hive
hdfs dfs -chmod 1777 /tmp/hive

hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod 777 /user/hive/warehouse
```

Validar:

```bash
hdfs dfs -ls /
hdfs dfs -ls /user/hive
hdfs dfs -ls /tmp
```

---

# 12. Inicializar metastore Derby

```bash
schematool -dbType derby -initSchema
```

Para consultar estado:

```bash
schematool -dbType derby -info
```

---

# 13. Arrancar HiveServer2

En primer plano:

```bash
$HIVE_HOME/bin/hiveserver2
```

O en segundo plano:

```bash
mkdir -p $HOME/bigdata/logs
nohup $HIVE_HOME/bin/hiveserver2 > $HOME/bigdata/logs/hiveserver2.out 2>&1 &
sleep 10
```

Validar proceso y puerto:

```bash
jps -l
ss -ltnp | grep 10000
ps -ef | grep -i hiveserver2 | grep -v grep
```

La validación correcta esperada es ver el puerto 10000 escuchando.

## 13.2 Para parar todos los servicios de hive
```bash
pkill -f hiveserver2 
pkill -f metastore 
pkill -f org.apache.hive 
```

---

# 14. Conectar con Beeline

Usar siempre el cliente correcto de Hive:

```bash
$HIVE_HOME/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default;transportMode=binary' -n arojaspa
```

Si conecta bien, debe verse algo como:

```text
Connected to: Apache Hive (version 4.1.0)
Driver: Hive JDBC (version 4.1.0)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 4.1.0 by Apache Hive
0: jdbc:hive2://127.0.0.1:10000/default>
```

---

# 15. Pruebas funcionales en Hive

Dentro de Beeline:

```sql
SHOW DATABASES;
CREATE DATABASE demo;
USE demo;

CREATE TABLE personas (
  id INT,
  nombre STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

SHOW TABLES;
DESCRIBE personas;
```

Crear archivo CSV desde Linux:

```bash
cat > /home/arojaspa/bigdata/personas.csv <<'EOF'
1,Ana
2,Luis
3,Carla
EOF
```

Cargarlo desde Beeline:

```sql
LOAD DATA LOCAL INPATH '/home/arojaspa/bigdata/personas.csv' INTO TABLE personas;
SELECT * FROM personas;
```

---

# 16. Troubleshooting completo realizado

## 16.1 Error XML en `hive-site.xml`

Error observado:

```text
String '--' not allowed in comment (missing '>'?)
```

Causa:
- el XML tenía un comentario mal formado

Solución:
- reemplazar `hive-site.xml` por un archivo limpio y válido
- validar con `xmllint` si se desea:

```bash
sudo apt install -y libxml2-utils
xmllint --noout /home/arojaspa/bigdata/hive/conf/hive-site.xml
```

---

## 16.2 Error Derby con `/home/USER/...`

Error observado:

```text
Failed to create database '/home/USER/bigdata/hive/metastore_db'
```

Causa:
- el archivo tenía la ruta literal `/home/USER/...`

Solución:
- reemplazar por la ruta real:

```xml
<value>jdbc:derby:;databaseName=/home/arojaspa/bigdata/hive/metastore_db;create=true</value>
```

Si hubo intento fallido previo:

```bash
rm -rf /home/arojaspa/bigdata/hive/metastore_db
schematool -dbType derby -initSchema
```

---

## 16.3 Error de impersonación / doAs

Error observado:

```text
User: arojaspa is not allowed to impersonate arojaspa
```

Causa:
- HiveServer2 intentaba ejecutar con `doAs=true`

Solución:
- desactivar impersonación para laboratorio local:

```xml
<property>
  <name>hive.server2.enable.doAs</name>
  <value>false</value>
</property>
```

---

## 16.4 Confusión por `beeline` equivocado

Problema observado:
- `which beeline` apuntaba a `/opt/spark/bin/beeline`
- `beeline --version` mostraba:

```text
Beeline version 2.3.9 by Apache Hive
```

Causa:
- el `PATH` estaba resolviendo primero el binario de Spark

Solución:
- usar explícitamente:

```bash
$HIVE_HOME/bin/beeline
```

y corregir el `PATH` en `~/.bashrc`.

---

## 16.5 `Connection refused`

Error observado:

```text
Could not open client transport with JDBC Uri...
java.net.ConnectException: Connection refused
```

Causas posibles que se revisaron:
- HiveServer2 no estaba arriba
- HiveServer2 no había terminado de inicializar
- el puerto 10000 no estaba escuchando todavía
- se estaba usando cliente incorrecto
- el arranque se estaba revisando antes de tiempo

Validaciones usadas:

```bash
ss -ltnp | grep 10000
jps -l
ps -ef | grep -i hiveserver2 | grep -v grep
tail -n 120 $HOME/bigdata/logs/hiveserver2.out
```

Resolución final:
- esperar a que HS2 realmente quedara escuchando en `127.0.0.1:10000`
- usar el `beeline` correcto
- conectar con:

```bash
$HIVE_HOME/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default;transportMode=binary' -n arojaspa
```

---

## 16.6 Warnings SLF4J / Log4j

Mensajes observados:

```text
Class path contains multiple SLF4J bindings
The use of package scanning to locate Log4j plugins is deprecated
```

Conclusión:
- son warnings de logging
- no bloquearon la instalación ni la conexión final

---

# 17. Comandos de operación diaria

## Arrancar HDFS y YARN

```bash
start-dfs.sh
start-yarn.sh
```

## Ver procesos

```bash
jps -l
```

## Arrancar HiveServer2

```bash
$HIVE_HOME/bin/hiveserver2
```

o

```bash
nohup $HIVE_HOME/bin/hiveserver2 > $HOME/bigdata/logs/hiveserver2.out 2>&1 &
```

## Verificar puerto

```bash
ss -ltnp | grep 10000
```

## Conectar con Beeline

```bash
$HIVE_HOME/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default;transportMode=binary' -n arojaspa
```

## Parar servicios

```bash
pkill -f hiveserver2 
pkill -f metastore 
pkill -f org.apache.hive 
stop-yarn.sh
stop-dfs.sh
```

---

# 18. Estado final esperado

Cuando todo quede bien, deberías tener:

- HDFS operativo
- YARN operativo
- metastore Derby inicializado
- HiveServer2 escuchando en `127.0.0.1:10000`
- Beeline 4.1.0 conectado correctamente
- consultas HiveQL funcionando

---

# 19. Validación final mínima

```bash
jps -l
ss -ltnp | grep 10000
$HIVE_HOME/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default;transportMode=binary' -n arojaspa
```

Y dentro de Beeline:

```sql
SHOW DATABASES;
```

Si eso responde correctamente, la instalación quedó lista.

---
