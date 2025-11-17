# Repo AI Data Engineer

Information about the course `Curso Procesos ETL para Workloads de AI` part of the `Programa Certified AI Data Engineer`

## To All Students 

Please update your `Ubuntu 24.04 systems` with `Apache HDFS` & `Apache Hive` the following to solve the issue with mapreduce:

### Stop active process
1. please stop all hdfs & yarn: stop-yarn.sh && stop-hdfs.sh
2. please stop all hive
    - pkill -f hiveserver2 
    - pkill -f metastore 
    - pkill -f org.apache.hive 

### Update the following files:
1. /usr/local/hadoop/etc/hadoop/core-site.xml
2. /usr/local/hadoop/etc/hadoop/hdfs-site.xml
3. /usr/local/hadoop/etc/hadoop/mapred-site.xml
4. /usr/local/hadoop/etc/hadoop/yarn-site.xml
5. /usr/local/hive/conf/hive-site.xml

### Copy .jar libraries from $HIVE_HOME to $HADOOP_HOME
1. sudo cp $HIVE_HOME/lib/commons-collections-*.jar $HADOOP_HOME/share/hadoop/common/lib/
2. sudo cp $HIVE_HOME/lib/commons-lang*.jar $HADOOP_HOME/share/hadoop/common/lib/
3. sudo cp $HIVE_HOME/lib/commons-logging*.jar $HADOOP_HOME/share/hadoop/common/lib/


### Start process
1. please start all hdfs & yarn: start-hdfs.sh && start-yarn.sh
2. please start all hive services in different terminal:
    - cd $HIVE_HOME && hive --service hiveserver2
    - beeline -u "jdbc:hive2://localhost:10000/default" -n suusuario
