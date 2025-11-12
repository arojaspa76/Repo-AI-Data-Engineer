#!/bin/bash
# ==============================================================
# Script de instalación automatizada de Hadoop 3.4.2 con Java 8
# Autor: Andrés Rojas Parra
# Fecha: Noviembre 2025
# Descripción: Configura Hadoop en modo pseudo-distribuido (1 nodo)
# ==============================================================

HADOOP_VERSION="3.4.2"
HADOOP_HOME="/usr/local/hadoop"
JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"

echo "===== [1/9] Actualizando paquetes ====="
sudo apt update -y && sudo apt upgrade -y

echo "===== [2/9] Instalando dependencias ====="
sudo apt install -y openjdk-8-jdk ssh rsync wget tar

echo "===== [3/9] Verificando Java ====="
java -version || { echo "Error: Java no instalado correctamente"; exit 1; }

echo "===== [4/9] Configurando SSH ====="
if [ ! -f ~/.ssh/id_rsa ]; then
  ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
fi
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

echo "===== [5/9] Descargando Hadoop $HADOOP_VERSION ====="
cd /usr/local
sudo wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
sudo tar -xzf hadoop-${HADOOP_VERSION}.tar.gz
sudo mv hadoop-${HADOOP_VERSION} hadoop
sudo chown -R $USER:$USER hadoop

echo "===== [6/9] Configurando variables de entorno ====="
if ! grep -q "HADOOP_HOME" ~/.bashrc; then
cat <<EOF >> ~/.bashrc

# Hadoop Environment Variables
export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_INSTALL=\$HADOOP_HOME
export HADOOP_MAPRED_HOME=\$HADOOP_HOME
export HADOOP_COMMON_HOME=\$HADOOP_HOME
export HADOOP_HDFS_HOME=\$HADOOP_HOME
export YARN_HOME=\$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_HOME/lib/native
export PATH=\$PATH:\$HADOOP_HOME/sbin:\$HADOOP_HOME/bin
EOF
fi
source ~/.bashrc

echo "===== [7/9] Configurando archivos XML ====="
cd $HADOOP_HOME/etc/hadoop

# core-site.xml
cat <<EOF > core-site.xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

# hdfs-site.xml
cat <<EOF > hdfs-site.xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:/usr/local/hadoop/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:/usr/local/hadoop/hdfs/datanode</value>
  </property>
</configuration>
EOF

# mapred-site.xml
cat <<EOF > mapred-site.xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
EOF

# yarn-site.xml
cat <<EOF > yarn-site.xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>localhost</value>
  </property>
</configuration>
EOF

# hadoop-env.sh
sed -i "s|^export JAVA_HOME.*|export JAVA_HOME=${JAVA_HOME}|" $HADOOP_HOME/etc/hadoop/hadoop-env.sh

echo "===== [8/9] Creando directorios del HDFS ====="
sudo mkdir -p /usr/local/hadoop/hdfs/namenode /usr/local/hadoop/hdfs/datanode
sudo chown -R $USER:$USER /usr/local/hadoop/hdfs

echo "===== [9/9] Formateando NameNode ====="
hdfs namenode -format

echo "===== Hadoop $HADOOP_VERSION instalado correctamente ====="
echo "Para iniciar los servicios:"
echo "  start-dfs.sh"
echo "  start-yarn.sh"
echo "Para verificar:"
echo "  jps"
echo "  hdfs dfsadmin -report"
echo "Interfaz Web HDFS: http://localhost:9870"
echo "Interfaz Web YARN: http://localhost:8088"
