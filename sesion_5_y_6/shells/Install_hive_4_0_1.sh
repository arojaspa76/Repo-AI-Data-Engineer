#!/bin/bash
# ============================================================
# Reinstalación limpia de Apache Hive 4.0.1 (Java 8 + Hadoop 3.4.2)
# Autor: Andres Felipe Rojas Parra
# Uso:
#   chmod +x reinstalar_hive_4_0_1.sh
#   ./reinstalar_hive_4_0_1.sh
# ============================================================

# ==== Parámetros ====
HIVE_VERSION="4.0.1"
HIVE_TAR="apache-hive-${HIVE_VERSION}-bin.tar.gz"
HIVE_URL="https://downloads.apache.org/hive/hive-${HIVE_VERSION}/${HIVE_TAR}"
HIVE_HOME="/usr/local/hive"
HADOOP_HOME="/usr/local/Hadoop"
JAVA_HOME_PATH="/usr/lib/jvm/java-8-openjdk-amd64"

echo "===== [0/8] Verificando prerequisitos ====="
if [ -z "$JAVA_HOME" ]; then
  echo "ADVERTENCIA: JAVA_HOME no está definido. Se usará: ${JAVA_HOME_PATH}"
  export JAVA_HOME="${JAVA_HOME_PATH}"
fi

if [ -z "$HADOOP_HOME" ]; then
  echo "ADVERTENCIA: HADOOP_HOME no está definido. Se usará: ${HADOOP_HOME}"
  export HADOOP_HOME="${HADOOP_HOME}"
fi

echo "JAVA_HOME  = $JAVA_HOME"
echo "HADOOP_HOME= $HADOOP_HOME"
sleep 1

# ==== FASE 1: Limpieza total de Hive ====
echo "===== [1/8] Deteniendo servicios de Hive si existen ====="
pkill -f hiveserver2 2>/dev/null || true
pkill -f metastore 2>/dev/null || true
pkill -f org.apache.hive 2>/dev/null || true

echo "===== [2/8] Eliminando instalación previa de Hive ====="
sudo rm -rf "${HIVE_HOME}"
rm -rf ~/metastore_db
rm -rf /tmp/hive* 2>/dev/null || true

echo "===== [3/8] Limpiando rutas de Hive en HDFS (si existen) ====="
hdfs dfs -rm -r -skipTrash /user/hive/warehouse 2>/dev/null || true
hdfs dfs -rm -r -skipTrash /tmp/hive 2>/dev/null || true

echo "===== [4/8] Descargando e instalando Hive ${HIVE_VERSION} ====="
cd /usr/local
sudo wget -nc "${HIVE_URL}"
sudo tar -xzvf "${HIVE_TAR}"
sudo mv "apache-hive-${HIVE_VERSION}-bin" hive
sudo rm -f "${HIVE_TAR}"

echo "===== Ajustando permisos de /usr/local/hive ====="
sudo chown -R $USER:$USER "${HIVE_HOME}"

echo "===== [5/8] Configurando variables de entorno (recordatorio) ====="
echo "Revisa tu ~/.bashrc y asegúrate de tener líneas como:"
echo "  export HIVE_HOME=${HIVE_HOME}"
echo "  export PATH=\$PATH:\$HIVE_HOME/bin"
echo "  export HADOOP_HOME=${HADOOP_HOME}"
echo "  export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin"
echo "Luego ejecuta: source ~/.bashrc"
echo

# No tocamos ~/.bashrc automáticamente para no mezclar con otras configs del alumno

echo "===== [6/8] Configurando hive-env.sh ====="
cd "${HIVE_HOME}/conf"
cp hive-env.sh.template hive-env.sh
cat <<EOF >> hive-env.sh

# Configuración añadida por reinstalar_hive_4_0_1.sh
export HADOOP_HOME=${HADOOP_HOME}
export HIVE_CONF_DIR=${HIVE_HOME}/conf
export JAVA_HOME=${JAVA_HOME_PATH}
EOF

echo "===== [7/8] Inicializando metastore Derby embebido ====="
cd "${HIVE_HOME}"
schematool -initSchema -dbType derby

echo "===== [7.1/8] Creando hive-site.xml mínimo ====="
cd "${HIVE_HOME}/conf"
cat > hive-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.server2.authentication</name>
    <value>NONE</value>
  </property>
</configuration>
EOF

echo "===== [8/8] Creando directorios en HDFS ====="
start-dfs.sh
start-yarn.sh

hdfs dfs -mkdir -p /tmp /user/hive/warehouse
hdfs dfs -chmod 1777 /tmp /user/hive/warehouse

echo
echo "======================================================="
echo "  Reinstalación de Apache Hive ${HIVE_VERSION} completada."
echo "  Pasos finales sugeridos:"
echo "    1) source ~/.bashrc"
echo "    2) En una terminal:  cd ${HIVE_HOME} && hive --service hiveserver2"
echo '    3) En otra:          beeline -u "jdbc:hive2://localhost:10000/default" -n '"$USER"
echo "======================================================="
