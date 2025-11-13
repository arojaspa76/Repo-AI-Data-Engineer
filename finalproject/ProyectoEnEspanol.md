# Ingeniero de Datos de IA

Información sobre el curso `Procesos ETL para Cargas de Trabajo de IA`, que forma parte del `Programa de Certificación de Ingeniero de Datos de IA`.

## Para todos los estudiantes

 Proyecto Final – Deep Learning, Visión por Computador y Big Data (YOLO + Hive)

## 1. Descripción General

Este proyecto integra **Deep Learning**, **Visión por Computador** y **Procesamiento Big Data**.  
Cada estudiante deberá construir una solución end-to-end compuesta por **dos sistemas claramente separados**:

1. **Sistema de Clasificación**:
   - Ejecuta YOLO (u otra red neuronal) sobre imágenes y videos.
   - Extrae atributos ricos de cada objeto detectado.
   - Escribe todas las detecciones **localmente en archivos CSV** (capa de staging).

2. **Sistema Batch / ETL** (solo Python, sin PySpark):
   - Lee los archivos CSV generados por el sistema de clasificación.
   - Realiza el proceso completo de **extracción, limpieza, transformación y carga**.
   - Envía la información procesada en lotes a **Apache Hive**, siguiendo reglas de tiempo (para video) y de finalización (para imágenes).
   - Garantiza que **no exista información duplicada** en Hive.

La solución debe cumplir buenas prácticas de ingeniería: entorno virtual, Makefile, linting, pruebas y documentación.

---

## 2. Requerimientos de Sistema Operativo y Python

1. **Ubuntu 24.04**
2. **Python 3.10**  
   - Se debe instalar siguiendo:  
     `sesion_5_y_6/guias/Guia_Instalacion_Python310_OpenCV_v410.pdf`
3. **Apache HDFS** instalado y funcionando
4. **Apache Hive** instalado y funcionando
5. **GPU NVIDIA (opcional)**
   - Si el equipo tiene GPU NVIDIA, se debe instalar:
     - OpenCV con CUDA
     - TensorFlow con CUDA  
   - Ver:  
     `sesion_5_y_6/StepByStepToInstallOpenCVWithCudaSupport.txt`

---

## 3. Requerimientos del Entorno Virtual

Todo el proyecto debe correr dentro de un **entorno virtual de Python**.

### Obligatorio:

1. Crear un **Makefile** que automatice:
   - Creación del entorno virtual
   - Instalación de librerías
   - Linting con pylint
   - Formateo del código
   - Ejecución de pruebas unitarias
2. Incluir un archivo `requirements.txt`
3. Incluir una carpeta **tests/** con pruebas para todos los `.py` y `.ipynb`
4. Comentar adecuadamente todo el código:
   - Cada función con docstring
   - Bloques lógicos
   - Flujo general del pipeline

---

## 4. Requerimientos de Datos y Modelo

### 4.1 Datos

1. Al menos **20 imágenes diferentes**
2. Al menos **2 videos**:
   - Deben contener personas
   - Máximo 20 segundos o 50 MB por video
3. Las imágenes y videos deben ser **capturados por ustedes**, no descargados.
4. Si usan captura en tiempo real, pueden utilizar:
   - Cámara USB
   - Cámara CSI
   - Cámara RTSP  
   Deben informar qué tipo de cámara usarán.

### 4.2 Modelos

1. Se puede utilizar:
   - YOLO preentrenado
   - YOLO ajustado por ustedes
   - Una red neuronal propia
2. Se puede utilizar **una o varias redes neuronales**
3. El propósito principal es **clasificar objetos y extraer características enriquecidas**

---

## 5. Requerimientos de Detección

La solución debe:

1. Detectar al menos **15 objetos/características** en imágenes
2. Detectar al menos **10 objetos/características** en videos
3. Para **cada objeto detectado**, guardar la mayor cantidad de información posible (ver Sección 6)

---

## 6. Atributos a Extraer por Objeto

Para **cada objeto detectado**, se deben extraer como mínimo:

### A. Información Básica y de Modelo

- `source_type` – `"image"` o `"video"`
- `source_id` – nombre del archivo (ej. `imagen_01.jpg`, `video_01.mp4`)
- `frame_number` – 0 para imágenes, número de frame en video
- `class_id`
- `class_name`
- `confidence`

### B. Información del Bounding Box

- `x_min`, `y_min`, `x_max`, `y_max`
- `width`, `height`
- `area_pixels` = `width * height`
- `frame_width`, `frame_height`
- `bbox_area_ratio` = área del bbox / área del frame
- `center_x`, `center_y`
- `center_x_norm`, `center_y_norm` (normalizados entre 0 y 1)
- `position_region` – una de:
  - `top-left`, `top-center`, `top-right`
  - `middle-left`, `middle-center`, `middle-right`
  - `bottom-left`, `bottom-center`, `bottom-right`

### C. Color Dominante (con OpenCV)

A partir del ROI del objeto:

- `dominant_color_name` – por ejemplo: `red`, `green`, `blue`, `black`, `white`, `yellow`
- `dom_r`, `dom_g`, `dom_b` – componentes RGB dominantes

### D. Metadatos de Video

- `timestamp_sec` – tiempo aproximado del frame (ej. `frame_number / fps`)

### E. Opcional para Personas

Si `class_name == "person"`, se puede incluir:

- Flags booleanos de objetos solapados (`has_backpack`, `has_cellphone`, etc.)
- Número de objetos cercanos en el mismo frame
- Resultados de estimación de pose (opcional)
- Emociones de la cara (opcional)

---

## 7. Arquitectura de Dos Sistemas (Separación Obligatoria)

El proyecto **debe estar implementado como dos sistemas / puntos de entrada distintos en Python**:

### 7.1 Sistema de Clasificación

- Implementado en uno o varios archivos (ej. `sistema_clasificacion.py`)
- Responsabilidades:
  - Cargar YOLO (y redes auxiliares si las hay)
  - Procesar imágenes y videos
  - Extraer todos los atributos de la Sección 6
  - **Escribir todas las detecciones en uno o más archivos CSV locales** (capa de staging)
- Restricción:
  - Este sistema **NO** debe conectarse a Hive.
  - Su única responsabilidad es **clasificar y generar CSVs**.

Se recomienda que el CSV incluya, además de los campos de la Sección 6:

- `ingestion_date` – fecha/hora de generación de la detección
- Un **identificador único de detección** (por ejemplo, `detection_id` o la combinación `source_id + frame_number + local_object_id`)

### 7.2 Sistema Batch / ETL (Solo Python)

- Implementado en un archivo diferente (ej. `sistema_batch_etl.py`)
- **No se permite PySpark**: se debe utilizar **Python puro** (ej. `csv`, `pandas` y algún conector/CLI para Hive).
- Responsabilidades:
  1. **Extracción**:
     - Leer los CSV generados por el Sistema de Clasificación.
  2. **Limpieza**:
     - Manejar valores nulos, coordenadas inválidas, confidencias fuera de rango, etc.
  3. **Transformación**:
     - Normalizar, castear tipos, generar campos derivados si es necesario.
  4. **Carga a Hive**:
     - Insertar los registros en una tabla de Hive **sin duplicados**.

#### Reglas de Envío de Lotes

- Para **imágenes**:
  - Una vez terminado el análisis de todas las imágenes, el sistema batch debe enviar los registros correspondientes a Hive.
- Para **videos**:
  - El sistema batch debe enviar la información en ventanas de **10 segundos de contenido de video**.
  - Ejemplo:
    - Para un video de 40 segundos se podrían tener hasta 4 lotes:  
      `[0–10s], [10–20s], [20–30s], [30–40s]`.

La agrupación de los 10 segundos se puede hacer con `timestamp_sec` o con `frame_number` y `fps`.

#### Prohibido Duplicar Datos (Requisito Imperativo)

- Está **terminantemente prohibido** enviar detecciones duplicadas a Hive.
- Deben diseñar una estrategia de **sincronización** entre los dos sistemas para garantizar que:
  - Cada detección se cargue **una sola vez**.
  - Re-ejecutar el sistema batch no genere filas duplicadas.

Ejemplos de estrategias:

- Definir una clave única (`source_id`, `frame_number`, `class_id`, `local_object_id`) y:
  - Eliminar duplicados en Python antes de insertar.
  - O implementar lógica/constraints en Hive para evitar insertar claves ya existentes.
- Mantener un archivo de **checkpoint** o una marca de “procesado” para los CSV.

---

## 8. Dataset de Salida y Esquema en Hive

El sistema batch debe producir datos estructurados y cargarlos a Hive como:

- Archivos **CSV** o **Parquet** en HDFS, y
- Una o más **tablas de Hive**.

Ejemplo de tabla en Hive (adaptable):

```sql
CREATE EXTERNAL TABLE yolo_objects (
  source_type           STRING,
  source_id             STRING,
  frame_number          INT,
  class_id              INT,
  class_name            STRING,
  confidence            DOUBLE,
  x_min                 INT,
  y_min                 INT,
  x_max                 INT,
  y_max                 INT,
  width                 INT,
  height                INT,
  area_pixels           INT,
  frame_width           INT,
  frame_height          INT,
  bbox_area_ratio       DOUBLE,
  center_x              DOUBLE,
  center_y              DOUBLE,
  center_x_norm         DOUBLE,
  center_y_norm         DOUBLE,
  position_region       STRING,
  dominant_color_name   STRING,
  dom_r                 INT,
  dom_g                 INT,
  dom_b                 INT,
  timestamp_sec         DOUBLE,
  ingestion_date        STRING,
  detection_id          STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///projects/yolo_objects/hive/';

```

## 9. Consultas Analíticas (Hive)

Deben entregar al menos 5 consultas en Hive, por ejemplo:

1. Conteo de objetos por clase.
2. Número de personas por video.
3. Área promedio de los bounding boxes por clase.
4. Distribución de colores dominantes por clase.
5. Número de objetos por ventana de 10 segundos en cada video.

## 10. Entregables

* README.md con instrucciones claras de ejecución.
* GUIA_PROYECTO_FINAL_ES.md.
* Carpeta src/ con:
  * Sistema de Clasificación (YOLO + CSV).
  * Sistema Batch / ETL (CSV → Hive, sin duplicados).
* Carpeta tests/ con pruebas.
* Makefile
* requirements.txt
* Scripts SQL para creación de tablas en Hive.
* Muestras de las imágenes y videos usados.

## 11. Evaluación

| Categoría                                          | Peso |
| -------------------------------------------------- | ---- |
| Detección correcta con YOLO                        | 20%  |
| Calidad de la extracción de atributos              | 20%  |
| Uso correcto de CSV local y separación de sistemas | 15%  |
| Lógica ETL y cumplimiento de ventana de 10s        | 20%  |
| Ausencia de duplicados en Hive                     | 10%  |
| Calidad de código + Makefile + pruebas             | 10%  |
| Documentación                                      | 5%   |


## 12. Notas Finales

Este es un proyecto de nivel de ingeniería.
El resultado debe ser limpio, modular, reproducible y bien documentado, con una separación clara entre:
* Sistema de Clasificación (detección + CSV)
* Sistema Batch / ETL (CSV → Hive, sin duplicados)

## ¡Éxitos con el desarrollo del proyecto!