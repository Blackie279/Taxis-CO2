<h1 align="center">  Semana 2: Data Engineering </h1>

## 📋 Indice
1. [Descripcion de la semana](#descripcion)
2. [Diagrama E-R](#e-r)
3. [Diccionario](#dicc)
4. [Data Warehouse y Pipeline](#dw)
5. [Flujo de Trabajo](#workflow)

## 1. Descripción de la semana. <a name="descripcion"></a>

El enfoque de la labor llevada a cabo durante la segunda semana del proyecto se centró en el ámbito de la ingeniería de datos. Este proceso abarcó la extracción de datos desde diversas fuentes, la posterior depuración para asegurar su calidad y, finalmente, la carga de estos datos en la infraestructura del Data Warehouse seleccionado. Para llevar a cabo estas tareas, se distribuyeron responsabilidades entre los miembros del equipo de trabajo, y los hitos y plazos de ejecución se encuentran representados en el Diagrama de Gantt adjunto. Este diagrama proporciona una visión detallada de las actividades realizadas y su programación.


## 2. Data Warehouse y Diagrama E-R <a name="e-r"></a>
Cuando implementamos la estructura de almacenamiento, optamos por utilizar un Data Warehouse a través de la herramienta BigQuery, que forma parte de la plataforma de Google Cloud (GCP). Debido a que se trata de un modelo de datos relacionales, elaboramos un diagrama de Entidad-Relación que visualiza de manera gráfica la disposición lógica de nuestra base de datos.

<img src="src/modelo_er.drawio(1).png" alt="modelo ER">

## 3. Diccionario de datos <a name="dicc"></a>

Con base en el diagrama E-R mencionado previamente, hemos creado un diccionario de datos para aclarar la interpretación de la información y verificar su precisión. Puedes consultarlo en el siguiente.


## 4. Pipeline <a name="dw"></a>

El siguiente diagrama proporciona un desglose detallado del pipeline que ilustra claramente el proceso de ETL desarrollado anteriormente. Por un lado, se destacan varias fuentes de datos, que continúan hacia el proceso de limpieza, transformación y validación realizado en Python con la librería Pandas, para posteriormente ser cargados en la base de datos de Google BigQuery. Este flujo hacia el Data Warehouse se conoce como carga inicial. En cuanto a la carga incremental, que se ejecuta de manera automática cada mes, se emplean herramientas proporcionadas por GCP: Cloud Scheduler, Cloud Pub/Sub, Cloud Function y Cloud Storage. Después de este proceso, los datos se cargan en la base de datos relacional de BigQuery.

<img src="src/ETL.drawio.png" alt="pipelines">

## 5. Flujo de Trabajo <a name="workflow"></a>

En lo que respecta al proceso de trabajo, el diagrama que sigue ofrece una vista integral del ciclo completo de vida de los datos en el proyecto. Se inicia con la recopilación de datos, seguida por su integración y carga en el Data Warehouse. Finalmente, se incluyen los distintos procesos que accederán a la base de datos: en un extremo se halla el Modelo de Machine Learning, que emplea Scikit-Learn como su herramienta, mientras que en el otro se encuentra la generación de informes y visualizaciones, con el objetivo de crear un panel de control utilizando PowerBI.

<img src="flujo_del_dato.drawio.png" alt="flujo del dato">
--------------------------------------------------------------------------------------------------------------------------------------------------------------
## Semana 2: Análisis Profundo de los Datos e Informes

Durante la segunda semana del proyecto, nuestro enfoque principal fue llevar a cabo un análisis detallado de los datos recopilados, con el objetivo de obtener una comprensión completa del panorama actual del mercado de taxis en la ciudad de Nueva York.

### Actividades Realizadas:

1. **Análisis Detallado de los Datos:**
   - Realizamos un análisis exhaustivo de los datasets proporcionados, explorando cada variable y comprendiendo la estructura de los datos.

2. **ETL en Servicio Cloud (GCP) para BigQuery:**
   - Implementamos un proceso ETL (Extract, Transform, Load) en un servicio cloud, en este caso Google Cloud Platform (GCP), para organizar y almacenar los datos en BigQuery, facilitando su posterior manipulación y análisis.

3. **Corrección y Refinamiento de KPIs:**
   - Basados en el análisis de los datos, revisamos y refinamos los KPIs previamente establecidos para asegurarnos de que estuvieran alineados con la información disponible y proporcionaran métricas precisas.

4. **Desarrollo de un Minimum Viable Product (MVP) de Dashboard:**
   - Creamos un prototipo de tablero de control que presentaba de manera visual algunos de los insights clave derivados del análisis de datos. Este MVP proporcionó una idea inicial de cómo sería el producto final.

### Resultados y Conclusiones:

Al finalizar la semana 2, habíamos logrado un profundo entendimiento de los datos y habíamos desarrollado un sistema eficiente para su gestión en la nube. También revisamos y ajustamos los KPIs para asegurarnos de que estuvieran bien alineados con la información disponible. El MVP del dashboard nos permitió visualizar de manera preliminar los insights más relevantes, lo que nos proporcionó una visión clara del valor que este proyecto puede aportar a la empresa interesada.
