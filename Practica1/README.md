## **PRACTICA1-SEMINARIO DE SISTEMAS 2**

### **Diagrama Modelo:**

![Alt text](./semi2_modelop1.png?raw=true "modelo relacional")

### **Explicacion ETL**

- **Datos Generales**

    | **Modelo** | **Hechos** | **Dimensiones** |
    |:-----:|:--------------:|:--------------:|
    | Estrella | Tsunami | Pais |
    | --- | ---  | Fecha |

- **Explicacion**

    1. Para iniciar con el proceso de ETL lo primero que se realizo es crear una tabla temporal con todas las columnas descritas en el archivo CSV para tener un mejor control sobre los datos.
    ![Alt text](./semi2_temporalp1.png?raw=true "tabla_temporal")
    2. Luego de tener la tabla "Temporal" se procedio a insercion de datos usando el archivo por medio de una carga masiva en sql server.
    ![Alt text](./semi2_cargamasivap1.png?raw=true "carga_masiva")
    3. Luego se procede a analizar los datos de la tabla temporal para poder construir un modelo que se adecue y que pueda ser eficiente al momento de realizar consultas.
    ![Alt text](./semi2_erp1.png?raw=true "modelo_relacional")
    4. Luego del analisis se procede a la insersion de datos a las tablas cosntruidas en el modelo a travez de consultas adecuadas teniendo asi data limpia y ordena.
    ![Alt text](./semi2_insersiondatap1.png?raw=true "insercion_datos")