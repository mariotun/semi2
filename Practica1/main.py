import pyodbc
import os

def conexiondb():
    try:
        server = 'localhost'
        database = 'practica1semi2'
        username = 'sa'
        password = 'semi2_123'
        driver = '{ODBC Driver 17 for SQL Server}'

        conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        connection = pyodbc.connect(conn_str)
        return connection

    except Exception as e:
        print("No se pudo conectar a SQL Server:",e)

def borrar_modelo():
    connection = pyodbc.connect('DRIVER={SQL Server};SERVER=nombre_servidor;DATABASE=nombre_base_de_datos;UID=usuario;PWD=password')
    cursor = connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS tabla1')
    cursor.execute('DROP TABLE IF EXISTS tabla2')
    connection.commit()
    connection.close()
    print("Se han borrado las tablas del modelo.")

# Función para crear modelo
def crear_modelo():
    sql = '''

    
    
    
    '''
    connection = conexiondb()
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE Estudiantes2 (
                        ID INT PRIMARY KEY,
                        Nombre VARCHAR(50),
                        Apellido VARCHAR(50),
                        Edad INT,
                        CorreoElectronico VARCHAR(100)
                        );
    """)
    #cursor.execute('CREATE TABLE tabla2 (...)')
    connection.commit()
    connection.close()
    print("Se han creado las tablas del modelo.")

# Función para extraer información
def extraer_informacion():
    nombre_archivo = input("Ingrese el nombre del archivo: ")
 
    sql = '''
    CREATE TABLE temporal1 (
        Year NVARCHAR(50),
        Mo NVARCHAR(50),
        Dy NVARCHAR(50),
        Hr NVARCHAR(50),
        Mn NVARCHAR(50),
        Sec NVARCHAR(50),
        Tsunami_Event_Validity NVARCHAR(50),
        Tsunami_Cause_Code NVARCHAR(50),
        Earthquake_Magnitude NVARCHAR(50),
        Deposits NVARCHAR(50),
        Latitude NVARCHAR(50),
        Longitude NVARCHAR(50),
        Maximum_Water_Height NVARCHAR(50),
        Number_of_Runups NVARCHAR(50),
        Tsunami_Magnitude NVARCHAR(50),
        Tsunami_Intensity NVARCHAR(50),
        Total_Deaths NVARCHAR(50),
        Total_Missing NVARCHAR(50),
        Total_Missing_Description NVARCHAR(50),
        Total_Injuries NVARCHAR(50),
        Total_Damage NVARCHAR(50),
        Total_Damage_Description NVARCHAR(50),
        Total_Houses_Destroyed NVARCHAR(50),
        Total_Houses_Damaged NVARCHAR(50),
        Country NVARCHAR(50),
        Location_Name NVARCHAR(50)
    );

    BULK INSERT temporal1
    FROM '/practica1/{}'
    WITH
    (
        FIELDTERMINATOR = ',',  -- Delimitador de campos
        ROWTERMINATOR = '\n',    -- Delimitador de filas
        FIRSTROW = 2            -- Número de la primera fila de datos
    );
    '''.format(nombre_archivo)

    print(sql)
    connection = conexiondb()

    cursor = connection.cursor()
    cursor.execute(sql)

    connection.commit()
    connection.close()
    print("Datos cargados en la tabla temporal.")



def cargar_informacion():
    print("CARGA DE INFORMACION")
    # Procesamiento de la información y carga a la base de datos
    # Esto dependerá del formato de los archivos y de cómo deseas cargar la información a la base de datos.

# Función para realizar consultas
def realizar_consultas():
    connection = pyodbc.connect('DRIVER={SQL Server};SERVER=nombre_servidor;DATABASE=nombre_base_de_datos;UID=usuario;PWD=password')
    cursor = connection.cursor()
    consulta1 = "SELECT * FROM tabla1"
    cursor.execute(consulta1)
    resultados = cursor.fetchall()
    with open('resultados_consulta.txt', 'w') as f:
        for resultado in resultados:
            f.write(str(resultado) + '\n')
    connection.close()
    print("Se han guardado los resultados de las consultas en 'resultados_consulta.txt'.")

# Función principal
def main():
    while True:
        print("\nOpciones:")
        print("a) Borrar modelo")
        print("b) Crear modelo")
        print("c) Extraer información")
        print("d) Cargar información")
        print("e) Realizar consultas")
        print("q) Salir")
        
        opcion = input("Seleccione una opción: ").lower()
        
        if opcion == 'a':
            borrar_modelo()
        elif opcion == 'b':
            crear_modelo()
        elif opcion == 'c':
            extraer_informacion()
        elif opcion == 'd':
            cargar_informacion()
        elif opcion == 'e':
            realizar_consultas()
        elif opcion == 'q':
            print("Saliendo de la aplicación...")
            break
        else:
            print("Opción no válida. Por favor, seleccione una opción válida.")

if __name__ == "__main__":
    main()

