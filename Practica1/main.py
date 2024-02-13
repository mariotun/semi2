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
    sql = '''
    DROP TABLE temporal1;
    DROP TABLE Earthquakes;
    DROP TABLE TsunamiEvents;
    DROP TABLE Locations;
    '''
    
    connection = conexiondb()

    cursor = connection.cursor()
    cursor.execute(sql)

    connection.commit()
    connection.close()
    print("Modelo barrado correctamente.")

# Función para crear modelo
def crear_modelo():
    sql = '''
    CREATE TABLE Temporal1 (
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

    -- tabla de Ubicaciones
    CREATE TABLE Locations (
        LocationID INT PRIMARY KEY IDENTITY,
        Latitude FLOAT,
        Longitude FLOAT,
        LocationName VARCHAR(255)
    );

    -- tabla de Eventos de Tsunami
    CREATE TABLE TsunamiEvents (
        TsunamiEventID INT PRIMARY KEY IDENTITY,
        Year INT,
        Mo INT,
        Dy INT,
        Hr INT,
        Mn INT,
        Sec FLOAT,
        TsunamiEventValidity INT,
        TsunamiCauseCode INT,
        MaximumWaterHeight FLOAT,
        NumberOfRunups INT,
        TsunamiMagnitude FLOAT,
        TsunamiIntensity FLOAT,
        TotalDeaths INT,
        TotalMissing INT,
        TotalMissingDescription INT,
        TotalInjuries INT,
        TotalDamage FLOAT,
        TotalDamageDescription INT,
        TotalHousesDestroyed INT,
        TotalHousesDamaged INT,
        Country VARCHAR(255),
        LocationID INT,
        FOREIGN KEY (LocationID) REFERENCES Locations(LocationID)
    );

    -- tabla de Terremotos
    CREATE TABLE Earthquakes (
        EarthquakeID INT PRIMARY KEY IDENTITY,
        TsunamiEventID INT,
        EarthquakeMagnitude FLOAT,
        Deposits INT,
        FOREIGN KEY (TsunamiEventID) REFERENCES TsunamiEvents(TsunamiEventID)
    );
    '''
    connection = conexiondb()

    cursor = connection.cursor()
    cursor.execute(sql)

    connection.commit()
    connection.close()
    print("Modelo creado correctamente.")

# Función para extraer información
def extraer_informacion():
    nombre_archivo = input("Ingrese el nombre del archivo: ")
 
    sql = '''
    BULK INSERT Temporal1
    FROM '/practica1/{}'
    WITH
    (
        FIELDTERMINATOR = ',',  -- Delimitador de campos
        ROWTERMINATOR = '\n',    -- Delimitador de filas
        FIRSTROW = 2            -- Número de la primera fila de datos
    );
    '''.format(nombre_archivo)

    connection = conexiondb()

    cursor = connection.cursor()
    cursor.execute(sql)

    connection.commit()
    connection.close()
    print("Datos cargados en la tabla temporal.")



def cargar_informacion():
    sql = '''
    -- Paso 1: Insertar datos en la tabla Locations
    INSERT INTO Locations (Latitude, Longitude, LocationName)
    SELECT DISTINCT 
        CONVERT(FLOAT, Latitude), 
        CONVERT(FLOAT, Longitude), 
        Location_Name
    FROM temporal1
    WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL AND Location_Name IS NOT NULL;

    -- Paso 2: Insertar datos en la tabla TsunamiEvents
    INSERT INTO TsunamiEvents (
        Year, Mo, Dy, Hr, Mn, Sec, TsunamiEventValidity, TsunamiCauseCode,
        MaximumWaterHeight, NumberOfRunups, TsunamiMagnitude, TsunamiIntensity,
        TotalDeaths, TotalMissing, TotalMissingDescription, TotalInjuries,
        TotalDamage, TotalDamageDescription, TotalHousesDestroyed, TotalHousesDamaged,
        Country, LocationID
    )
    SELECT 
        CONVERT(INT, Year), 
        CONVERT(INT, Mo), 
        CONVERT(INT, Dy), 
        CONVERT(INT, Hr), 
        CONVERT(INT, Mn), 
        CONVERT(FLOAT, Sec), 
        CONVERT(INT, Tsunami_Event_Validity), 
        CONVERT(INT, Tsunami_Cause_Code), 
        CONVERT(FLOAT, Maximum_Water_Height), 
        CONVERT(INT, Number_of_Runups), 
        CONVERT(FLOAT, Tsunami_Magnitude), 
        CONVERT(FLOAT, Tsunami_Intensity), 
        CONVERT(INT, Total_Deaths), 
        CONVERT(INT, Total_Missing), 
        CONVERT(INT, Total_Missing_Description), 
        CONVERT(INT, Total_Injuries), 
        CONVERT(FLOAT, Total_Damage), 
        CONVERT(INT, Total_Damage_Description), 
        CONVERT(INT, Total_Houses_Destroyed), 
        CONVERT(INT, Total_Houses_Damaged), 
        Country, 
        L.LocationID
    FROM temporal1 T
    JOIN Locations L ON T.Location_Name = L.LocationName
    WHERE Year IS NOT NULL AND Mo IS NOT NULL AND Dy IS NOT NULL AND Hr IS NOT NULL 
        AND Mn IS NOT NULL AND Sec IS NOT NULL AND Tsunami_Event_Validity IS NOT NULL 
        AND Tsunami_Cause_Code IS NOT NULL AND Maximum_Water_Height IS NOT NULL 
        AND Number_of_Runups IS NOT NULL AND Tsunami_Magnitude IS NOT NULL 
        AND Tsunami_Intensity IS NOT NULL AND Total_Deaths IS NOT NULL 
        AND Total_Missing IS NOT NULL AND Total_Missing_Description IS NOT NULL 
        AND Total_Injuries IS NOT NULL AND Total_Damage IS NOT NULL 
        AND Total_Damage_Description IS NOT NULL AND Total_Houses_Destroyed IS NOT NULL 
        AND Total_Houses_Damaged IS NOT NULL AND Country IS NOT NULL;

    -- Paso 3: Insertar datos en la tabla Earthquakes
    INSERT INTO Earthquakes (
        TsunamiEventID, EarthquakeMagnitude, Deposits
    )
    SELECT 
        TE.TsunamiEventID, 
        CONVERT(FLOAT, Earthquake_Magnitude), 
        CONVERT(INT, Deposits)
    FROM temporal1 T
    JOIN TsunamiEvents TE ON T.Year = TE.Year AND T.Mo = TE.Mo 
        AND T.Dy = TE.Dy AND T.Hr = TE.Hr AND T.Mn = TE.Mn AND T.Sec = TE.Sec
    WHERE Earthquake_Magnitude IS NOT NULL AND Deposits IS NOT NULL;
    '''

    connection = conexiondb()

    cursor = connection.cursor()
    cursor.execute(sql)

    connection.commit()
    connection.close()
    print("Datos cargados correctamente al modelo.")

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

