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
    DROP TABLE Temporal;
    DROP TABLE Tsunami;
    DROP TABLE Fecha;
    DROP TABLE Pais;
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
    CREATE TABLE Temporal (
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

    CREATE TABLE Pais (
        PaisID INT PRIMARY KEY IDENTITY,
        Pais VARCHAR(255)
    );

    CREATE TABLE Fecha (
        FechaID INT PRIMARY KEY IDENTITY,
        Year INT,
        Mo INT,
        Dy INT,
        Hr INT,
        Mn INT,
        Sec FLOAT
    );

    CREATE TABLE Tsunami (
        TsunamiID INT PRIMARY KEY IDENTITY,
        LocationName VARCHAR(255),
        TsunamiMagnitude FLOAT,
        TsunamiIntensity FLOAT,
        TsunamiEventValidity INT,
        TsunamiCauseCode INT,
        MaximumWaterHeight FLOAT,
        NumberOfRunups INT,
        EarthquakeMagnitude FLOAT,
        Latitude FLOAT,
        Longitude FLOAT,
        TotalDeaths INT,
        TotalInjuries INT,
        TotalMissing INT,
        TotalMissingDescription INT,
        TotalDamage FLOAT,
        TotalDamageDescription INT,
        TotalHousesDestroyed INT,
        TotalHousesDamaged INT,
        PaisID INT,
        FOREIGN KEY (PaisID) REFERENCES Pais(PaisID),
        FechaID INT,
        FOREIGN KEY (FechaID) REFERENCES Fecha(FechaID)
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
    BULK INSERT Temporal
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
    INSERT INTO Pais (Pais)
    SELECT DISTINCT Country
    FROM Temporal;

    INSERT INTO Fecha (Year,Mo,Dy,Hr,Mn,Sec)
    SELECT DISTINCT CONVERT(INT,Year),CONVERT(INT,Mo),CONVERT(INT,Dy),CONVERT(INT,Hr),CONVERT(INT,Mn),CONVERT(FLOAT,Sec)
    FROM Temporal;

    INSERT INTO Tsunami (LocationName,TsunamiMagnitude,TsunamiIntensity,TsunamiEventValidity,TsunamiCauseCode,
        MaximumWaterHeight,NumberOfRunups,EarthquakeMagnitude,Latitude,Longitude,TotalDeaths,TotalInjuries,TotalMissing,
        TotalMissingDescription,TotalDamage,TotalDamageDescription,TotalHousesDestroyed,TotalHousesDamaged,PaisID,FechaID)
    SELECT DISTINCT 
        Location_Name,
        CONVERT(FLOAT, Tsunami_Magnitude),
        CONVERT(FLOAT, Tsunami_Intensity),
        CONVERT(INT, Tsunami_Event_Validity),
        CONVERT(INT, Tsunami_Cause_Code),
        CONVERT(FLOAT, Maximum_Water_Height),
        CONVERT(INT, Number_of_Runups),
        CONVERT(FLOAT, Earthquake_Magnitude),
        CONVERT(FLOAT, Latitude), 
        CONVERT(FLOAT, Longitude),
        CONVERT(INT, Total_Deaths),
        CONVERT(INT, Total_Injuries),
        CONVERT(INT, Total_Missing),
        CONVERT(INT, Total_Missing_Description),
        CONVERT(FLOAT, Total_Damage),
        CONVERT(INT, Total_Damage_Description),
        CONVERT(INT, Total_Houses_Destroyed),
        CONVERT(INT, Total_Houses_Damaged),
        Pais.PaisID,
        Fecha.FechaID
    FROM Temporal,Pais,Fecha
    where Temporal.Country = Pais.Pais AND Temporal.Year = Fecha.Year;
    '''

    connection = conexiondb()

    cursor = connection.cursor()
    cursor.execute(sql)

    connection.commit()
    connection.close()
    print("Datos cargados correctamente al modelo.")

# Función para realizar consultas
def realizar_consultas():
    connection = conexiondb()
    cursor = connection.cursor()

    sql1 = '''
    SELECT 'Pais' AS Tabla, COUNT(*) AS Total FROM Pais
    UNION ALL
    SELECT 'Fecha' AS Tabla, COUNT(*) AS Total FROM Fecha
    UNION ALL
    SELECT 'Tsunami' AS Tabla, COUNT(*) AS Total FROM Tsunami;
    '''

    sql2 = '''
    SELECT Year, COUNT(*) AS Cantidad
    FROM Fecha F
    JOIN Tsunami T ON F.FechaID = T.FechaID
    GROUP BY Year
    ORDER BY Year;
    '''

    sql3 = '''
    SELECT DISTINCT P.Pais, F.Year
    FROM Pais P
    JOIN Tsunami T ON P.PaisID = T.PaisID
    JOIN Fecha F ON T.FechaID = F.FechaID
    ORDER BY P.Pais, F.Year;
    '''

    sql4 = '''
    SELECT P.Pais, AVG(T.TotalDamage) AS PromedioDamage
    FROM Pais P
    JOIN Tsunami T ON P.PaisID = T.PaisID
    WHERE P.Pais IS NOT NULL and T.TotalDamage IS NOT NULL
    GROUP BY P.Pais;
    '''

    sql5 = '''
    SELECT TOP 5 P.Pais, SUM(T.TotalDeaths) AS TotalMuertes
    FROM Pais P
    JOIN Tsunami T ON P.PaisID = T.PaisID
    GROUP BY P.Pais
    ORDER BY TotalMuertes DESC;
    '''

    sql6 = '''
    SELECT TOP 5 F.Year, SUM(T.TotalDeaths) AS TotalMuertes
    FROM Fecha F
    JOIN Tsunami T ON F.FechaID = T.FechaID
    GROUP BY F.Year
    ORDER BY TotalMuertes DESC;
    '''

    sql7 = '''
    SELECT TOP 5 F.Year, COUNT(*) AS TotalTsunamis
    FROM Fecha F
    JOIN Tsunami T ON F.FechaID = T.FechaID
    GROUP BY F.Year
    ORDER BY TotalTsunamis DESC;
    '''

    sql8 = '''
    SELECT TOP 5 P.Pais, SUM(T.TotalHousesDestroyed) AS CasasDestruidas
    FROM Pais P
    JOIN Tsunami T ON P.PaisID = T.PaisID
    GROUP BY P.Pais
    ORDER BY CasasDestruidas DESC;
    '''

    sql9 = '''
    SELECT TOP 5 P.Pais, SUM(T.TotalHousesDamaged) AS CasasDanadas
    FROM Pais P
    JOIN Tsunami T ON P.PaisID = T.PaisID
    GROUP BY P.Pais
    ORDER BY CasasDanadas DESC;
    '''

    sql10 = '''
    SELECT P.Pais, AVG(T.MaximumWaterHeight) AS PromedioAlturaAgua
    FROM Pais P
    JOIN Tsunami T ON P.PaisID = T.PaisID
    WHERE P.Pais IS NOT NULL AND T.MaximumWaterHeight IS NOT NULL
    GROUP BY P.Pais;
    '''

    consultas = [sql1,sql2,sql3,sql4,sql5,sql6,sql7,sql8,sql9,sql10]
    
    connection = conexiondb()
    cursor = connection.cursor()

    with open("resultados_consulta.txt", 'w') as f:
        for i, consulta in enumerate(consultas, start=1):
            try:
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                f.write(f"Resultados de la consulta {i}:\n")
                for resultado in resultados:
                    f.write(str(resultado) + '\n')
                f.write("\n")
            except Exception as e:
                print(f"Error al ejecutar la consulta {i}: {str(e)}")

    connection.close()

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

