import sqlite3
import sys
import os
import csv
import zipfile
import StringIO
import datetime

db = sqlite3.connect(sys.argv[1])
cursor = db.cursor()
'''
parses the operday and hour columns in the zonalload table to create a DateUTC
column so it is easier to query the zonlload and temperature tables through
a common data point
'''
def parse_to_utc(operday,hour):
    if hour[:2] == "24":
        convertedtime = datetime.datetime.strptime(operday + " 00:00", "%m/%d/%Y %H:%M")\
                        + datetime.timedelta(days=1)\
                        + datetime.timedelta(hours=6)
        return convertedtime.strftime("%Y-%m-%d %H:%M:%S")
    else:
        btime = datetime.datetime.strptime(operday + " " + hour, "%m/%d/%Y %H:%M") + datetime.timedelta(hours=6)
        return btime.strftime("%Y-%m-%d %H:%M:%S")
'''
rounds the utc of the weather data so it can match the utc of the zonalload,
making it easier for querying
'''
def round_utc(utc):
    rtime = datetime.datetime.strptime(utc[:13]+":00:00", "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours =1)
    return rtime.strftime("%Y-%m-%d %H:%M:%S")
'''
inserts zonalload values into the zonaload table
'''
def insert_zonal_load(x):
    reader = csv.DictReader(x)
    for d in reader:
        datalist = [d[i] for i in reader.fieldnames]
        datalist.append(parse_to_utc(datalist[0],datalist[1]))
        cursor.executemany('INSERT OR REPLACE INTO zonalload VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',[tuple(datalist)])
    db.commit()

'''
inserts weather data into the appropriate table
'''
def insert_weather_data(x,city):
    reader = csv.DictReader(x)
    for d in reader:
        datalist = [d[i] for i in reader.fieldnames]
        rel_data = (datalist[13],(round_utc(datalist[13])),datalist[1])
        cursor.executemany('INSERT OR REPLACE INTO '+ city +' VALUES (?,?,?)',[rel_data])
    db.commit()
'''
extracts the data from the csv files
'''
def import_data(region):
    for name in os.listdir("weather_data"):#iterate through files in directory
        if name.endswith(region + "_2014.zip") or name.endswith(region + "_2015.zip"):#check if zipfile
            with zipfile.ZipFile("weather_data/"+name) as z:#tempdefine zip as z
                for f in z.namelist():#iterate through files in zip
                    if f.endswith("csv"):#checks for csv file
                        data = StringIO.StringIO(z.read(f))#creates string buffer/memory files
                        try:
                            insert_weather_data(data, region)
                        except:
                            print("Error Data Insertion: " + region)
    print(region + " data import done!")
'''
creates the database tables
'''
def create_tables():

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS zonalload(OperDay TEXT,
                                  HourEnding TEXT,
                                  COAST REAL,
                                  EAST REAL,
                                  FAR_WEST REAL,
                                  NORTH REAL,
                                  NORTH_C REAL,
                                  SOUTHERN REAL,
                                  SOUTH_C REAL,
                                  WEST REAL,
                                  TOTAL REAL,
                                  DSTflag TEXT,
                                  DateUTC TEXT,
                                  PRIMARY KEY(DateUTC))''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS KDAL(
                                  DateUTC TEXT,
                                  RoundedUTC TEXT,
                                  TemperatureF REAL,
                                  PRIMARY KEY(RoundedUTC))''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS KHOU(
                                  DateUTC TEXT,
                                  RoundedUTC TEXT,
                                  TemperatureF REAL,
                                  PRIMARY KEY(RoundedUTC))''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS KSAT(
                                  DateUTC TEXT,
                                  RoundedUTC TEXT,
                                  TemperatureF REAL,
                                  PRIMARY KEY(RoundedUTC))''')

    db.commit()
    for name in os.listdir("system_load_by_region"):#iterate through files in directory
        if name.endswith("zip"):#check if zipfile
            with zipfile.ZipFile("system_load_by_region/"+name) as z:#tempdefine zip as z
                for f in z.namelist():#iterate through files in zip
                    if f.endswith("csv"):#checks for csv file
                        data = StringIO.StringIO(z.read(f))#creates string buffer/memory files
                        try:
                            insert_zonal_load(data)
                        except:
                            print("Error Duplicate Data zonalload")
    print("zonal_load import done!")
    import_data('KDAL')
    import_data('KHOU')
    import_data('KSAT')

create_tables()
