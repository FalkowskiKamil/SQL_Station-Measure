import sqlite3
from sqlite3 import Error
import csv
from sqlalchemy import create_engine


def create_connection(db_file):
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       return conn
   except sqlite3.Error as e:
       print(e)
   return conn

def execute_sql(conn, sql):
   try:
       c = conn.cursor()
       c.execute(sql)
   except Error as e:
       print(e)   

def add_stations(conn, stations):
   sql = '''INSERT INTO stations(station,latitude,longitude,elevation,name,country,state)
             VALUES(?,?,?,?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, stations)
   conn.commit()
   return cur.lastrowid

def add_measure(conn, measure):
   sql = '''INSERT INTO measure(station_id, station, date, precip, tobs)
             VALUES(?,?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, measure)
   conn.commit()
   return cur.lastrowid


def select_all(conn, table):
   cur = conn.cursor()
   cur.execute(f"SELECT * FROM {table}")
   rows = cur.fetchall()
   return rows

def select_where(conn, table, **query):
   cur = conn.cursor()
   qs = []
   values = ()
   for k, v in query.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
   rows = cur.fetchall()
   return rows

def update(conn, table, id, **kwargs):
   parameters = [f"{k} = ?" for k in kwargs]
   parameters = ", ".join(parameters)
   values = tuple(v for v in kwargs.values())
   values += (id, )
   sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
   try:
       cur = conn.cursor()
       cur.execute(sql, values)
       conn.commit()
       print("OK")
   except sqlite3.OperationalError as e:
       print(e)


def delete_where(conn, table, **kwargs):
   qs = []
   values = tuple()
   for k, v in kwargs.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   sql = f'DELETE FROM {table} WHERE {q}'
   cur = conn.cursor()
   cur.execute(sql, values)
   conn.commit()
   print("Deleted")

def delete_all(conn, table):
   sql = f'DELETE FROM {table}'
   cur = conn.cursor()
   cur.execute(sql)
   conn.commit()
   print("Deleted")

if __name__ == "__main__":

  create_stations_sql = """
   -- stations table
   CREATE TABLE IF NOT EXISTS stations (
      id integer PRIMARY KEY,
      station text,
      latitude text,
      longitude text,
      elevation text,
      name text,
      country text,
      state text
   --CONSTRAINT UC_stations UNIQUE (station)
   );
   """
  create_measure_sql = """
   -- measure table
   CREATE TABLE IF NOT EXISTS measure (
      id integer PRIMARY KEY,
      station_id integer NOT NULL,
      station VARCHAR(250) NOT NULL,
      date TEXT,
      precip TEXT,
      tobs text
   --FOREIGN KEY (station_id) REFERENCES stations (id)
   );
   """

db_file = "database.db"

conn = create_connection(db_file)
if conn is not None:
       execute_sql(conn, create_stations_sql)
       execute_sql(conn, create_measure_sql)
       conn.close()

with open("clean_stations.csv", newline="") as csvfile:
   reader=csv.DictReader(csvfile)
   for row in reader:
      table_row=list()
      for k,v in row.items():
         table_row.append(v)
      conn = create_connection("database.db")
      st_id=add_stations(conn,table_row)

with open("clean_measure.csv", newline="") as csvfile:
   reader=csv.DictReader(csvfile)
   for row in reader:
      table_row=list()
      conn=create_connection("database.db")
      search=(row['station'])
     
      with conn:
         for_index=select_where(conn, table='stations', station=search)
         table_row.append(for_index[0][0])      
      
      for k,v in row.items():
         table_row.append(v)
      conn = create_connection("database.db")
      ms_id=add_measure(conn,table_row)

engine = create_engine('sqlite:///database.db', echo=True)
conn = engine.connect()
conn.execute("SELECT * FROM stations LIMIT 5").fetchall()