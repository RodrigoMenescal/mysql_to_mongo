from pymongo import MongoClient
import MySQLdb
import decimal
from decimal import *
from datetime import date


def connection():
	db = MySQLdb.connect(host="localhost", user="lab",passwd="xico", db="lab_mongo")
	cursor = db.cursor()
	return cursor
	
def nameTb(base):
	table=[]
	cursor=connection()
	cursor.execute( "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE '%"+base+"%'")
	for tabla in cursor:
		table.append(tabla[0])
	return table
		
def nameColunas(base):
	table = nameTb(base)
	cursor=connection()
	for tb in table:
		colum = []
		cursor.execute( "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA LIKE '%"+base+"%' AND TABLE_NAME = '"+tb+"' ")
		for col in cursor:
			colum.append(col[0])
		val = valores(tb)
		arr=crearDict(colum,val)
		insertMongo(arr,tb)
		val=None
		arr=None
		colum=None

def valores(nombreTabla):
	valores=[]
	cursor=connection()
	cursor.execute("SELECT * FROM  "+nombreTabla+";")
	for val in cursor:
		valores.append(val)
	return valores
	
	
			
def crearDict(colum,valores):
	arrJson = []
	for val in valores:

		diccionario = dict.fromkeys(colum)
		for i in range(len(colum)):
			#print(type(val[i]))
			if isinstance(val[i],Decimal):#problemas in decimal en mongo 
				value = float(val[i])# conversion to Float
			elif isinstance(val[i],str):#problemas in string
				value = val[i].decode('ascii','ignore')
			elif isinstance(val[i],date):
				value = str(val[i])
			else :
				value= val[i]
			diccionario[colum[i]] = value
		arrJson.append(diccionario)
		diccionario = None
	return arrJson 


def insertMongo(objMysql,nameCollect):
	print(objMysql)
		
	
if __name__ =="__main__":
	nameColunas("lab_mongo")#name the basede 

