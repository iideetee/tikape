import sqlite3
import sys
import datetime
from time import time
import random

db = sqlite3.connect("database.db")
db.isolation_level = None

def luo_tietokanta():
	c = db.cursor()
	c.execute("CREATE TABLE Asiakas (id INTEGER PRIMARY KEY, asiakasNimi TEXT UNIQUE)")
	c.execute("CREATE TABLE Paikka (id INTEGER PRIMARY KEY, paikkaNimi TEXT UNIQUE)")
	c.execute("CREATE TABLE Paketti (id INTEGER PRIMARY KEY, seurantaKoodi TEXT UNIQUE, asiakasId INT)")
	c.execute("CREATE TABLE Tapahtuma (id INTEGER PRIMARY KEY, pakettiId INT, paikkaId INT, tapahtumaKuvaus TEXT, paivamaara TEXT)")
	print("Tyhjät taulut luotu kantaan...")
 
	createSecondaryIndex = "CREATE INDEX index_asiakas_nimi ON Asiakas(asiakasNimi)"
	c.execute(createSecondaryIndex)
 
	createSecondaryIndex = "CREATE INDEX index_seurantakoodi ON Paketti(seurantaKoodi)"
	c.execute(createSecondaryIndex)
	print("Indexit lisätty...")

def nayta_toiminnot():
	print("TOIMINNOT")
	print("1. Luo tietokanta")
	print("2. Lisää uusi paikka")
	print("3. Lisää uusi asiakas")
	print("4. Lisää uusi paketti")
	print("5. Lisää uusi tapahtuma")
	print("6. Hae paketin tapahtumat")
	print("7. Hae paketit ja tapahtumien määrä")
	print("8. Hae paikan tapahtumien määrä tiettynä päivänä")
	print("9. Suorita tietokannan tehokkuustesti")
	print("10. Sulje ohjelma")

def lisaa_paikka(paikka):	
	c = db.cursor()
	c.execute("begin")
	try:
		c.execute("INSERT INTO Paikka (paikkaNimi) VALUES (?)",[paikka])
		c.execute("commit")
		print("Paikka lisätty!")
	except sqlite3.Error:
		print("Paikan lisäys ei onnistunut!")
		c.execute("rollback")

 
def lisaa_asiakas(asiakasNimi):
	c = db.cursor()
	c.execute("begin")
	try:
		c.execute("INSERT INTO Asiakas (asiakasNimi) VALUES (?)",[asiakasNimi])
		c.execute("commit")
		print("Asiakas lisätty!")
	except sqlite3.Error:
		print("Asiakkaan lisäys ei onnistunut!")
		c.execute("rollback")
 
def lisaa_paketti(seurantaKoodi, asiakasNimi):
	c = db.cursor()
	c.execute("SELECT id FROM Asiakas WHERE asiakasNimi=?",[asiakasNimi])
	asiakasId = c.fetchone()
	if asiakasId != None:
		c.execute("begin")
		try:
			c.execute("INSERT INTO Paketti (seurantaKoodi, asiakasId) VALUES (?,?)",[seurantaKoodi,asiakasId[0]])
			c.execute("commit")
			print("Paketti asiakkaalle lisätty")
		except sqlite3.Error:
			print("Paketin lisäys ei onnistunut!")
			c.execute("rollback")
	else:
		print("Asiakasta ei löytynyt")
        
def lisaa_tapahtuma(seurantaKoodi, paikka, kuvaus):
	today = datetime.datetime.now()

	d = today.strftime("%d.%m.%Y %H:%M")

	c = db.cursor()
	c.execute("SELECT id FROM Paketti WHERE seurantaKoodi=?",[seurantaKoodi])
	pakettiId = c.fetchone()
    
	c.execute("SELECT id FROM Paikka WHERE paikkaNimi=?",[paikka])
	paikkaId = c.fetchone()
 
	if pakettiId == None:
		print("Pakettia ei löytynyt") 
	elif paikkaId == None:
		print("Paikkaa ei löytynyt") 
	else:
		c.execute("begin")
		try:
			c.execute("INSERT INTO Tapahtuma (pakettiId, paikkaId, tapahtumaKuvaus, paivamaara) VALUES (?,?,?,?)",[pakettiId[0],paikkaId[0],kuvaus,d])
			c.execute("commit")
			print("Tapahtuma lisätty")
		except sqlite3.Error:
			print("Paketin lisäys ei onnistunut!")
			c.execute("rollback")

def hae_tapahtumat(seurantaKoodi):
    c = db.cursor()
    c.execute("SELECT * FROM Tapahtuma LEFT JOIN Paketti ON Paketti.id=Tapahtuma.pakettiId LEFT JOIN Paikka ON Paikka.id=Tapahtuma.paikkaId WHERE Paketti.seurantakoodi=?",[seurantaKoodi])
    tapahtumat = c.fetchall()
    if not tapahtumat:
        print("Tapahtumia ei löytynyt")
    else:
        for tapahtuma in tapahtumat:
            print(tapahtuma[4],", ", tapahtuma[9],", " ,tapahtuma[3])
            
def hae_paketit(nimi):
	c = db.cursor()
	c.execute("SELECT Paketti.seurantaKoodi, COUNT(Tapahtuma.id) FROM Asiakas A JOIN Paketti ON Paketti.asiakasId=A.id JOIN Tapahtuma ON Paketti.id=Tapahtuma.pakettiId WHERE A.asiakasNimi=? GROUP BY Paketti.seurantaKoodi",[nimi])
	total = c.fetchone()
	if total == None:
		print("Tapahtumia ei löytynyt")
	else:
		print(total[0],",",total[1],"tapahtumaa")
        
def hae_tapahtumat_pvm(paikka,pvm):
    c = db.cursor()
    c.execute("SELECT COUNT(Tapahtuma.id) FROM Tapahtuma LEFT JOIN Paikka ON Paikka.id=Tapahtuma.paikkaId WHERE paikka.paikkaNimi=? AND Tapahtuma.paivamaara LIKE ?",[paikka, '%'+pvm+'%'])
    tapahtumat = c.fetchall()
    if not tapahtumat:
        print("Tapahtumia ei löytynyt")
    else:
        for tapahtuma in tapahtumat:
            print("Tapahtumien määrä:",tapahtuma[0])
   
def aja_tehokkuustesti():
	c = db.cursor()
 
	start = time()
	c.execute("begin")
	try:
		for value in range(1000):
			c.execute("INSERT INTO Paikka (paikkaNimi) VALUES (?)",["P"+str(value)])
		c.execute("commit")
	except sqlite3.Error:
		print("Paikkojen lisäys ei onnistunut!")
		c.execute("rollback")
  
	stop = time()
	print("Lisätty 1000 paikkaa. Aikaa kului",stop-start,"sekuntia")
  
	start = time()
	c.execute("begin")
	try:
		for value in range(1000):
			c.execute("INSERT INTO Asiakas (asiakasNimi) VALUES (?)",["A"+str(value)])
		c.execute("commit")
	except sqlite3.Error:
		print("Asiakkaiden lisäys ei onnistunut!")
		c.execute("rollback")
  
	stop = time()
	print("Lisätty 1000 asiakasta. Aikaa kului",stop-start,"sekuntia")
  
	start = time()
	c.execute("begin")
	try:
		for value in range(1000):
			c.execute("INSERT INTO Paketti (seurantaKoodi, asiakasId) VALUES (?,?)",["FI0000"+str(value),value])
		c.execute("commit")
	except sqlite3.Error:
		print("Pakettien lisäys ei onnistunut!")
		c.execute("rollback")
  
  
	stop = time()
	print("Lisätty 1000 pakettia. Aikaa kului",stop-start,"sekuntia")
  
	start = time()
	c.execute("begin")
	try:
		for value in range(1000000):
			today = datetime.datetime.now()
			d = today.strftime("%d.%m.%Y %H:%M")
			c = db.cursor()
			c.execute("INSERT INTO Tapahtuma (pakettiId, paikkaId, tapahtumaKuvaus, paivamaara) VALUES (?,?,?,?)",[random.randrange(1000),random.randrange(1000),"nopeutta",d])
		c.execute("commit")
	except sqlite3.Error:
		print("Pakettien lisäys ei onnistunut!")
		c.execute("rollback")
            
	stop = time()
	print("Lisätty 1000000 tapahtumaa. Aikaa kului",stop-start,"sekuntia")
 
	start = time()
	for value in range(1000):
		c.execute("SELECT Paketti.seurantaKoodi, COUNT(Tapahtuma.id) FROM Asiakas A JOIN Paketti ON Paketti.asiakasId=A.id JOIN Tapahtuma ON Paketti.id=Tapahtuma.pakettiId WHERE A.id=? GROUP BY Paketti.seurantaKoodi",[value])
     
	stop = time()
	print("Haettu asiakkaan 1000 tapahtumaa. Aikaa kului",stop-start,"sekuntia")
 
	start = time()
	for value in range(1000):
		c.execute("SELECT * FROM Tapahtuma JOIN Paketti ON Paketti.id=Tapahtuma.pakettiId JOIN Paikka ON Paikka.id=Tapahtuma.paikkaId WHERE Paketti.id=?",[value])
     
	stop = time()
	print("Haettu 1000 paketin tapahtumaa. Aikaa kului",stop-start,"sekuntia")
 
            
nayta_toiminnot()
while True:
	print("Valitse toiminto:")
	selection = input()

	if selection == "1":
	  try:
	  	luo_tietokanta()
	  except sqlite3.OperationalError:
	  	print("Tietokanta on jo luotu")
	elif selection == "2":
		print("Anna paikan nimi:")
		paikka = input()
		lisaa_paikka(paikka)
	elif selection == "3":
		print("Anna asiakkaan nimi:")
		asiakasNimi = input()
		lisaa_asiakas(asiakasNimi)
	elif selection == "4":
		print("Anna paketin seurantakoodi:")
		seurantaKoodi = input()
		print("Anna asiakkaan nimi:")
		asiakasNimi = input()
		lisaa_paketti(seurantaKoodi,asiakasNimi)
	elif selection == "5":
		print("Anna paketin seurantakoodi:")
		seurantaKoodi = input()
		print("Anna tapahtuman paikka:")
		paikka = input()
		print("Anna tapahtuman kuvaus:")
		kuvaus = input()
		lisaa_tapahtuma(seurantaKoodi,paikka,kuvaus)
	elif selection == "6":
		print("Anna paketin seurantakoodi:")
		seurantaKoodi = input()
		hae_tapahtumat(seurantaKoodi) 
	elif selection == "7":
		print("Anna asiakkaan nimi:")
		nimi = input()
		hae_paketit(nimi)
	elif selection == "8":
		print("Anna paikan nimi:")
		paikka = input()
		print("Anna päivämäärä:")
		pvm = input()
		hae_tapahtumat_pvm(paikka,pvm)	
	elif selection == "9":
		aja_tehokkuustesti()
	elif selection == "10":
		print("Bye!")
		break
	else:
	 print("Valinta pitää olla väliltä 1-10")

