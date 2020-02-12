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
	c.execute("INSERT INTO Paikka (paikkaNimi) VALUES (?)",[paikka])
	print("Paikka lisätty")
 
def lisaa_asiakas(asiakasNimi):
	c = db.cursor()
	c.execute("INSERT INTO Asiakas (asiakasNimi) VALUES (?)",[asiakasNimi])
	print("Asiakas lisätty")
 
def lisaa_paketti(seurantaKoodi, asiakasNimi):
    c = db.cursor()
    c.execute("SELECT id FROM Asiakas WHERE asiakasNimi=?",[asiakasNimi])
    asiakasId = c.fetchone()
    if asiakasId != None:
        c.execute("INSERT INTO Paketti (seurantaKoodi, asiakasId) VALUES (?,?)",[seurantaKoodi,asiakasId[0]])
        print("Paketti asiakkaalle lisätty")
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
		c.execute("INSERT INTO Tapahtuma (pakettiId, paikkaId, tapahtumaKuvaus, paivamaara) VALUES (?,?,?,?)",[pakettiId[0],paikkaId[0],kuvaus,d])
		print("Tapahtuma lisätty")

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
    
	c.execute("SELECT id FROM Asiakas WHERE asiakasNimi=?",[nimi])
	asiakasId = c.fetchone()
 
	c.execute("SELECT id ,seurantaKoodi FROM Paketti WHERE asiakasId=?",[asiakasId[0]])
	data = c.fetchall()
	for d in data:
		PakettiId = d[0]
		seurantaKoodi = d[1]
		c.execute("SELECT COUNT(Tapahtuma.id) FROM Tapahtuma WHERE pakettiId=? ",[PakettiId])
		total = c.fetchone()
		if total == None:
			print("Tapahtumia ei löytynyt")
		else:
			print(seurantaKoodi,",",total[0],"tapahtumaa")
      

	#c.execute("SELECT Paketti.seurantaKoodi, COUNT(Tapahtuma.id) FROM Asiakas A LEFT JOIN Paketti ON Paketti.asiakasId=A.id LEFT JOIN Tapahtuma ON Paketti.id=Tapahtuma.pakettiId WHERE A.asiakasNimi=? GROUP BY Paketti.seurantaKoodi",[nimi])
	#total = c.fetchone()
	#if total == None:
	#	print("Tapahtumia ei löytynyt")
	#else:
	#	print(seurantaKoodi,",",total[0],"tapahtumaa")
        
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
            
	stop = time()
	print("Lisätty 1000000 tapahtumaa. Aikaa kului",stop-start,"sekuntia")
 
	start = time()
	for value in range(1000): 
		c.execute("SELECT id ,seurantaKoodi FROM Paketti WHERE asiakasId=?",[random.randrange(1000)])
		data = c.fetchall()
		for d in data:
			PakettiId = d[0]
			seurantaKoodi = d[1]
			c.execute("SELECT COUNT(Tapahtuma.id) FROM Tapahtuma WHERE pakettiId=? ",[PakettiId])

		#c.execute("SELECT Paketti.seurantaKoodi, COUNT(Tapahtuma.id) FROM Asiakas A LEFT JOIN Paketti ON Paketti.asiakasId=A.id LEFT JOIN Tapahtuma ON Paketti.id=Tapahtuma.pakettiId WHERE A.id=? GROUP BY Paketti.seurantaKoodi",[value])
     
	stop = time()
	print("Haettu asiakkaan 1000 tapahtumaa. Aikaa kului",stop-start,"sekuntia")
 
	start = time()
	for value in range(1000):
		c.execute("SELECT * FROM Tapahtuma LEFT JOIN Paketti ON Paketti.id=Tapahtuma.pakettiId LEFT JOIN Paikka ON Paikka.id=Tapahtuma.paikkaId WHERE Paketti.id=?",[value])
     
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
		try:
			lisaa_paikka(paikka)
		except sqlite3.IntegrityError:
			print("Kyseinen paikka löytyy jo tietokannasta")
	elif selection == "3":
		print("Anna asiakkaan nimi:")
		asiakasNimi = input()
		try:
			lisaa_asiakas(asiakasNimi)
		except sqlite3.IntegrityError:
			print("Kyseinen nimi löytyy jo tietokannasta")
	elif selection == "4":
		print("Anna paketin seurantakoodi:")
		seurantaKoodi = input()
		print("Anna asiakkaan nimi:")
		asiakasNimi = input()
		try:
			lisaa_paketti(seurantaKoodi,asiakasNimi)
		except sqlite3.IntegrityError:
			print("Samalla seurantakoodilla oleva paketti on jo järjestelmässä")
	elif selection == "5":
		print("Anna paketin seurantakoodi:")
		seurantaKoodi = input()
		print("Anna tapahtuman paikka:")
		paikka = input()
		print("Anna tapahtuman kuvaus:")
		kuvaus = input()
		try:
			lisaa_tapahtuma(seurantaKoodi,paikka,kuvaus)
		except sqlite3.Error as e:
			print(type(e).__name__) 
	elif selection == "6":
		print("Anna paketin seurantakoodi:")
		seurantaKoodi = input()
		try:
			hae_tapahtumat(seurantaKoodi)
		except sqlite3.Error as e:
			print(type(e).__name__) 
	elif selection == "7":
		print("Anna asiakkaan nimi:")
		nimi = input()
		try:
			hae_paketit(nimi)
		except sqlite3.Error as e:
			print(type(e).__name__) 
	elif selection == "8":
		print("Anna paikan nimi:")
		paikka = input()
		print("Anna päivämäärä:")
		pvm = input()
		try:
			hae_tapahtumat_pvm(paikka,pvm)	
		except sqlite3.Error as e:
			print(type(e).__name__) 
	elif selection == "9":
		try:
			aja_tehokkuustesti()
		except sqlite3.Error as e:
			print(type(e).__name__) 
	elif selection == "10":
		print("Bye!")
		break
	else:
	 print("Valinta pitää olla väliltä 1-10")

