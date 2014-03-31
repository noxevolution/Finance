from peewee import *
import hashlib
from configobj import ConfigObj
import fail
import time
import requests
import xlrd, datetime, xlwt
import urllib
import urllib2
import dateutil.parser
import json
Config = json.loads(open('config.json', 'r').read())

class ABSRBAConfig (Model):
	link = TextField()
	processing = BooleanField(default=False)
	sheet = IntegerField()
	val_cell = IntegerField()

	class Meta:
		db = SqliteDatabase('db')

class ABSRBAResult (Model):
	identifier = CharField(64)
	date = DateField()
	link = TextField()
	value = FloatField()
	inserted = FloatField()

	class Meta: 
		db = SqliteDatabase('db')

def setup ():
        try:
                ABSRBAConfig.create_table()
        except Exception, e:
                print "\nException: ", e

        try:
                ABSRBAResult.create_table()
        except Exception, e:
                print "\nException: ", e
	
	

def extract_and_process (code):
	f = urllib2.urlopen(code.link)
	#data = f.read()
	#with open("buffer.xls", "wb") as buf:
		#buf.write(data)
	downloaded_book=xlrd.open_workbook('buffer.xls')
	downloaded_sheet=downloaded_book.sheet_by_index(int(code.sheet))
	
	for r in range(downloaded_sheet.nrows): 
		x=downloaded_sheet.cell(r,0).value
		y=downloaded_sheet.cell(r,int(code.val_cell)).value

		if isinstance(x,float) and isinstance(y, float): 
			x1=datetime.datetime(*xlrd.xldate_as_tuple(x,downloaded_book.datemode))
			print x,":", downloaded_book.datemode,":", x1

			print "[" + str(x1) + '] => ' + str(y)
			record = ABSRBAResult.create( link = code.link, date = x1, value = float(y), identifier = hashlib.sha256(code.link + str(y) + str(x1)).hexdigest(), inserted = float(time.time() * 1000) )


def FetchDataFromWeb (): 
	global Config
	try:
		code = ABSRBAConfig.select().where(ABSRBAConfig.processing != True).get()
	except:
		# Return false, there are no more records left
		return False
		pass
	else:
		code.processing = True
		#code.save()
		try:
			data = extract_and_process(code)
	
		except Exception as E:
			error = fail.Error.create(query = code.link, timestamp = int(time.time() * 1000), error = str(E))
			print "FAIL: "+code.link+"["+ str(E) + "]"
			pass
	return code

if __name__ == '__main__':

	setup()
	FetchDataFromWeb()
