
##================================================[ IMPORTS MODULE ]================================================
import datetime
from peewee import *
import hashlib
from configobj import ConfigObj
import fail
import time
import requests
import dateutil.parser
import json
import pandas.io.data as web

##================================================[ GLOBAL DECLARATIONS ]===========================================

Config = json.loads(open('config.json', 'r').read())

##================================================[ CLASS DECLARATIONS ]============================================

class YahooFinanceConfig (Model):
	ticker = CharField()
	start = DateField()
	end = DateField()
	processing = BooleanField(default=False)

	class Meta:
		db = SqliteDatabase('db')

class YahooFinanceResult (Model):
	identifier = CharField(64)
	ticker = CharField()
	date = DateField()
	inserted = FloatField()
	open = FloatField()
	high = FloatField()
	low = FloatField()
	volume = FloatField()
	close = FloatField()

	class Meta: 
		db = SqliteDatabase('db')

##================================================[ MAIN CODING ]===================================================

def SplitDateIntoString(start_date):
	if start_date.find("/") < 0:
		if start_date.find("\\") < 0:
			if start_date.find("-") < 0:
				print "\nWrong date format!"
				return "0"
			else:
				return start_date.split("-")
		else:
			return start_date.split("\\")
	else:
		return start_date.split("/")
	

# Imports a set of codes from an .ini file. This will also create the SQL tables needed to house the data
def FetchDataFromWeb (): 
	global Config
	print "\nFetching data ..."
	try:
		code = YahooFinanceConfig.select().where(YahooFinanceConfig.processing != True).get()
	except:
		print "\nReturn false, there are no more records left"
		return False
		pass
	else:
		#code.processing = True
		
		lStartDateList = SplitDateIntoString(raw_input("Enter the Start Date [in dd/mm/yyyy format] : "))
		lEndDateList = SplitDateIntoString(raw_input("Enter the End Date [in dd/mm/yyyy format] : "))
		
		code.start = unicode(datetime.date(int(lStartDateList[2]), int(lStartDateList[1]), int(lStartDateList[0])))
		code.end = unicode(datetime.date(int(lEndDateList[2]), int(lEndDateList[1]), int(lEndDateList[0])))
		#code.save()
		
		delete_quary = YahooFinanceResult.delete().where(YahooFinanceConfig.processing != True)
		delete_quary.execute()
		
		f=web.DataReader(code.ticker, 'yahoo', code.start, code.end)
		for date, _value in f.iterrows():
			try:
				value = {
					'open': _value['Open'],
					'high': _value['High'],
					'low': _value['Low'],
					'volume': _value['Volume'],
					'close': _value['Close']
				}
				record = YahooFinanceResult.create(
						identifier = hashlib.sha256(code.ticker + str(date) + code.start + code.end).hexdigest(),
						ticker = code.ticker, 
						date = (dateutil.parser.parse(str(date))), 
						inserted = int(time.time() * 1000), 
						open = _value['Open'],
						high = _value['High'],
						low = _value['Low'],
						volume = _value['Volume'],
						close = _value['Close']
					)
				print code.ticker + "[" + str(date) + '] => ' + json.dumps(value)
			except Exception as E:
				error = fail.Error.create(query = code.ticker, timestamp = int(time.time() * 1000), error = str(E))
				print str(E)
				pass
	return code


if __name__ == '__main__':
	try:
		YahooFinanceResult.create_table()
	except Exception, e:
		print "\nException: ", e
	
	try:
		YahooFinanceConfig.create_table()
	except Exception, e:
		print "\nException: ", e
		
	FetchDataFromWeb()
	
