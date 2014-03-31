from peewee import *
import hashlib
from configobj import ConfigObj
import fail
import time
import requests
import dateutil.parser
import json
import datetime

Config = json.loads(open('config.json', 'r').read())

class GoogleTrendsConfig (Model):
	query = CharField()
	processing = BooleanField(default=False)

	class Meta:
		db = SqliteDatabase('db')

class GoogleTrendsResult (Model):
	identifier = CharField(64)
	date = DateField()
	query = CharField()
	value = FloatField()
	inserted = FloatField()

	class Meta: 
		db = SqliteDatabase('db')

def setup ():
	try:
		GoogleTrendsConfig.create_table()
	except Exception, e:
		print "\nException: ", e
		
	try:
		GoogleTrendsResult.create_table()
	except Exception, e:
		print "\nException: ", e
		
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

def FetchDataFromWeb (): 
	global Config
	try:
		code = GoogleTrendsConfig.select().where(GoogleTrendsConfig.processing != True).get()
	except:
		# Return false, there are no more records left
		return False
		pass
	else:
		#code.processing = True
		lStartDateList = SplitDateIntoString(raw_input("Enter the Start Date [in dd/mm/yyyy format] : "))
		lEndDateList = SplitDateIntoString(raw_input("Enter the End Date [in dd/mm/yyyy format] : "))
		
		code.start = unicode(datetime.date(int(lStartDateList[2]), int(lStartDateList[1]), int(lStartDateList[0])))
		code.end = unicode(datetime.date(int(lEndDateList[2]), int(lEndDateList[1]), int(lEndDateList[0])))
		#code.save()
		
		delete_quary = GoogleTrendsResult.delete().where(GoogleTrendsConfig.processing != True)
		delete_quary.execute()
		
		try:
			data = requests.get('http://google-trends.herokuapp.com/' + Config['google_trends']["email"] + '/' + Config['google_trends']["password"] + '/' + code.query).json()

			if data['error'] != None:
				raise Exception(data.error)

			for result in data['results']:
				val = result[code.query]
				print code.query + '[' + result['date'] + '] => ' + str(val)
				GoogleTrendsResult.create(identifier =  hashlib.sha256(code.query + code.query + result['date']).hexdigest(), query = code.query, date = dateutil.parser.parse(str(result['date'])), inserted = int(time.time() * 1000), value = float(val))
		except Exception as E:
			error = fail.Error.create(query = code.query, timestamp = int(time.time() * 1000), error = str(E))
			print str(E)
			pass
	return code

if __name__ == '__main__':

	setup()
	FetchDataFromWeb ()
