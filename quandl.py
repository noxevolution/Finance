from peewee import *
import Quandl
import hashlib
from configobj import ConfigObj
import fail
import time
import requests
import dateutil.parser
import json
Config = json.loads(open('config.json', 'r').read())


class QuandlConfig (Model):
	identifier = CharField(64)
	code = CharField()
	description = TextField()
	frequency = CharField()
	dataset = CharField()
	timestamp = FloatField()
	processing = BooleanField(default=False)

	class Meta:
		db = SqliteDatabase('db')

class QuandlResult (Model):
	identifier = CharField(64)
	query = CharField(64)
	date = DateField()
	inserted = FloatField()
	key = TextField()
	value = FloatField()

	class Meta: 
		db = SqliteDatabase('db')

# Imports a set of codes from an .ini file. This will also create the SQL tables needed to house the data

def import_codes (code_file): 
	lInflationCodeList = []
	try:
		QuandlConfig.create_table()
		QuandlResult.create_table()
	except:
		pass
	config = ConfigObj(code_file)
	keys = config.keys()
	for frequency in keys: 
		print '[' + frequency + ']'
		for description in config[frequency]:
			code = config[frequency][description]
			dataset = (code_file.split('/').pop())
			entry = QuandlConfig.create( identifier = hashlib.sha256(dataset + frequency + code + description).hexdigest(), code = code, description = description, frequency = frequency, dataset = dataset, timestamp = -1 	)
			print description + ' => ' + code
			if description.find("Inflation Rate") >= 0:
				lInflationCodeList.append(code)
	return set(lInflationCodeList)
				

def FetchDataFromWeb (pInflationCodeList): 
	global Config
	try:
		code = QuandlConfig.select().where(QuandlConfig.processing != True).get()
	except:
		# Return false, there are no more records left
		return False
		pass
	else:
		code.processing = True
		code.save()
		for lcode in pInflationCodeList:
			for date in Config['quandl']['dates']:
			# Using offical Quandl Python Lib (you need to import quandl):
			# data = json.loads(Quandl.get(code.code, authtoken=Config['quandl']['authtoken'], trim_start=date[0],trim_end=date[1])) 
			# Using the offical Quandl REST/JSON API (much easier): 
			
				try:
					lReqst = 'http://www.quandl.com/api/v1/datasets/'+lcode+'.json?&auth_token='+Config['quandl']['authtoken']+'&trim_start='+date[0]+'&trim_end='+date[1]
					print lReqst
					data = requests.get(lReqst).json()
					for pair in data['data']:
						print data['name'] + "[" + pair[0] + '] => ' + str(pair[1])
						QuandlResult.create(identifier =  hashlib.sha256(code.identifier + data['name']).hexdigest(), query = code.identifier, date = dateutil.parser.parse(pair[0]), inserted = int(time.time() * 1000), value = float(pair[1]), key = data['name'])
				except Exception as E:
					error = fail.Error.create(query = code.identifier, timestamp = int(time.time() * 1000), error = str(E))
					print str(E)
					pass
	return code

if __name__ == '__main__':

	# Import a set of codes contained in an ini file, into the SQL database
	# import_codes('AusSocietyCodes')
	lInflationCodeList = import_codes('AusEconomicCodes')
	print "\n=====================[ CODES]======================\n"
	print lInflationCodeList
	print "="*100
	
	FetchDataFromWeb(lInflationCodeList)
