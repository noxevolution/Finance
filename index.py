import quandl
import fail
import abs_rba
import google_trends
import google_finance
import yahoo_finance

modules = {	'quandl': quandl.FetchDataFromWeb, 'abs_rba': abs_rba.FetchDataFromWeb, 'google_finance': google_finance.FetchDataFromWeb, 'yahoo_finance': yahoo_finance.FetchDataFromWeb }

def hasNext (): 
	for module in modules:
		if modules[module] != False:
			return True
	return False

if __name__ == '__main__':
	try:
		fail.Error.create_table()
	except:
		pass

	prev = None
	while hasNext():
		for module, FetchDataFromWeb in modules.iteritems():
			if FetchDataFromWeb == False:
				continue
			code = FetchDataFromWeb();

			if code == False:
				print module + " is complete"
				modules[module] = False

	
	print "Done..."
