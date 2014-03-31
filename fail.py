from peewee import *

class Error (Model):
	query = CharField(64)
	timestamp = FloatField()
	error = TextField()

	class Meta:
		db = SqliteDatabase('db')

