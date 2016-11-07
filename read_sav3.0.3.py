#!/usr/bin/python

import pdb
import getopt
import numpy
from sqlalchemy import types 
from sqlalchemy import create_engine, exc 
from sqlalchemy.sql import text
from sqlalchemy import MetaData, Table,Column,ForeignKey
from sqlalchemy.dialects.mysql import TINYTEXT, ENUM
import logging
import re, glob, os.path, sys, inspect
import pandas as pd
from csv import reader as csvreader 
import warnings
import savReaderWriter
import sys, traceback

#~ SAVRW_DISPLAY_WARNS = warn
logging.basicConfig(filename='error.log',filemode='w', level=logging.WARN)
logger = logging.getLogger('sqlalchemy.engine')
#~ logger.setLevel(logging.WARN)

def line_no():
	"""Returns the current line number in our program."""
	return inspect.currentframe().f_back.f_lineno

def first_substring(strings, substring):
	"""From Stack Overflow http://stackoverflow.com/a/2171094 """
	return next((i for i, string in enumerate(strings) if substring in string),-1)

log_level = 'ERROR'
health_table = pd.DataFrame()
health_vars = []



def main(argv):
	global log_level
	global filepath	
	global health_table
	filepath = '/media/data/'
	filename = []
	global health_var_file
	global health_vars
	usemessage = os.path.basename(__file__) + " [--filename <file_path>] --secure <health_var_file> [--help] [--loglevel <loglevel>]"

	try : 
		opts, args = getopt.getopt(argv,'hl:f:s:',["help", "debug","filename=" ,'secure=', 'loglevel='])
	except getopt.GetoptError :
		print usemessage
		sys.exit(2)
	for opt, arg in opts :
		if opt in ('-h','--help') :
			print usemessage
			#~ return true
		if opt in ('--filename') :
			filename = arg
		if opt in ('--secure','-s'):
			health_var_file = arg
		if opt in ('--loglevel','-l') :
			log_level = arg
	
	try :
		health_var_file
	except :
		health_var_file = glob.glob(filepath + '/health*.csv')
		try : 
			health_var_file = health_var_file[0]
		except :
			print "no health variable file information found."

	if log_level != 'ERROR' :
		logger.setLevel(log_level)

	files = []
	if filename :
		files.append(filename)
	else :
		files = glob.glob(filepath + '/*.sav')

	built_tables = []
	if health_var_file:
		health_vars = health_vals()
	if files[0] :
		memberbase_idx = first_substring(files,'member')
		memberbase_filename = files.pop(memberbase_idx)
		files.insert(0,memberbase_filename)
		for filename in files :
			try :
				tablename = readfile(filename)
				if tablename :
					built_tables.append(tablename)
			except ValueError, e:
				logger.error("Line %s : some kind of error in %s : %s" % (line_no(), filename, str(e)))
			except TypeError, e:
				#~ pdb.set_trace()
				logger.error("Line %s : some kind of error in %s : %s" % (line_no(), filename, str(e)))
	#~ pdb.set_trace()
	if built_tables[0] :
		for table in built_tables :
			table_structure(table)
	
	if health_table.any().any() :
		health_table.to_sql('health',engine,if_exists='replace',index=True,dtype={'CODE2':types.String(10)})
	return True
	
def health_vals() :
	health_var_names = []
	if health_var_file :
		health_vars = glob.glob(filepath + health_var_file) 
		with open(health_vars[0]) as csvfile:
			for row in csvreader(csvfile) :
				health_var_names = health_var_names + row
	health_var_names = [var.upper().strip() for var in health_var_names]
	return health_var_names	

def anonymize(table,tablename) :
	memberbase = pd.read_sql_table('memberbase',engine,columns=['CODE1','CODE2']) 
	memberbase['CODE1'] = memberbase['CODE1'].apply(str.strip)
	table['ID.name'] = table['ID.name'].apply(str.strip)
	if memberbase.any().any() :
		print "trying to anonymize %s" % tablename
		panel = memberbase[['CODE1','CODE2']]
		joined_table = table.merge(panel,how='left',right_on='CODE1',left_on="ID.name")
	failed = table[pd.isnull(joined_table['CODE2'])]
	print "CODE2 lookup succeeded for %s records" % len(joined_table.index)
	print "CODE2 lookup failed for %s records" % len(failed.index)
	return table,failed	


survey_code_reg = re.compile('([0-9]{2})_?([0-9]{2})')

def relevant_health(health_vars,tablename) :
	
	survey_name = survey_code_reg.search(tablename)
	survey_name = survey_name.group()
	relevant = [var for var in health_vars if var.startswith('Q' + survey_name) ] 
	
	return relevant
		
	
def extract_health(health_vars,table) :
	'''subtract health data from table, add to "health_table"'''
	global health_table
	temp_health_table = pd.DataFrame()
	combined = r"(\b" + r"\b)|(\b".join(health_vars) + r"\b)"
	for column in table.columns :
		if re.match(combined,column) :
			health_var = table[column]
			temp_health_table[column] = health_var
			table.drop(column,inplace = True, axis=1 )
	if health_table.empty :
		health_table = temp_health_table
	else :
		health_table = health_table.join(temp_health_table,how="outer")
	return table
	

def SQL_connect() :

	engine = create_engine('mysql://{user}:{passwd}@{host}/{db}'.format(host = 'localhost',
		user = 'python',
		passwd = 'mcmmpython',
		db = 'mcmdev'))
	return engine

engine = SQL_connect()

def build_schema(table):
	schema = {}
	for column in table.select_dtypes(include=[numpy.number]) :
		if column == 'CODE2' :
			schema[column] = types.String(10)
			#~ pdb.set_trace()
		elif 3000 >= table[column].max() >= 1920 :
			schema[column] = 'date'
		else :
			schema[column] = types.SmallInteger
	for column in table.select_dtypes(exclude=[numpy.number]) :
		if column == 'CODE2' :
			schema[column] = types.String(10)
		elif table[column].dtype.name == 'category' :
			catlist = [str(x) for x in table[column].cat.categories.tolist()]
			schema[column] = types.Enum(*catlist)
		else :
			schema[column] = types.Text
	schema['CODE2'] = types.String(10)

	return schema


def build_table(filename,table):
	tablename=filename.split('/')[-1]
	tablename=tablename.split('.')[0:-1]
	if isinstance( tablename, (list, tuple)) :
		tablename='_'.join(tablename)
	questions = [col for col in table.columns if col.startswith('Q')]

	if not questions :
		if tablename == 'memberbase' :
			if 'CODE2' in table:
				table['CODE2'] = table['CODE2'].astype('int').astype('str')
				table.set_index('CODE2',inplace=True)

			return table, 'memberbase', None
		else :
			is_memberbase = raw_input('Does %s contain memberbase information? (y/n):' % filename)		
			if is_memberbase.lower() == 'y' :
				return table,'memberbase' 
	# Generate a list of bad columns, names indicate data are not survey answers

	if 'ID.format' in table :
		table,failed = anonymize(table,tablename)
	cols = [col for col in table.columns if ( col.endswith('SUM') or ( not col.startswith('Q') and col.upper() != 'CODE2') )]
	table.drop(cols,inplace = True, axis=1 )
	if cols[0]: 
		logger.info	("Columns %s omited from %s because they contained internal or sensitive data" % (cols,filename))
	index = next(( col for col in table.columns if col.upper() == 'CODE2' ),None)
	for column in table.select_dtypes(include=[numpy.number]) :
		if table[column].max() <= 99 :
			try : 
				table[column] = table[column].astype('int').astype('str').astype('category')
			except ValueError, e :
				logger.warn("%s in %s could not be set as pandas dtype 'category' because: %s" % (column, filename, str(e)) )
		elif table[column].max < 32767 :
			try :
				table[column] = table[column].astype('short')
			except :
				logger.warn("%s in %s could not be set as pandas dtype 'short'" % (column, filename) )
	if index == None :
		logger.warn("table %s does not contain the indexing column 'CODE2' and cannot be combined with other tables" % tablename)

	else :
		if index and (index != 'CODE2' ):
			table.rename(columns={index:'CODE2'},inplace=True)
        if 'CODE2' in table :
			table['CODE2'] = table['CODE2'].astype('int').astype('str')
			table.set_index('CODE2',inplace=True)
	try: 
		schema = build_schema(table)
	except ValueError, e: 
		print str(e)
	return table,tablename,schema

def readfile(filename):
	with savReaderWriter.SavReaderNp(filename,recodeSysmisTo=99) as sNp :
		try :
			table = pd.DataFrame(sNp.to_structured_array())
		except ValueError, e :
			logger.error("line %s: %s %s" % (line_no(), filename, str(e)) )
			return
		except TypeError, e :
			logger.error("line %s: %s %s" % (line_no(), filename, str(e)) )
			return 
		logger.info("Attempting to process file %s" % filename)
		#~ pdb.set_trace()
		table,tablename,schema = build_table(filename,table)
		if tablename != 'memberbase' and health_vars:
			table_health = relevant_health(health_vars,tablename)
			if table_health : 
				table = extract_health(table_health,table)
		
		success = False
		metadata = None
		if schema :
			schema = schema
		else :
			schema = {'CODE2':types.String(10)}
		try:

			table.to_sql(tablename,engine,schema=metadata,if_exists='replace',index=True,dtype=schema)
			success = True
			print "table %s saved to database\n" % filename
		except pd.core.groupby.DataError, e:
			logger.error("DataError: Line %s: %s for %s" % (line_no(), filename, str(e)) )
		except exc.IntegrityError, e:
			logger.error("Integrity Error: Line %s: %s for %s" % (line_no(), str(e)))

			print '-'*60
			traceback.print_exc(file=sys.stdout)
			print '-'*60
		except AttributeError, e :
			logger.error("Attribute Error: Line %s: %s for %s" % (line_no(), filename, str(e)) )

			print '-'*60
			traceback.print_exc(file=sys.stdout)
			print '-'*60
		#~ pdb.set_trace()
		if table.index.name == 'CODE2' and success:
			return tablename

def table_structure(tablename):
	connection = engine.connect()
	query = "ALTER TABLE %s ADD PRIMARY KEY(`CODE2`);" % tablename
	#~ pdb.set_trace()
	try :
		connection.execute(query)
	except exc.SQLAlchemyError, e:
		logger.error( str(e))
	if tablename != 'memberbase' :
		query = "ALTER TABLE %s ADD FOREIGN KEY (CODE2) REFERENCES memberbase(CODE2) ON DELETE CASCADE ON UPDATE CASCADE;" % tablename
		try :
			connection.execute(query)
			print "table %s succesfully connected" % tablename
		except exc.SQLAlchemyError, e:
			logger.error( str(e))

main(sys.argv[1:])
