#!/home/guest-user/.virtualenvs/dev/bin/python

import pdb
import getopt
import numpy
from sqlalchemy import schema as alchemyschema
from sqlalchemy import engine_from_config, exc, types
from sqlalchemy.sql import text
from sqlalchemy import MetaData, Table,Column,ForeignKey
from sqlalchemy.dialects.mysql import TINYTEXT, ENUM
import ConfigParser
import logging
import re, glob, sys, inspect
from os import path, environ
import pandas as pd
from csv import reader as csvreader 
import warnings
import savReaderWriter
import sys, traceback
import progressbar

sys.defaultencoding='utf-8'

scriptdir = path.dirname(path.realpath(sys.argv[0]))
homedir = environ['HOME']
desktopdir=homedir + '/Desktop/'

Config = ConfigParser.ConfigParser()
Config.read(scriptdir + '/config.ini')

log_level = 'WARN'

if Config.get('settings','debug') :
	log_level = 'INFO'

logging.basicConfig(filename=desktopdir +'load-error.log',filemode='w')

logger = logging.getLogger('sqlalchemy.engine')
logger.setLevel(log_level)

#used to record CODE1s we couldn't convert to CODE2s in anonymization
xlwriter = pd.ExcelWriter(desktopdir + 'Code1LookupFailed.xlsx',engine='xlsxwriter',options={'encoding':'unicode'})

sqlsettings = dict(Config.items('sqlalchemy'))

def SQL_connect() :
	try:
		engine = engine_from_config( sqlsettings )
	except exc.SQLAlchemyError, e:
		print e
		exit
	return engine

def line_no():
	"""Returns the current line number in our program."""
	return inspect.currentframe().f_back.f_lineno

def first_substring(strings, substring):
	"""From Stack Overflow http://stackoverflow.com/a/2171094 """
	return next((i for i, string in enumerate(strings) if substring in string),-1)


health_table = pd.DataFrame()
health_vars = []



def main(argv):
	global log_level
	global filepath	
	global health_table
	filepath = Config.get('filepaths','data')
	filename = []
	log_level = 'ERROR'
	global health_var_file
	health_var_file = Config.get('filepaths','health')
	global health_vars
	usemessage = path.basename(__file__) + " [--filename <file_path>] --secure <health_var_file> [--help] [--loglevel <loglevel>]"
	
	try : 
		opts, args = getopt.getopt(argv,'hl:f:s:',["help", "debug","filename=" ,'secure=', 'loglevel='])
	except getopt.GetoptError :
		print usemessage
		sys.exit(2)
	for opt, arg in opts :
		if opt in ('-h','--help') :
			print usemessage
			sys.exit(2)
		if opt in ('--filename') :
			filename = arg
		if opt in ('--secure','-s'):
			health_var_file = arg
		if opt in ('--loglevel','-l') :
			log_level = arg
		else :
			if Config.get('settings','debug'):
				log_level = 'INFO'
			elif Config.get('settings','loglevel'):
				log_level =  Config.get('settings','loglevel')
				
	
	try :
		health_var_file
	except :
		health_var_file = glob.glob(filepath + 'Health*.csv')
		try : 
			health_var_file = health_var_file[0]
		except :
			print "no health variable file information found."

	if log_level != 'ERROR' :
		logger.setLevel(log_level)

	files = []
	if filename :
		files.append(filename)
		files.append(glob.glob(filepath + '[mM]emberbase*.sav')[0])
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
				table = readut8file(filename)
			except ValueError, e:
				logger.error("Line %s : some kind of error in %s : %s" % (line_no(), filename, str(e)))
				continue
			except TypeError, e:
				#~ pdb.set_trace()
				logger.error("Line %s : some kind of error in %s : %s" % (line_no(), filename, str(e)))
				continue
			except exc.IntegrityError, e:
				logger.error("Line %s : some kind of error in %s : %s" % (line_no(), filename, str(e)))
				continue

			if table is None:
				pass
			else :
				try :
					tablename = write_to_db(table,filename)
				except exc.OperationalError, e:
					# todo: does this ever get used?
					print "\t%s failed.  Trying %s with backup parser\n" % (filename, filename)
					table = readbadfile(filename)
					if table is not None :
						tablename = write_to_db(table,filename)
					else : 
						raise
			if tablename :
				built_tables.append(tablename)

	
	if health_table.any().any() :
		try : 
			health_table.to_sql('health',engine,if_exists='replace',index=True,dtype={'CODE2':types.String(10)})
		except exc.SQLAlchemyError, e:
			logger.error(str(e))
	xlwriter.save()
	xlwriter.close()
	return True
	
def health_vals() :
	health_var_names = []
	if health_var_file :

		with open(health_var_file) as csvfile:
			for row in csvreader(csvfile) :
				health_var_names = health_var_names + row
	health_var_names = [var.upper().strip() for var in health_var_names]
	return health_var_names	

def anonymize(table,tablename) :
	preserve_dtypes = {}
	memberbase = pd.read_sql_table('memberbase',engine,columns=['CODE1','CODE2']) 
	if memberbase.any().any() :
		print "trying to anonymize %s" % tablename
		panel = memberbase[['CODE1','CODE2']]
		preserve_dtypes = table.dtypes.to_dict()
	try: 
		joined_table = table.merge(panel,how='inner',right_on='CODE1',left_on="ID.name")
		#~ for column in 
		## dtypes are set to OBJ after merge because no idea, so they have to be restored
		for column in joined_table.columns: 
			try:
				joined_table[column] = joined_table[column].astype(preserve_dtypes[column])
			except KeyError, e:
				#~ print "keytype could not be cast %s" % e
				pass
		#~ pdb.set_trace()
		
		# ID.name which could not be matched to CODE1 in memberbase
		failed = table[~table['ID.name'].isin(joined_table['ID.name'])]['ID.name'].to_frame()
		## to_frame because this is just a series, which doesn't have a to_excel method
		failed.to_excel(xlwriter,sheet_name=tablename,columns=['ID.name'])
	except:
		logger.error("ur anonymizing code is borked somewhere man")
		return False
	return joined_table,failed	

survey_code_reg = re.compile('([0-9]{2})_?([0-9]{2})')

def relevant_health(health_vars,tablename) :
	
	survey_name = survey_code_reg.search(tablename)
	survey_name = survey_name.group()
	relevant = [var for var in health_vars if var.startswith('Q' + survey_name) ] 
	return relevant
		
	
def extract_health(health_vars,table) :
	'''subtract health data from table, add to "health_table"'''
	if len(health_vars) < 1 :
		return table
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
	

engine = SQL_connect()

def build_schema(table):
	schema = {}
	
	#~ pdb.set_trace()
	for column in table.select_dtypes(include=[numpy.number]) :
		if column == 'CODE2' :
			schema[column] = types.String(10)
		else :
			schema[column] = types.SmallInteger
	for column in table.select_dtypes(exclude=[numpy.number]) :
		if column == 'CODE2' :
			schema[column] = types.String(10)
		elif table[column].dtype.name == 'category' :
			catlist = [str(x.upper()) if isinstance(x,basestring) else x for x in table[column].cat.categories.tolist()]
			if '99' not in catlist :
				catlist.append('99')
			#remove duplicates
			catlist = set(catlist)
			#make it a list object again
			catlist = list(catlist)
			try :
				schema[column] = types.Enum(*catlist)
			except TypeError, e:
				logger.info("%s has objecttype that can't be made enum: %s" % (column, str(e)))
				schema[column] = types.SmallInteger
		else :
			#~ pdb.set_trace()
			schema[column] = types.Text
	schema[u'CODE2'] = types.String(10)
	#~ pdb.set_trace()
	return schema


def build_table(filename,table):
	tablename=filename.split('/')[-1]
	tablename=tablename.split('.')[0:-1]
	if isinstance( tablename, (list, tuple)) :
		tablename='_'.join(tablename)
	
	questions = [col for col in table.columns if col.startswith('Q')]

	if not questions :
		if tablename == 'memberbase' :
			### Birthyear works for rawmode = False, not for rawmode = True
			table['CODE1'] = table['CODE1'].map(str.strip)
	if 'Code2' in table:
		table['CODE2'] = table.pop('Code2')
		table['CODE2'] = table['CODE2'].astype('int').astype('U10')
		indexloc = table['CODE2'][table['CODE2'].duplicated()]
		if len(indexloc) > 0:
			logger.warn('dropping non-unique rows with CODE2 = %s from %s' % (indexloc.values,tablename))
			table.drop_duplicates(subset=['CODE2'],keep=False,inplace=True)
		table.set_index('CODE2', inplace=True)
	index = next(( col for col in table.columns if col.upper() == 'CODE2' ),None)
	#~ pdb.set_trace()
	for column in table.select_dtypes(include=[numpy.number]) :
		if table[column].max() <= 99 :
			try : 
				table[column] = table[column].astype('int').astype('str').astype('category')
			except ValueError, e :
				logger.warn("%s in %s could not be set as pandas dtype 'category' because: %s" % (column, filename, str(e)) )
			except TypeError, e:
				logger.warn("%s in %s could not be set as pandas dtype 'category' because: %s" % (column, filename, str(e)) )
		elif table[column].max < 32767 :
			try :
				table[column] = table[column].astype('short')
			except :
				logger.warn("%s in %s could not be set as pandas dtype 'short'" % (column, filename) )
	#~ table.convert_objects()
	#~ if tablename != 'memberbase' :
			#~ pdb.set_trace()
	
	for column in table.select_dtypes(include=['object','O'],exclude=['int64','category']) :
		## strip strings of millions of spaces
		
		table[column] = table[column].map(str.strip)
		try :
			table[column] = table[column].astype('int')
			if len(table[column].unique()) < 50 :
				try : 
					table[column] = table[column].astype('category')
				except :
					logger.info('could not cast %s as category data' % column)
		except ValueError:
			logger.info("%s cannot be set as int" % column)
				
	if tablename != 'memberbase': 

		if 'ID.format' in table :
			## merge after stripping spaces for easier matching
			table,failed = anonymize(table,tablename)
			
		## now remove nono-conforming column names, such as 'ID.format'
		cols = [col for col in table.columns if ( col.endswith('SUM') or ( not col.startswith('Q') and col.upper() != 'CODE2') )]
		table.drop(cols,inplace = True, axis=1 )
		if len(cols) > 0:
			logger.info	("Columns %s omited from %s because they contained internal or sensitive data" % (cols,filename))

	else :
		if index and (index != 'CODE2' ):
			table.rename(columns={index:'CODE2'},inplace=True)
        if 'CODE2' in table :
			table.set_index('CODE2',inplace=True)
	try: 
		schema = build_schema(table)
	except ValueError, e: 
		logger.error(str(e))


	return table,tablename,schema

def readbadfile(filename):
	with savReaderWriter.SavReaderNp(filename,recodeSysmisTo=99,rawMode=True,ioUtf8=False) as sNp :
		try :
			
			table = pd.DataFrame(sNp.to_structured_array())
		except ValueError, e :
			logger.error("line %s: %s %s" % (line_no(), filename, str(e)) )
			return
		except TypeError, e :
			logger.error("line %s: %s %s" % (line_no(), filename, str(e)) )
			return 
	return table	

cleanmeta = MetaData(bind=engine)
cleanmeta.reflect()
cleanmeta.drop_all()

meta = MetaData(bind=engine)

def makemetadata(schema,tablename):
	if schema :
		if tablename != 'memberbase' :
			table = Table(tablename, meta, 
				# these tables need CODE2 referencing memberbase.CODE2
				*[Column(column, schema[column]) if column != 'CODE2' else Column(column, schema[column], ForeignKey('memberbase.CODE2'), primary_key = True) for column in schema.keys()]
				)
		else :
			table = Table(tablename, meta, 
				*[Column(column, schema[column]) if column != 'CODE2' else Column(column, schema[column], primary_key = True) for column in schema.keys()]
				
				)
		return table
	else :
		return None

def readut8file(filename):
	#~ pdb.set_trace()
	#~ with savReaderWriter.SavReaderNp(filename,recodeSysmisTo=99,rawMode=True,ioUtf8=True) as sNp :
	with savReaderWriter.SavReaderNp(filename,recodeSysmisTo=99,ioUtf8=True) as sNp :
		try :
			table = pd.DataFrame(sNp.to_structured_array())
		except ValueError, e :
			logger.error("line %s: %s %s" % (line_no(), filename, str(e)) )
			return
		except TypeError, e :
			logger.error("line %s: %s %s" % (line_no(), filename, str(e)) )
			return
		except e:
			logger.error("line %s: %s %s" % (line_no(), filename, str(e)) )
			return
	return table
	
def write_to_db(table,filename):
		metatable = None
		logger.info("Attempting to process file %s" % filename)
		table,tablename,schema = build_table(filename,table)
		metatable = makemetadata(schema,tablename)
		if tablename != 'memberbase' :
			if health_vars:
				table_health = relevant_health(health_vars,tablename)
				table = extract_health(table_health,table)
		
		success = False
		
		if metatable != None :
			try: 
				metatable.drop(checkfirst=True)
			except exc.OperationalError, e:
				print str(e)
			except exc.IntegrityError, e:
				print str(e)
			try: 
				metatable.create()
			except exc.OperationalError, e:
				print str(e)
		else :
			logger.error("No MetaData for table %s" % tablename)
		metadata = None
		if schema :
			schema = schema
		else :
			schema = {'CODE2':types.String(10)}
		try:
			#~ pdb.set_trace()
			table.to_sql(tablename,engine,schema=metadata,if_exists='append',index=True)
			success = True
			print "\nfile %s saved to database\n" % tablename
		except ValueError, e:
			logger.error("ValueError: Line %s: %s for %s" % (line_no(), filename, str(e)) )
			print "%s was not saved to db: see error.log for details" % filename
		except pd.core.groupby.DataError, e:
			logger.error("DataError: Line %s: %s for %s" % (line_no(), filename, str(e)) )
			print "%s was not saved to db: see error.log for details" % filename
		except exc.IntegrityError, e:
			logger.error("Integrity Error: %s: %s " % (filename, str(e)[0:200]))
			print "%s failed\n\t ...attempting workaround; " % tablename
			print "\tlooking for bad blocks"
			num_rows = len(table)
			bar=progressbar.ProgressBar(redirect_stdout=True)
			#~ bar=progressbar.ProgressBar()
			#Iterate one row at a time
			rowssuccess = 0
			rowfailed = 0
			step = 50
			for i in bar(xrange(0,num_rows,step)):
				##TODO: this could probably be abstracted into a recursive function w/ step values as arg?
				try:
					# Try inserting blocks of rows
					table.iloc[i:i+step].to_sql(tablename,engine,schema=metadata,if_exists='append',index=True)
					rowssuccess += step
				except exc.IntegrityError, f:
					# look for bad row in failed block
					print "\t\tbad block found; locating bad line(s) %s - %s" % (i,i+step)
					bar.update(int(round(i/step)))
					for n in range(0,step) :
						try :
							table.iloc[i+n:i+n+1].to_sql(tablename,engine,schema=metadata,if_exists='append',index=True)
							rowssuccess += 1
						except exc.IntegrityError, e:
1							rowfailed +=1
							logger.error("CODE2 = %s failed in %s.  Occured > 1 in survey, or not in memberbase" % (int(e.params[0]),tablename))
							bar.update(int(round(i/step)))
							#Ignore duplicates
							pass
			if rowssuccess > 0 :
				print "%s: %s rows from %s saved to database" % (tablename, rowssuccess,tablename)
			if rowfailed > 0:
				print "\t%s row(s) failed" % rowfailed
				print "\tsee load-error.log for details of failed entries."
		except exc.SQLAlchemyError, e:
			logger.error("SQLAlchemy Error: %s: %s" % (filename, str(e)))
			print "%s was not saved to db: see load-error.log for details" % filename
		except exc.OperationalError, e:
			logger.error("Operational Error: Line %s: %s " % (line_no(), str(e)))
			print "%s was not saved to db: see load-error.log for details" % filename
		except AttributeError, e :
			logger.error("Attribute Error: Line %s: %s for %s" % (line_no(), filename, str(e)) )
			print "%s was not saved to db: see error.log for details" % filename

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
