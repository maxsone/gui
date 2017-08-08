#!/home/guest-user/.virtualenvs/dev/bin/python

from sqlalchemy import engine_from_config, exc, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql.expression import join as sqljoin 
from pandas import DataFrame, read_sql_query, compat, ExcelWriter, read_excel
from pandas import lib as pdlib 
import logging
import ConfigParser
from Tkinter import *
import tkMessageBox
from tkFileDialog import asksaveasfilename
from os import environ, path
import pdb 
import sys, glob, inspect
import regex as re

sys.defaultencoding = 'utf-8'

scriptdir = path.dirname(path.realpath(sys.argv[0]))
homedir = environ['HOME']
desktopdir=homedir + '/Desktop/'

Config = ConfigParser.ConfigParser()
Config.read(scriptdir + '/config.ini')

try:
	import pwd
except ImportError:
	import getpass
	pwd = None
#~ import unittest

####
# useful debug stuff

debug = Config.get('config','debug')

def line_no():
	"""Returns the current line number in our program."""
	return inspect.currentframe().f_back.f_lineno

if debug:
	def excepthook(etype, value, traceback):
		pdb.set_trace()
	import traceback
	import sys
	sys.excepthook = excepthook


if debug :
	logging.basicConfig(filename=desktopdir +'db-debug.log',filemode='w', level=logging.DEBUG)
else :
	logging.basicConfig(filename=desktopdir + 'db-error.log',filemode='w', level=logging.ERROR)

pdb.set_trace()

sqlsettings = dict(Config.items('sqlalchemy'))

def SQL_connect() :
	try:
		engine = engine_from_config( sqlsettings )
	except exc.SQLAlchemyError, e:
		print e
	return engine

try :
	engine = SQL_connect()
except :
	logging.error("failed to connect to MySQL database %s at %s" % (sqlsettings['sqlalchemy.dbname'],sqlsettings['sqlalchemy.url']))


root = Tk()
frame = Frame(root)

# What tables (surveys) do we have in the db?
metadata = MetaData()
metadata.reflect(bind=engine)
base = automap_base(metadata=metadata)

base.prepare(engine,reflect=True)

tabledict = {}

# setup relationships

tables = StringVar()
tablenames = []
tablenames = base.classes.keys()
tabledict = { x: base.classes[x] for x in tablenames }

#tablenames is used to populate menu, shouldn't have direct access
tablenames.remove('memberbase')

#we're going to want to be able to reference this.
MEMBERBASE = base.classes['memberbase'] 

#set this in the app, it wants a space seperated list because suddenly we're in bash?
tables.set(' '.join(sorted(tablenames)))

 # more easily refer to tables by name, mainly used in debug

selected_tables = []

def log():
	raise NotImplementedError

def current_user():
  if pwd:
    return pwd.getpwuid(geteuid()).pw_name
  else:
    return getpass.getuser()
	
def return_query():
	raise NotImplementedError

class Application(Frame):

	def createWidgets(self):
		self.Header_text = Label(self,text="MCM Database Interface")
		
		self.Left=Frame(self)
		self.Right=Frame(self)
		self.Left.grid(column=0,row=1)
		self.Right.grid(column=1,row=1)

		#inside the 'bottom' frame
		self.bottom=Frame(self,borderwidth=1)
		self.bottom.grid(column=0,row=50,columnspan=2)
		self.QUIT = Button(self.bottom)
		self.QUIT["text"] = "QUIT"
		self.QUIT["fg"]   = "red"
		self.QUIT["command"] =  self.quit
		self.QUIT.grid(row=0,column=1)
		self.EXPORT = Button(self.bottom) #button sends selection to Application.export_selection()
		self.EXPORT["text"] = "export selection"
		self.EXPORT["command"] = self.export_selection
		self.EXPORT.grid(column=0,row=0)


		# inside the 'tables' frame
		self.tables = Frame(self.Left,borderwidth=1)
		self.optiontext = Label(self.tables,text="Select tables from which to include data:")
		self.optionbox = Listbox(self.tables,listvariable=tables, selectmode=MULTIPLE)
		self.SELECT = Button(self.tables, text="Select", command=self.select_list)
		self.SELECT.grid(row=3,column=0)
		self.optionbox.grid(row=2, column=0)
		self.optiontext.grid(row=1, column=0)
		self.tables.grid(row=1, column=0, rowspan=len(tablenames)+2)
		
		# inside the 'constraints' frame
		self.constraints = Frame(self.Right,borderwidth=1)
		self.inner = IntVar()
		self.JOIN = Checkbutton(self.constraints,text="select only common respondents",variable=self.inner)
		self.JOIN.select()
		self.JOIN.grid(row=3,column=0)
		colcount = 0
		
		# inside the 'seperateFrame' frame (used to space radiobuttons)
		self.seperateFrame = Frame(self.constraints)
		joint = [('joint',1),('seperate',0)]
		self.joint = IntVar()
		self.joint.set('0')
		for text, val in joint:
			self.JOINT = Radiobutton(self.seperateFrame,text=text,value=val,variable=self.joint,command=self.resolveconflict) #check that both joint and combine are not checked
			self.JOINT.grid(row=0,column=colcount,sticky=W)
			colcount+=1
		self.seperateFrame.grid(row=5,sticky=W)


		# inside the 'members' frame (inside the 'constraints' frame)
		self.members = Frame(self.constraints,borderwidth=1)
		self.constrainmembers = IntVar() #constrain by characteristics of members?
		self.constrainmembersButton = Checkbutton(self.members,variable=self.constrainmembers,command=self.MEMBERBASE_selectors,text="Constrain selection by MEMBERBASE characteristics")
		self.constrainmembersButton.grid(column=0,sticky=W)
		self.members.grid(column=0,sticky=W)
		self.m_selectors = [] #which member char., which value?
		self.selectors_count = 0
		
		
		# inside the 'presets' frame'
		self.presets = Frame(self.Left,borderwidth=1)
		self.predefined=IntVar()
		
		# top selection menu
		self.PREDEF = Radiobutton(self,text='Use data from these tables',value=0,variable=self.predefined,command=self.showpredef)
		self.PREDEF.grid(column=0,row=0)
		self.PREDEF = Radiobutton(self,text='Use predefined topic sets',value=1,variable=self.predefined,command=self.showpredef)
		self.PREDEF.grid(column=1,row=0,columnspan=2,sticky=W)
				
	def selectPreDef(self,subject):
		tablesdict=self.matrix[subject]
		self.MEMBERBASE_selectors()
		self.EXPORT.grid(column=1,row=0)
		self.constraints.grid(column=1,row=1)
		
	def showpredef(self):
		if self.predefined.get():
			self.tables.grid_forget()
			self.matrix = matrix()
			self.subject = StringVar()
			predeflabel = Label(self.presets,text="select predefined column sets:")
			predeflabel.grid(row=0)
			self.TOPIC = OptionMenu(self.presets,self.subject,*self.matrix.keys(),command=self.selectPreDef)
			#~ self.TOPIC = OptionMenu(self.presets,self.subject,*self.matrix.keys())
			self.TOPIC.grid(row=1,column=0)
			self.JOIN.grid(column=1)
			self.presets.grid(row=1,column=0)
		else :
			self.presets.grid_forget()
			self.tables.grid(column=0,row=0)

	def add_mb_selector(self):
		self.selectors_count +=1
		self.m_s_button.grid_forget()
		self.MEMBERBASE_selectors()

	def MEMBERBASE_selectors(self) :
		cur_row = self.selectors_count + 4
		if self.constrainmembers.get() :
			varname = StringVar()
			self.membercols = Frame(self.members,borderwidth=1)
			self.m_s_var = OptionMenu(self.membercols,varname,*MEMBERBASE.__table__.columns.keys())

			#todo: assign *MEMBERBASE.__table__.columns.keys() to a var and remove one 
			#every time it's used, since we can probably only assign var once
			self.m_s_var.grid(row=cur_row,column=1)
			self.m_s_entry = Entry(self.membercols)
			##todo: should be a dropdown using MEMBERBASE.__table__.columns[key].type once key is selected
			self.m_s_entry.grid(row=cur_row,column=2)
			self.m_s_button = Button(self.membercols,text="add another",command=self.add_mb_selector)
			self.m_s_button.grid(row=cur_row,column=3)
			# add pairs of key/value to list, so we can find them pairwise later
			# for reasons best known to tkinter, you can't get() on an OptionMenu, but you can on a StringVar assigned to it
			self.m_selectors.append((varname,self.m_s_entry))
			self.membercols.grid(row=cur_row)
			self.members.grid(row=3)
			self.JOIN.grid(column=0)
		else :
			try :
				self.membercols.grid_forget()
			except (NameError, AttributeError):
				pass

	def add_mb_selector(self):
		self.selectors_count +=1
		self.MEMBERBASE_selectors()
		
	#~ def update_widget(self, options, labeltext):
		#~ ttk.Label(self.tables, text=labeltext)
		#~ self.optionbox = options
		#~ self.optionbox.grid(row=0,column=0)
		#~ return

	def __init__(self, master=None):
		Frame.__init__(self, master)
		self.grid(row=0,column=0)
		self.createWidgets()

	def export_selection(self):
		try :
			 export(self.outdir)
			 select_list(self)
		except :
			try: 
				self.outdir = self.file_save_as()
				export(self.outdir)
			except IOError as e: 
				tkMessageBox.showinfo("Error","something went wrong somewhere: %s" % e) 

	def select_list(self):
		self.constraints.grid(column=2)
		selection_list = list()
		self.selection = self.optionbox.curselection()
		for i in self.selection :
			entry = self.optionbox.get(i)
			selection_list.append(entry)
		tables_set(selection_list)
		if  self.JOIN.winfo_ismapped():
			pass
		else :
			pass
			
	def resolveconflict(self):

		if self.joint.get() == 1 and self.inner.get() == 0:
			self.inner.set(1)
			tkMessageBox.showinfo("Alert","It is very rarely desired to have merged datasets containg all data ('FULL OUTER JOIN').  If you are certain this is what you want, please contact the database adminstrator.") 
		
	def selected_options(self):
		pdb.set_trace()
		#~ self.SELECT.grid_forget()
		#~ self.optiontext.grid_forget()
		#~ self.EXPORT.grid(column=0)
		self.MEMBERBASE_selectors()
		
	def file_save_as(self):
		if debug == True:
			f = '/home/mcmadmin/development/output/out.xlsx'
			return f
		
		f = asksaveasfilename(defaultextension='.xlsx',initialdir=desktopdir)
		if not f: # askdirectory return `None` if dialog closed with "cancel".
			tkMessageBox.showinfo("Error","Must select a directory") 
			pass
		else :
			return f
		
def tables_set(db_tables):
	global selected_tables

	selected_tables = [base.classes[i] for i in db_tables]

		
#~ def find_joint_membership() :
	#~ tablenames = [table.__table__.description for table in selected_tables]
	#~ q = session.query()
	#~ return joint_members
 

def to_dict_dropna(data):
	return dict((k, v.dropna().tolist()) for k, v in compat.iteritems(data) if len(v.dropna().tolist()) > 0 )

def matrix():

	filepath = Config.get('config','data')
	try :
		matrixfile =  glob.glob(filepath + 'matrix*.xlsx')[-1]
	except :
		return False
	columns = Config.get('matrix','columns')
	sheetname= Config.get('matrix','sheetname')
	matrix = read_excel(matrixfile,sheetname=sheetname,parse_cols=columns)
	grouped = matrix.groupby('OVERALL THEME')
	matrixgroups = grouped.groups
	subjectgroup = dict()
	for group in matrixgroups.keys():
		subjectgroup[group] = to_dict_dropna(matrix.iloc[grouped.groups[group]])
		subjectgroup[group].pop('OVERALL THEME',None)
	return subjectgroup
	###  matrix.iloc[grouped.groups['???']] just rows in group
	###  to_dict_dropna(matrix.iloc[grouped.groups['???']])
	###  matrixdict = matrix.iloc[grouped.groups['???']].to_dict('list')
	###  matrix.iloc[grouped.groups['???']].dropna(axis=1,how='all') # remove empty columns

def export(f):
	global selected_tables
	presets = App.presets.winfo_ismapped()
	member_filter = App.constrainmembers.get() 
	Inner = App.inner.get()
	Joint = App.joint.get()
	filename = f
	if filename == None :
		pass
	# using xlsxwriter because it handles unicode better
	writer = ExcelWriter(filename,engine='xlsxwriter',options={'encoding':'unicode'})
	if presets :
		# probably we want individual sheets w/ records on them?
		preset_columns = qpreset()
		# table[0]._parententity.mapped_table.description
		for table, columns in  preset_columns.iteritems():
			tablequery = session.query().with_entities(*columns)
			df = read_sql_query(tablequery.statement,engine,index_col="CODE2")
			df.to_excel(writer,index='false',sheet_name=table)
	else :
		if len(selected_tables) > 0 :
			pass
		else :
			pdb.set_trace()
			#~ App.select_list()


		if not Joint: 
			filtered_members=session.query(MEMBERBASE.CODE2)
			if member_filter :
				filtered_members = qfilter()
			if Inner :
				filtered_members = filtered_members.join(*selected_tables)
			if presets :
				selected_tables = preset_columns
			for table in selected_tables:
				sheetname = "%s" % table.__table__.description

				if Inner or member_filter:
					#~ if debug:
						#~ try:
							#~ pdb.set_trace()
							#~ records = session.query(table).join(MEMBERBASE,MEMBERBASE.CODE2==table.CODE2).filter(MEMBERBASE.AGEGR == '6')
						#~ except Exception as err:
							#~ print traceback.format_exc()
							#~ pdb.set_trace()
							#~ print err.message
					try :
						records = session.query(table).filter(table.CODE2.in_(filtered_members))
					except Exception as err:
						pdb.set_trace()
						logging.error('%s: %s' % (line_no(), err.message))
					#~ records = session.query(table.__table__.alias()).join(filtered_members)
				else :
					records = session.query(table)
				#make this into a pandas dataframe, because of manageability of dfs, and pandas nice excel methods
				try :
					df = read_sql_query(records.statement,engine,index_col="CODE2")
					df.to_excel(writer,index='false',sheet_name=sheetname)
					if debug:
						#~ pdb.set_trace()
						logging.debug('using query: %s' % records.statement.compile(engine,compile_kwargs={"literal_binds": True}))
				except Exception as err:
					logging.error('%s: %s' % (line_no(), err.message))
				
		else :
			records = qjoin(selected_tables)

			df = read_sql_query(records.statement,engine,index_col="CODE2")
			df.to_excel(writer,index='false')

	writer.save()
	## todo: test for success
	tkMessageBox.showinfo("Alert:","Selected data has been written to %s" % f) 
	session.close()
	root.quit

def qpreset():
	subjects = App.matrix[App.subject.get()]

	#~ query = session.query()
	select_columns = dict()
	for tablename in subjects.keys():
		#trying to get matrix names to match on horrible db tablename names
		tablekey = str()
		if tablename in tabledict.keys():
			tablekey = tablename
		else :
			tablekey = re.sub(ur"\p{P}+", "", tablename)
			matches = [dbtablename for dbtablename in tabledict.keys() if tablekey in dbtablename]
			if len(matches) > 1:
				# if multiple matches, try for the one with the highest V value
				tablekey = sorted(matches)[len(matches)-1]				
			elif len(matches) < 1 :
				continue
			else :
				tablekey = matches[0]

		columns = subjects[tablename]
		columns = [ re.sub(ur"\*", "", col) for col in columns ]
		tableobj = tabledict[tablekey]
		try :
			column_objs = [tableobj.__dict__[col] for col in columns]
			column_objs = [tableobj.CODE2] + column_objs
			select_columns[tablekey]=column_objs
		except KeyError, e:
			logging.error("%s: %s" % (line_no(), str(e)))

	return select_columns
		#~ query = query.with_entities(*tablecolumns)
		

	return query

def qfilter():
	filter_dict = {}
	query = session.query(aliased(MEMBERBASE).CODE2)
	for i in App.m_selectors:
		try : 
			key = i[0].get()
			if key == '' :
				key = None
				continue
		except :
			continue
		try: 
			value = i[1].get()
		except:
			continue
		filter_dict[key] = value
	try :
		query = query.filter_by(**filter_dict)
	except:
		pdb.set_trace()
		# for like, >,< use  getattr(MEMBERBASE,key)
	return query
			
def qjoin(db_tables):
	tables = db_tables[:]
	firsttable = tables.pop(0)
	query = session.query(firsttable,*tables)

	for i in tables :
		try: 
			query = query.join(i, i.CODE2==firsttable.CODE2)
		except AttributeError, e:
			print str(e)
	
	return query

session = Session(engine)

## session.query(MEMBERBASE).filter(MEMBERBASE.BIRTHYEAR >= 1980).one()

q = session.query()
App = Application(master=root)
App.mainloop()


root.destroy()
