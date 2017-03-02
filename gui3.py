#!/usr/bin/python -w
from sqlalchemy import engine_from_config, inspect, exc, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import join as sqljoin 
from pandas import DataFrame, read_sql_query, compat, ExcelWriter, read_excel
from pandas import lib as pdlib 
import logging
import ConfigParser
from Tkinter import *
import tkMessageBox
from tkFileDialog import asksaveasfilename
from os import geteuid, path
import pdb 
import sys, glob
import regex as re

sys.defaultencoding = 'utf-8'

try:
	import pwd
except ImportError:
	import getpass
	pwd = None
import unittest

debug = True
debugdir = path.realpath(__file__)


if debug :
	logging.basicConfig(filename='debug.log',filemode='w', level=logging.DEBUG)
else :
	logging.basicConfig(filename='gui-error.log',filemode='w', level=logging.ERROR)

Config = ConfigParser.ConfigParser()
Config.read('./config.ini')



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
memberbase = base.classes['memberbase'] 

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

		#inside the 'admin' frame
		self.admin=Frame(self,borderwidth=1)
		self.admin.grid(column=0,row=50)
		self.QUIT = Button(self.admin)
		self.QUIT["text"] = "QUIT"
		self.QUIT["fg"]   = "red"
		self.QUIT["command"] =  self.quit
		self.QUIT.grid(column=20,sticky=SE)
		self.EXPORT = Button(self.admin) #button sends selection to Application.export_selection()
		self.EXPORT["text"] = "export selection"
		self.EXPORT["command"] = self.export_selection

		
		# inside the 'constraints' frame
		self.constraints = Frame(self,borderwidth=1)
		self.inner = IntVar()
		self.JOIN = Checkbutton(self.constraints,text="select only common respondents",variable=self.inner)
		self.JOIN.select()
		joint = [('joint',1),('seperate',0)]
		self.joint = IntVar()
		self.joint.set('0')
		colcount = 0
		
		# inside the 'seperateFrame' frame (used to space radiobuttons)
		self.seperateFrame = Frame(self.constraints)
		for text, val in joint:
			self.JOINT = Radiobutton(self.seperateFrame,text=text,value=val,variable=self.joint,command=self.resolveconflict) #check that both joint and combine are not checked
			self.JOINT.grid(row=0,column=colcount,sticky=W)
			colcount+=1
		self.seperateFrame.grid(row=5,sticky=W)
		self.JOIN.grid(row=7,column=0,sticky=W)

		# inside the 'members' frame (inside the 'constraints' frame)
		self.members = Frame(self.constraints,borderwidth=1)
		self.constrainmembers = IntVar()
		self.constrainmembersButton = Checkbutton(self.members,variable=self.constrainmembers,command=self.memberbase_selectors,text="Constrain selection by memberbase characteristics")
		self.constrainmembersButton.grid(column=0,sticky=W)
		self.members.grid(column=0,sticky=W)

		# inside the 'tables' frame
		self.tables = Frame(self,borderwidth=1)
		self.optiontext = Label(self.tables,text="Select tables from which to include data:")
		self.optionbox = Listbox(self.tables,listvariable=tables, selectmode=MULTIPLE)
		self.SELECT = Button(self.tables, text="Select", command=self.select_list)
		self.SELECT.grid(row=3,column=0)
		self.optionbox.grid(row=2, column=0)
		self.optiontext.grid(row=1, column=0)
		self.tables.grid(row=1, column=0, rowspan=len(tablenames)+2)		
		# inside the 'presets' frame'
		self.presets = Frame(self,borderwidth=1)
		self.predefined=IntVar()
		#~ Label(self.members,)
		self.PREDEF = Radiobutton(self,text='Use data from these tables',value=0,variable=self.predefined,command=self.showpredef)
		self.PREDEF.grid(column=0,row=0)
		self.PREDEF = Radiobutton(self,text='Use predefined topic sets',value=1,variable=self.predefined,command=self.showpredef)
		self.PREDEF.grid(column=1,row=0,sticky=W)
				
	def selectPreDef(self,subject):
		tablesdict=self.matrix[subject]
		self.memberbase_selectors()
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

		
	def memberbase_selectors(self) :
		if self.constrainmembers.get() :
			memberbase_column = StringVar()
			self.membercols = Frame(self.members,borderwidth=1)
			self.member_args = {}
			self.member_select = OptionMenu(self.membercols,memberbase_column,*memberbase.__table__.columns.keys())
			self.member_select.grid(row=4,column=1)
			entry = Entry(self.membercols).grid(row=4,column=2)
			if memberbase_column.get() :
				if self.Entry.get() :
					member_args[memberbase_column.get()] = self.Entry.get()
					self.member_select.grid(row=2,column=1)	
			self.membercols.grid(row=2)
			self.members.grid(row=3)
			self.JOIN.grid(column=1)
		else :
			try :
				self.membercols.grid_forget()
			except (NameError, AttributeError):
				pass

	
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
		selection_list = list()
		self.selection = self.optionbox.curselection()
		for i in self.selection :
			entry = self.optionbox.get(i)
			selection_list.append(entry)
		tables_set(selection_list)
		if  self.JOIN.winfo_ismapped():
			pass
		else :
			self.selected_options()
			
	def resolveconflict(self):

		if self.joint.get() == 1 and self.inner.get() == 0:
			self.inner.set(1)
			tkMessageBox.showinfo("Alert","It is very rarely desired to have merged datasets containg all data ('FULL OUTER JOIN').  If you are certain this is what you want, please contact the database adminstrator.") 
		
	def selected_options(self):

		
		self.SELECT.grid_forget()
		self.optiontext.grid_forget()
		self.EXPORT.grid(column=0)
		self.memberbase_selectors()
		
	def file_save_as(self):
		f = asksaveasfilename(defaultextension='.xlsx')
		if not f: # askdirectory return `None` if dialog closed with "cancel".
			tkMessageBox.showinfo("Error","Must select a directory") 
			pass
		else :
			return f
		
def tables_set(db_tables):
	global selected_tables

	selected_tables = [base.classes[i] for i in db_tables]

		
def find_joint_membership() :
	tablenames = [table.__table__.description for table in selected_tables]
	q = session.query()
	return joint_members
 

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
	presets = App.presets.winfo_ismapped()
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
		if len(selected_tables) == 0 :
			App.select_list()

		if not Joint: 
			if Inner :
				CODE2s = session.query(memberbase.CODE2).join(*selected_tables)
			if presets :
				selected_tables = preset_columns
			for table in selected_tables:
				sheetname = "%s" % table.__table__.description
				other_tables = selected_tables[:] ## makin a copy because we're gonna modify this
				other_tables.remove(table)
				if Inner :
					records = session.query(table).filter(table.CODE2.in_(CODE2s))
				else :
					records = session.query(table)
				#make this into a pandas dataframe, because of manageability of dfs, and pandas nice excel methods
				df = read_sql_query(records.statement,engine,index_col="CODE2")
				df.to_excel(writer,index='false',sheet_name=sheetname)
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
				pdb.set_trace()
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
			logging.error("%s" % str(e))
			pdb.set_trace()
	pdb.set_trace()
	return select_columns
		#~ query = query.with_entities(*tablecolumns)
		

	return query

def qfilter(filter_expressions):
	query = session.query(memberbase.CODE2)
	for expression in filter_expressions :
		query = query.filter(expression)
	return filterquery
			
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

## session.query(memberbase).filter(memberbase.BIRTHYEAR >= 1980).one()

q = session.query()
App = Application(master=root)
App.mainloop()


root.destroy()
