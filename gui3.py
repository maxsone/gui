#!/usr/bin/python -w
from sqlalchemy import engine_from_config, inspect, exc, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import join as sqljoin 
from pandas import DataFrame, read_sql_query, ExcelWriter
from pandas import lib as pdlib
import logging
import ConfigParser
from Tkinter import *
import tkMessageBox
from tkFileDialog import asksaveasfilename
from os import geteuid, path
import pdb 
import sys

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
		self.QUIT = Button(self)
		self.QUIT["text"] = "QUIT"
		self.QUIT["fg"]   = "red"
		self.QUIT["command"] =  self.quit

		self.inner = IntVar()
		self.JOIN = Checkbutton(self,text="select only common respondents",variable=self.inner)
		self.JOIN.select()
		
		self.EXPORT = Button(self) #button sends selection to Application.export_selection()
		self.EXPORT["text"] = "export selection"
		self.EXPORT["command"] = self.export_selection
		
		self.options = Frame(self)
		self.optiontext = Label(self.options,text="Select tables from which to include data:")
		self.optionbox = Listbox(self.options,listvariable=tables, selectmode=MULTIPLE)
		self.SELECT = Button(self.options, text="Select", command=self.select_list)
		self.optionbox.pack()
		self.optiontext.pack()
		self.options.pack({"side":"left"})
		
		self.QUIT.pack({"side": "right"})
		
		self.SELECT.pack({"side": "right"})
		
	def memberbase_selectors(self) :
		memberbase_column = StringVar()
		self.member_args = {}
		self.member_select = OptionMenu(self,memberbase_column,*memberbase.__table__.columns.keys())
		self.member_select.pack()
		entry = Entry(self).pack({"side":"left"})
		if memberbase_column.get() :
			if self.Entry.get() :
				member_args[memberbase_column.get()] = self.Entry.get()
				self.member_select.pack()		
	
	def update_widget(self, options, labeltext):
		ttk.Label(self.options, text=labeltext)
		self.optionbox = options
		self.optionbox.pack()

	def __init__(self, master=None):
		Frame.__init__(self, master)
		self.pack()
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
		pdb.set_trace()
		if self.joint.get() == 1 and self.inner.get() == 0:
			self.inner.set(1)
			tkMessageBox.showinfo("Alert","It is very rarely desired to have merged datasets containg all data ('FULL OUTER JOIN').  If you are certain this is what you want, please contact the database adminstrator.") 
		
	def selected_options(self):

		joint = [('joint',1),('seperate',0)]
		self.joint = IntVar()
		self.joint.set('0')
		for text, val in joint:
			self.JOINT = Radiobutton(self,text=text,value=val,variable=self.joint,command=self.resolveconflict)
			self.JOINT.pack({'side':'right'})	
		
		self.SELECT.pack_forget()
		self.optiontext.pack_forget()
		self.EXPORT.pack({"side":"right"})
		self.JOIN.pack()
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
	
	
def export(f):

	Inner = App.inner.get()
	Joint = App.joint.get()
	filename = f
	if filename == None :
		pass
	# using xlsxwriter because it handles unicode better
	writer = ExcelWriter(filename,engine='xlsxwriter',options={'encoding':'unicode'})
	if len(selected_tables) == 0 :
		App.select_list()

	if not Joint: 
		if Inner :
			pdb.set_trace()
			CODE2s = session.query(memberbase.CODE2).join(*selected_tables)
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
		#~ if filter :

		df = read_sql_query(records.statement,engine,index_col="CODE2")
		df.to_excel(writer,index='false')


	writer.save()
	tkMessageBox.showinfo("Alert:","Selected data has been written to %s" % f) 
	session.close()
	root.quit

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
