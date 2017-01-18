#!/usr/bin/python -w
from sqlalchemy import create_engine, inspect, exc, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from pandas import DataFrame
import logging
import ConfigParser
from Tkinter import *
import tkMessageBox
from tkFileDialog import askdirectory
from os import geteuid, path
import csv
import pdb 
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
dbuser = Config.get('SQL','user')
dbpasswd = Config.get('SQL','passwd')
dbhost = Config.get('SQL','host')
dbname = Config.get('SQL','db')

def SQL_connect() :

	engine = create_engine('mysql://{user}:{passwd}@{host}/{db}'.format(host = dbhost,
		user = dbuser,
		passwd = dbpasswd,
		db = dbname),
		#~ echo=True
		)
	return engine

try :
	engine = SQL_connect()
except :
	logging.error("failed to connect to MySQL database %s at %s") % (db,localhost)


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
tablenames.remove('memberbase')
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
		self.joined = IntVar()
		self.Header_text = Label(self,text="MCM Database Interface")
		self.QUIT = Button(self)
		self.QUIT["text"] = "QUIT"
		self.QUIT["fg"]   = "red"
		self.QUIT["command"] =  self.quit
		self.JOIN = Checkbutton(self,text="select only common respondents",variable=self.joined)
		self.EXPORT = Button(self)
		self.EXPORT["text"] = "export selection"
		self.EXPORT["command"] = self.export_selection
		self.options = Frame(self)
		self.optiontext = Label(self.options,text="Select tables from which to include data:")
		self.optionbox = Listbox(self.options,listvariable=tables, selectmode=MULTIPLE)
		self.SELECT = Button(self.options, text="Select", command=self.select_list)
		self.optionbox.pack()
		self.optiontext.pack()
		self.options.pack({"side":"right"})
		self.QUIT.pack({"side": "left"})
		self.SELECT.pack({"side": "left"})

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
		tables_set(q,selection_list)
		self.SELECT.pack_forget()
		self.optiontext.pack_forget()
		self.EXPORT.pack({"side":"left"})
		self.JOIN.pack()
		
	def file_save_as(self):
		f = askdirectory()
		if f is None: # askdirectory return `None` if dialog closed with "cancel".
			tkMessageBox.showinfo("Error","Must select a directory") 
			return
		else :
			return f
		
def tables_set(query,db_tables):
	global selected_tables

	selected_tables = [base.classes[i] for i in db_tables]
	for i in db_tables :
		query.add_entity(base.classes[i])
		
def find_joint_membership() :
	tablenames = [table.__table__.description for table in selected_tables]
	q = session.query()
	pdb.set_trace()
	return joint_members
	
	
def export(f):
	Join = App.joined.get()
	for table in selected_tables:
		filename = "%s/%s.csv" % (f, table.__table__.description)
		if Join: 
			CODE2s=session.query(tabledict['memberbase'].CODE2).join(*selected_tables)
			#~ member_set = find_joint_membership()
			other_tables = selected_tables[:] ## makin a copy because we're gonna modify this
			other_tables.remove(table)
			try:
				pdb.set_trace()
				records = session.query(table).join(tabledict['memberbase']).join(*other_tables).all()
				# try against CODE2s? one by one?
				
			except exc.SQLAlchemyError, e:
				tkMessageBox.showinfo("Error:", e) 
				return e
		else :
			records = session.query(table)
		with open(filename, 'wb') as tablefile : 
			out = csv.writer(tablefile)
			out.writerow([column.name for column in table.__mapper__.columns])
			out.writerows([[ getattr(curr, column.name) for column in table.__mapper__.columns ] for curr in records ])
	tkMessageBox.showinfo("Alert:","Selected data has been written to %s" % f) 
	pdb.set_trace()
	session.close()
	root.quit

			
def join(query,db_tables):
	db_tables.pop(0)
	for i in db_tables :
		query = query.join(base.classes[i])		
			
	




session = Session(engine)

## session.query(base.classes.memberbase).filter(base.classes.memberbase.BIRTHYEAR >= 1980).one()

q = session.query()
App = Application(master=root)
App.mainloop()

root.destroy()
