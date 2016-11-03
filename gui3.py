#!/usr/bin/python -w
from sqlalchemy import create_engine, inspect, exc 
from Tkinter import *
from os import geteuid
import pdb 
try:
	import pwd
except ImportError:
	import getpass
	pwd = None


def SQL_connect() :

	engine = create_engine('mysql://{user}:{passwd}@{host}/{db}'.format(host = 'localhost',
		user = 'python',
		passwd = 'mcmmpython',
		db = 'mcmdev'))
	return engine

engine = SQL_connect()

root = Tk()
frame = Frame(root)

# What tables (surveys) do we have in the db?
meta = inspect(engine)

tables = StringVar()

tables.set(' '.join(meta.get_table_names()))



#~ tablebox = Listbox(frame, listvariable=tables, selectmode=MULTIPLE)

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
		self.QUIT.pack({"side": "left"})
		self.options = Frame(self)
		self.optiontext = Label(self.options,text="Select tables from which to include data:")
		self.optionbox = Listbox(self.options,listvariable=tables, selectmode=MULTIPLE)
		self.optionbox.pack()
		self.optiontext.pack()
		self.options.pack()
		self.SELECT = Button(self.options, text="Select", command=self.select_list)
		self.SELECT.pack({"side": "left"})

	def update_widget(self, options, labeltext):
		ttk.Label(self.options, text=labeltext)
		self.optionbox = options
		self.optionbox.pack()

	def __init__(self, master=None):
		Frame.__init__(self, master)
		self.pack()
		self.createWidgets()

	def select_list(self):
		selection_list = list()
		selection = self.optionbox.curselection()
		for i in selection :
			entry = self.optionbox.get(i)
			selection_list.append(entry)
		pdb.set_trace()
		print selection_list
		return selection_list
		

App = Application(master=root)
App.mainloop()

root.destroy()
