# 
from argparse import ArgumentParser
from pprint import pprint
from sys import argv
import MySQLdb
import shutil 
import time 
import os 

def my_arg_parse():
	description = "this is my app description"
	parser = ArgumentParser(description=description)
	parser.add_argument("-conf", dest="configdir", 
						 	  metavar="config_dir", 
							  default="./config_dir",
							   	 help="config dir abs path")
	opts = parser.parse_args()
	return opts

class FilesHandler(object):
	def __init__(self, configdir):
		print "in FilesHandler __init__()"
		self._config_dir  = os.path.abspath(configdir)
		self._config_file = None
		self._config_data = None

	def _get_config_file(self):
		print "in _get_config_file()"
		if not self._config_file:
			for file in os.listdir(self._config_dir):
				if file == "config_file.conf":	
					self._config_file = os.path.join(self._config_dir,file )
		return self._config_file

	def _set_config_data(self):
		print "_set_config_data()"
		if not self._config_data:
			config_file = self._get_config_file()	
			line_list = []
			config_data_dict = {}
			for i, line in enumerate(file(config_file)):
				if " = " in line: 
					if line.startswith("#"):
						continue
					for i in line.strip().split("="):
						line_list.append(i.strip())
			keys_list = line_list[::2]
			vals_list = line_list[1::2]
			self._config_data = dict(zip(keys_list, vals_list))

	def _get_config_data(self):
		print "_get_config_data()"
		if not self._config_data:
			self._set_config_data()
		return self._config_data

	def _get_src_dir(self):
		print "_get_src_dir()"
		if not self._config_data:
			self._set_config_data()	
		src_dirs = []
		for d in self._config_data.get("srcdir", None).split("'"):
			if "/" in d:
				src_dirs.append(d)
		return src_dirs

	def _get_data_files(self):
		print "_get_data_files()"
		src_dirs = self._get_src_dir()
		file_list = []
		for d in src_dirs:
			for f in os.listdir(d):
				print "currenting file is %s" % f
				if f.endswith(".tmp"):
					continue
				if f.startswith("."):
					continue
				if os.path.isfile(os.path.join(d,f)):
					file_list.append(os.path.join(d,f))
		return file_list
		
	
	def _get_data(self, data_file):
		print "in _get_data()"
		data_file = file(data_file)
		data_lines = []
		for line in data_file:
			if line.strip():
				data_lines.append(line.strip().split())
		data_file.close()
		return data_lines
	
	def handle_data_file(self):
		print "in handle_data_file()"
		files = self._get_data_files()
		if not files:
			print "there is no file need to handle"
			return []
		datas = []
		for f in files:
			for data in self._get_data(f):
				datas.append(data)
			backup_dir = os.path.dirname(f)+ "/backup"
			if not os.path.exists(backup_dir):
				print "create %s" % backup_dir
				os.mkdir(backup_dir)
			temp_file = os.path.join(backup_dir,os.path.basename(f))
			if os.path.exists(temp_file):
				os.remove(temp_file)
			shutil.move(f,backup_dir)
		return datas

class DBhandler(FilesHandler):
	def __init__(self, *args, **kwargs):	
		print "in DBhandler __init__()"
		super(DBhandler, self).__init__(*args, **kwargs)
		self.datas 	  = self.handle_data_file()
		config_data   = self._get_config_data()
		self.srcdir   = config_data.get("srcdir"  , None)
		self.dstdir   = config_data.get("dstdir"  , None)
		self.hostip   = config_data.get("hostip"  , None)
		self.user     = config_data.get("user"    , None)
		self.passwd   = config_data.get("passwd"  , None)
		self.timeset  = config_data.get("timeset" , None)
		self.database = config_data.get("database", None)
		self.insert_cmd_template = config_data.get("insert_cmd_template", None)
	
	def insert_data_to_db(self):
		print "in insert_data_to_db()"
		if not self.datas:
			print "there's no data need to be handled"
		conn = MySQLdb.connect(host=self.hostip, 
							   user=self.user, 
							   passwd=self.passwd)
		cur = conn.cursor()
		conn.select_db('%s' % self.database)
		for data in self.datas:
			insert_cmd = self.insert_cmd_template %  tuple(data)
			print insert_cmd
			cur.execute(insert_cmd)	
			time.sleep(1)
		conn.commit()
		cur.close()
		conn.close()

def main():
	print "main startting\n"
	while True:
		myc = DBhandler(opts.configdir)
		myc.insert_data_to_db()
		time.sleep(int(myc.timeset))

if __name__ == "__main__":
	opts = my_arg_parse()
	main()



