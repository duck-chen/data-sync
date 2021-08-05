#!/usr/bin/python
#coding=utf-8
import pymysql
import sys
import time

class MySql:
	# 初始化变量
	

	def __init__(self, config):
		self._config = config
		self.db = None
		self.debug = False
		self.cursor = ''
		self.s_table = ''
		self.s_where = ''
		self.s_leftjoin = ''
		self.s_select = '*'
		self.s_order = ''
		self.s_group = ''
		self.s_whereRaw = ''
		self.data = []

	def connect(self, host_name):
		self._host = self._config[host_name]['host']
		self._user = self._config[host_name]['user']
		self._ps = self._config[host_name]['password']
		self._db_name = self._config[host_name]['db_name']
		self._port = self._config[host_name]['port']
		self._conn()
		return self


	def _conn(self):
		try:
			self.db = pymysql.connect(host = self._host, user = self._user, passwd = self._ps, db = self._db_name, port = self._port, charset = "utf8")
			return True
		except Exception as e:
			raise e
			self.log('e',"Mysql Error")
			return False
		

	def _reConn (self,num = 28800,stime = 3):
		_number = 0
		_status = True
		while _status and _number <= num:
			try:
				self.db.ping() #cping 校验连接是否异常
				_status = False
			except Exception as e:
				if self._conn()==True: #重新连接,成功退出
					_status = False
					break
				_number +=1
				time.sleep(stime)

			

	####### 过程函数  return self ###########################################################
	def table(self,table):
		self.s_table = table
		return self

	def leftjoin(self , table , key1 , eq , key2):
		self.s_leftjoin += " LEFT JOIN %s ON %s %s %s " %(table , key1 , eq , key2)
		return  self

	def where(self , key , eq , value = ''):
		if(value == ''):
			value = eq
			eq = '='
		self.s_where += ' AND ' if(self.s_where != '') else ''
		self.s_where += " %s %s " %(key , eq)
		self.s_where += ' %s'
		self.data.append(value);
		return  self

	def whereRaw(self , raw):
		self.s_where += ' AND ' if(self.s_where != '') else ''
		self.s_where += raw
		return  self

	def whereIn(self , key , data):
		s = []
		for d in data:
			s.append('%s')
			self.data.append(d);

		self.s_where += ' AND ' if(self.s_where != '') else ''
		self.s_where += " %s in (%s) " %(key, ','.join(s))
		return  self

	def select(self , s):
		self.s_select  = s;
		return self

	def groupBy(self , key):
		self.s_group += " GROUP BY %s " %(key)
		return self

	def whereNull(self, key):
		if self.s_where:
			self.s_where += " AND %s IS NULL " %(key)
		else:
			self.s_where = " %s IS NULL " %(key)
		return self

	def whereNotNull(self, key):
		if self.s_where:
			self.s_where += " AND %s IS NOT NULL " %(key)
		else:
			self.s_where = " %s IS NOT NULL " %(key)
		return self

	def orderBy(self, key ,order):
		self.s_order += " ORDER BY %s %s " %(key , order)
		return self

	####### 最终函数 #################################################################
	def getSql(self , number = 0):
		limit = ''
		if(number):
			limit = " LIMIT %d" %(number)
		where = '';
		if(self.s_where):
			where = " WHERE %s " %(self.s_where)
		sql = "SELECT %s FROM %s %s %s %s %s %s" %(self.s_select , self.s_table , self.s_leftjoin , where , self.s_order , self.s_group , limit )
		return sql;

	def first(self):
		sql = self.getSql(1)
		if self.debug:
			print(sql)
			print(self.data)
		self._reConn()
		self.cursor = self.db.cursor()
		self.cursor.execute(sql , self.data)
		self.db.commit()
		self.clear()
		self.clearData()
		line = self.cursor.fetchone()

		result = {}
		if (line == None):
			return result

		col = self.cursor.description
		k = 0
		for i in line:
			result[ col[k][0] ] = i
			k = k + 1
		return result


	def get(self , number = 0):
		if(number == 1):
			return self.first()
		sql = self.getSql()
		if self.debug:
			print(sql)
			print(self.data)
		self._reConn()
		self.cursor = self.db.cursor()
		self.cursor.execute(sql , self.data)
		self.db.commit()
		self.clear()
		self.clearData()
		line = self.cursor.fetchone()
		data = []
		if (not line):
			return data
		col = self.cursor.description
		while line:
			dis = {}
			k = 0
			for i in line:
				dis[ col[k][0] ] = i
				k = k + 1
			data.append(dis)
			line = self.cursor.fetchone()
		return data

	def query(self,sql):
		if self.debug:
			print (sql)
		flag = 0
		self._reConn()
		self.cursor = self.db.cursor()
		try:
			self.cursor.execute(sql)
			self.db.commit()
			flag = 1
		except Exception as e:
			raise e
			self.db.rollback()
			flag = 0
		return flag

	# 写入一条[]或者多条{}记录
	def insert(self,data):
		# 传进来的是一个数组 即多条记录
		if(type(data) == type([1,2])):
			key = [];
			key2 = [];
			value = []
			for d in data:
				temp_key = []
				temp = []
				temp_key2 = []
				for d2 in d:
					temp_key.append(d2)
					temp.append(d[d2])
					temp_key2.append('%s')

				value.append(temp)
				key = temp_key;
				key2 = temp_key2
			sql = "INSERT INTO %s(%s) VALUES (%s)" %(self.s_table , ','.join(key) , ','.join(key2));
			if self.debug:
				print (sql)
				print (value)
			flag = 0
			try:
				self._reConn()
				self.cursor = self.db.cursor()
				self.cursor.executemany(sql , value)
				self.db.commit()
				flag = 1
			except Exception as e:
				raise e
				self.db.rollback()
				flag = 0
			self.clear();
			self.clearData()
			return flag

		# 传进来的是一个字典 即一条记录
		if(type(data) == type({'a':1})):
			key1 = []
			key2 = []
			value = []
			for d in data:
				key2.append('%s')
				key1.append('`' + d  + '`')
				value.append(data[d])
			sql = "INSERT INTO %s(%s) VALUES (%s)" %(self.s_table , ','.join(key1) , ','.join(key2));
			if self.debug:
				print (sql)
				print (value)
			flag = 0
			try:
				self._reConn()
				self.cursor = self.db.cursor()
				self.cursor.execute(sql ,value)
				self.db.commit()
				flag = 1
			except Exception as e:
				raise e
				self.db.rollback()
				flag = 0
			self.clear();
			self.clearData()
			return flag
		self.log('w' , 'data format error')
		self.clear()
		self.clearData()
		return 0

	# 更新记录
	def update(self,data):
		setdata = [];
		for i in data:
			if type(data[i]) == type(1):
				setdata.append( "%s = %d "  %(i , data[i]) )
			else:
				setdata.append( "%s = '%s' "  %(i , data[i]) )

		where = '';
		if(self.s_where):
			where = " WHERE %s " %(self.s_where)
		sql = "UPDATE %s SET %s %s" %( self.s_table , ','.join(setdata) , where)

		if self.debug:
			print (sql)
		flag = 0
		try:
			self._reConn()
			self.cursor = self.db.cursor()
			self.cursor.execute(sql ,self.data)
			self.db.commit()
			flag = 1
		except Exception as e:
			raise e
			self.db.rollback()
			flag = 0
		self.clear();
		self.clearData()
		return flag

	def last_insert_id(self):
		sql = 'SELECT LAST_INSERT_ID() as id;'
		try:
			self._reConn()
			self.cursor = self.db.cursor()
			self.cursor.execute(sql ,self.data)
			self.db.commit()

		except Exception as e:
			raise e

		line = self.cursor.fetchone()
		return line[0]



	# 删除记录
	def delete(self):
		where = '';
		if(self.s_where):
			where = " WHERE %s " %(self.s_where)
		sql = "DELETE FROM %s %s" %(self.s_table , where)
		if self.debug:
			print (sql)
			print (self.data)
		flag = 0
		try:
			self._reConn()
			self.cursor = self.db.cursor()
			self.cursor.execute(sql , self.data)
			self.db.commit()
			flag = 1
		except Exception as e:
			raise e
			self.db.rollback()
			flag = 0

		self.clear();
		self.clearData()
		return flag

	# 计算查询结果的行数
	def count(self):
		if(self.s_where):
			where = " WHERE %s " %(self.s_where)
		sql = "SELECT count(*) as number FROM %s %s %s %s" %(self.s_table , self.s_leftjoin , where , self.s_group)
		if self.debug:
			print (sql)
			print (self.data)
		try:
			self._reConn()
			self.cursor = self.db.cursor()
			self.cursor.execute(sql , self.data)
			self.db.commit()
			line = self.cursor.fetchone()
			self.clear()
			self.clearData()
			return line[0]
		except Exception as e:
			raise e



	# 日志输出
	def log(self,type,str):
		T = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
		if(type == 'l'):
			print ('['+T+'][Info] '+str)
		if(type == 'w'):
			print ('['+T+'][Warning] '+str)
		if(type == 'e'):
			print ('['+T+'][Error] '+str)
			sys.exit(1)


	# 清除上一个查询中间存储
	def clear(self):
		self.s_table = ''
		self.s_where = ''
		self.s_leftjoin = ''
		self.s_select = '*'
		self.s_order = ''
		self.s_group = ''
		self.s_whereRaw = ''

	def clearData(self):
		self.data = []

	def close(self):
		self.db.close()

	# def __del__(self):
	# 	self.db.close()

