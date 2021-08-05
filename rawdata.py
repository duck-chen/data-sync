import sys
import os
import re
import time,datetime
import uuid
from config import get_config
from mongo import Mongo
from mysql import MySql

class Rawdata(object):
	"""同步患者中的数据"""
	def __init__(self, config):
		self._config = get_config(config)
		self._mongo = Mongo(self._config).connect('iknow-mongo-yt')
		self._mysql = MySql(self._config).connect('isee-mysql')


	def insert_rawdata(self):
		select = {
				'name':1,
				'samplelist':1,
				'datapath':1,
		}

		page = 0
		persize = 1000
		while True:
			records = self._mongo['bclmon']['sequencing'].find({}, select).skip(page*persize).limit(persize)
			i = 0
			for record in records:
				i+=1
				s_uuid = ''.join(str(uuid.uuid4()).split('-'))     #生成随机数的uuid
				record['sn'] = s_uuid		                       #这里不能将值分配给函数，左边不能是函数		
				run_id = record.get('name')
				file_name = record.get('name')				
				list_sample = record.get('samplelist')
				
				if not list_sample:
					continue	
				for v in list_sample:
					patient_sn = v.get('sample')
					sample_sn = v.get('sample')
					file_size = v.get('size')
					file_type = v.get('datatype')
					path = record.get('datapath')
					if not path:					
						continue
					disk_storage = path[:8]
					relative_path = path[9:]

					data = {
						'sn':record['sn'],
						'run_id':run_id,
						'patient_sn':patient_sn,
						'sample_sn':sample_sn,
						'disk_storage':disk_storage,
						'relative_path':relative_path,
						'file_size':file_size,
						'file_name':file_name,
						'file_type':file_type
					}

					res = self._mysql.table('rawdata').where('sample_sn', sample_sn).select('id,sample_sn').first()
					if res:
						continue
					self._mysql.table('rawdata').insert(data)

			if i == 0:
				break

			print('update page '+str(page))
			page +=1

	print("insert rawdata finished")


if __name__ == '__main__':
	o = Rawdata(sys.argv[1])
	o.insert_rawdata()