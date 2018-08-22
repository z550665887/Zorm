

class queue_set(object):
	def __init__(self, field_name, datas):
		self.info = []
		for data in datas:
			queue = Queue()
			for name, dat in zip(field_name, data):
				# print(name ,dat)
				setattr(queue, name.replace('`',''), dat)
			self.info.append(queue)

class Queue(object):
	"""docstring for Queue"""
	def __init__(self,):
		# for x in field_name:
			# setattr()
		pass