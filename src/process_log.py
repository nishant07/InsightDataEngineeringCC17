import re
from sys import argv
from heapq import *
from collections import Counter

def top_k_elements(element_dict, top_k=[], k=10):
	""" Maintains top k elements at any time for a dictionary to which
		the element belongs to.

		Args:
			element (dict): A dictionary with singl element to be 
							compared to the existing top_k elements
			top_k (heapq): Min heap containing top_k elements
			k (int): No of top elements need to be returned (default 10)

		Returns:
			list: Return a list with k elements

	"""
	#print element_dict
	element = element_dict.items() 
	element_key, element_value = element[0][0], element[0][1]
	
	if top_k:
		element_keys = map(lambda k: k[1], top_k)
		#print element_keys

	if top_k and element_key in element_keys:
		del top_k[element_keys.index(element_key)]
		heappush(top_k, (element_value, element_key))
	else:		
		if not top_k or len(top_k) < k:
			heappush(top_k, (element_value, element_key))
		else:
			if top_k[0][0] == element_value:
				if top_k[0][1] > element_key:
					heapreplace(top_k, (element_value, element_key))
			elif top_k[0] < (element_value, element_key):
				heapreplace(top_k, (element_value, element_key))
			else:
				pass
	return top_k

def decompose_server_log(server_log):
	""" Decomposes each log into host, timestamp, resource, 
		HTTP reply code, bytes.
	
	Args:
		server_log (str): A server log
	Returns:
		tuple: A tuple with decomposed values mentioned in the definition
	"""
	raw_log = server_log.split()
	try:
		host = raw_log[0]
		timestamp = re.search(r"\[(.*)\]",server_log).group()
		request = re.search(r"\"(.*)\"", server_log).group()
		resource = request.split()[1]
		response_code = raw_log[-2]
		if raw_log[-1] == '-':
			bytes = 0
		else:
			bytes = int(raw_log[-1])
	except IndexError:
		print raw_log
		return ('dummy_host','dummy_ts','SOS',-1)
	return (host, timestamp, resource, response_code, bytes)

def analyze_server_logs():
	""" Main function from which the execution begins for
		the implementation of all the features.
	"""
	host_list = Counter()
	timestamps = list()
	resources_list = dict()
	top_k_host = []
	top_k_resources = []
	with open(argv[1],'r') as input_file:		
		for server_log in input_file:
			decomposed_log = decompose_server_log(server_log)
			#print decomposed_log
			try:
				host = decomposed_log[0]
				resource = decomposed_log[2]
				bytes = decomposed_log[4]
			except IndexError:
				print "Host,resource,bytes", decomposed_log
				continue

			host_list[host] += 1
			resources_list[resource] = resources_list.get(resource,0) + bytes

			top_k_host = top_k_elements({host: host_list[host]}, top_k_host)
			top_k_resources = top_k_elements({resource: resources_list[resource]}, top_k_resources)

	print top_k_host
	print top_k_resources

			
			
if __name__ == "__main__":
	analyze_server_logs()
