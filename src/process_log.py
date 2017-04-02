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
	element = element_dict.items() 
	element_key, element_value = element[0][0], element[0][1]
	
	if top_k or len(top_k) < k:
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

def decompose_server_log(sever_log):
	""" Decomposes each log into host, timestamp, resource, 
		HTTP reply code, bytes.
	Args:
		server_log (str): A server log
	Returns:
		tuple: A tuple with decomposed values mentioned in the definition
	"""
	pass

def analyze_server_logs():
	""" Main function from which the execution begins for
		the implementation of all the features.
	"""
	host_list = Counter()
	timestamps = list()
	resources_list = Counter()
	with open(argv[1],'r') as input_file:		
		for server_log in input_file:
			pass
			
if __name__ == "__main__":
	analyze_server_logs()
