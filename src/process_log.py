import re
import heapq
from sys import argv
#from heapq import *
from collections import Counter, deque
from datetime import datetime, timedelta

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
		if element_value > top_k[element_keys.index(element_key)][0]:
			del top_k[element_keys.index(element_key)]
			heapq.heappush(top_k, (element_value, element_key))
	else:		
		if not top_k or len(top_k) < k:
			heapq.heappush(top_k, (element_value, element_key))
		else:
			if top_k[0][0] == element_value:
				if top_k[0][1] > element_key:
					heapq.heapreplace(top_k, (element_value, element_key))
			elif top_k[0] < (element_value, element_key):
				heapq.heapreplace(top_k, (element_value, element_key))
			else:
				pass
	return top_k

def str_to_ts(str_ts):
	""" Transforms timestamp of format DD/MON/YYYY:HH:MM:SS -0400 
		string to datetime object of Python
	
	Args:
		str_ts (str): Timestamp in raw string format
	Returns:
		datetime : Timestamp in form of Python datetime object
	"""
	format = "%d/%b/%Y:%H:%M:%S"
	str_ts = str_ts.split()[0]
	ts = datetime.strptime(str_ts, format)
	return ts

def ts_to_str(ts):
	""" Transforms datetime-timestamp to DD/MON/YYYY:HH:MM:SS -0400 
		string format
	
	Args:
		ts (datetime): Timestamp in form of Python datetime object
	Returns:
		str : Timestamp in raw string format
	"""
	format = "%d/%b/%Y:%H:%M:%S"
	str_ts = ts.strftime(format)
	return ''.join([str_ts," -0400"])

def find_busiest_windows(timestamp, timestamps, 
		top_k_busiest_windows, curr_win_length, k=10):
	""" Finds busiest k 60 minute window.
	Args:
		timestamp (datetime): Next timestamp in the log
		timestamps (deque): Current 60 minute window
		top_k_busiest_windows (list): Current top k busiest windows
		k (int): Top K value (default 10)
	Returns:
		tuple: (Current 60 min window, current top k businest windows)
	"""
	WINDOWS_SIZE = 60 #in minutes
	delta = timedelta(minutes=WINDOWS_SIZE)
	timestamps.append(timestamp)

	if (timestamp - timestamps[0]) >= delta:
		str_ts = ts_to_str(timestamps.popleft())
		top_k_busiest_windows = top_k_elements({str_ts: curr_win_length},
									top_k_busiest_windows) 
		while (timestamp - timestamps[0]) >= delta:
			str_ts = ts_to_str(timestamps.popleft())
			curr_win_length -= 1
			top_k_busiest_windows = top_k_elements({str_ts: curr_win_length},
										top_k_busiest_windows) 
	else:
		curr_win_length += 1
	return (timestamps, top_k_busiest_windows, curr_win_length)



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
		str_timestamp = re.search(r"\[(.*)\]",server_log).group()[1:-1]
		timestamp = str_to_ts(str_timestamp)
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
	#print timestamp
	return (host, str_timestamp, timestamp, resource, response_code, bytes)

def analyze_server_logs():
	""" Main function from which the execution begins for
		the implementation of all the features.
	"""
	host_list = Counter()
	timestamps = deque([datetime(1900, 01, 01, 00, 00, 00)])
	curr_win_length = len(timestamps)
	resources_list = Counter()
	top_k_host = []
	top_k_resources = []
	top_k_busiest_windows = []

	with open(argv[1],'r') as input_file:		
		for server_log in iter(input_file.readline,''):
			decomposed_log = decompose_server_log(server_log)
			#print decomposed_log
			try:
				host = decomposed_log[0]
				str_timestamp = decomposed_log[1]
				timestamp = decomposed_log[2]
				resource = decomposed_log[3]
				response_code = decomposed_log[4]
				bytes = decomposed_log[5]
			except IndexError:
				print "Host,resource,bytes", server_log
				continue
			#break
			host_list[host] += 1
			resources_list[resource] = resources_list.get(resource,0) + bytes
			timestamps, top_k_busiest_windows, \
			curr_win_length = find_busiest_windows( #str_timestamp, 
													timestamp, timestamps,
													top_k_busiest_windows,
													curr_win_length)

			top_k_host = top_k_elements({host: host_list[host]}, top_k_host)
			top_k_resources = top_k_elements({resource: resources_list[resource]}, 
												top_k_resources)

	print top_k_host
	print top_k_resources
	print top_k_busiest_windows

			
			
if __name__ == "__main__":
	analyze_server_logs()
