import re
import heapq
from sys import argv
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
	element = element_dict.items() 
	element_key, element_value = element[0][0], element[0][1]
	
	if top_k:
		element_keys = map(lambda k: k[1], top_k)

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

def sort_heap(top_k_heap, order = "desc"):
	""" Sorts the heap in given order
	Args:
		top_k_heap (heapq): Heap to be sorted
		order (str): Sorting order (default "desc")
	Returns:
		list 
	"""
	sorted_heap = [heapq.heappop(top_k_heap) for i in xrange(len(top_k_heap))]
	if order == "desc":
		return sorted_heap[::-1]
	else:
		return sorted_heap

def find_busiest_windows(timestamp, timestamps,
						top_k_busiest_windows, curr_win_length, k=10):
	""" Finds busiest k 60 minute window.
	Args:
		timestamp (datetime): Next timestamp in the log
		timestamps (deque): Current 60 minute window
		top_k_busiest_windows (list): Current top k busiest windows
		k (int): Top K value (default 10)
	Returns:
		tuple: (deque, heapq, int)
	"""
	WINDOW_SIZE = 60 #in minutes
	delta = timedelta(minutes=WINDOW_SIZE)
	timestamps.append(timestamp)
	curr_win_length += 1

	if (timestamp - timestamps[0]) >= delta:
		str_ts = ts_to_str(timestamps.popleft())
		curr_win_length -= 1
		top_k_busiest_windows = top_k_elements({str_ts: curr_win_length},
									top_k_busiest_windows) 
		while (timestamp - timestamps[0]) >= delta:
			str_ts = ts_to_str(timestamps.popleft())
			curr_win_length -= 1
			top_k_busiest_windows = top_k_elements({str_ts: curr_win_length},
										top_k_busiest_windows) 

	return (timestamps, top_k_busiest_windows, curr_win_length)

def blocked(host, timestamp, resource, response_code, 
				flagged_hosts_list, blocked_hosts_list):
	""" Checks if the entry should be blocked and returns 
		blocked hosts list

	Args:
		host (str): host/IP address
		timestamp (datetime): Timestamp of the log entry
		resource (str): Requested resource
		response_code (str): HTTP response code 
		flagged_hosts_list (dict): Hosts with failed attempt in 
									20 second window
		blocked_hosts_list (dict): Hosts with 5 minutes waiting period				
	Returns:
		tuple: (Boolean, Dictionary, Dictionary)
	"""
	blocked_status = False

	if host in blocked_hosts_list:
		if timedelta(minutes=5) >= timestamp - flagged_hosts_list[host][-1]:
			blocked_status = True
		else:
			del flagged_hosts_list[host]
			del blocked_hosts_list[host]
	if host not in flagged_hosts_list:
		if resource == "/login" and response_code == "401":
			flagged_hosts_list[host] = [timestamp]
	else:
		if timedelta(seconds=20) < timestamp - flagged_hosts_list[host][-1]:
			if resource == "/login" and response_code == "401":
				flagged_hosts_list[host] = [timestamp]
		else:
			if len(flagged_hosts_list[host]) == 1:
				if resource == "/login":
					if response_code == "401":
						flagged_hosts_list[host].append(timestamp)
					elif response_code == "200":
						del flagged_hosts_list[host]
					else:
						pass
			elif len(flagged_hosts_list[host]) == 2:
				if resource == "/login":
					if response_code == "401":
						flagged_hosts_list[host].append(timestamp)
						blocked_hosts_list[host] = blocked_hosts_list.get(host, 0) + 1
					elif response_code == "200":
						del flagged_hosts_list[host]
					else:
						pass
			else:
				pass
	return (blocked_status, flagged_hosts_list, blocked_hosts_list)

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
		request_type = request.split()[0]
		resource = request.split()[1]
		response_code = raw_log[-2]
		if raw_log[-1] == '-':
			bytes = 0
		else:
			bytes = int(raw_log[-1])
	except IndexError:
		return None
	return (host, str_timestamp, timestamp, request_type, resource, 
			response_code, bytes)

def analyze_server_logs():
	""" Main function from which the execution begins for
		the implementation of all the features.
	"""
	feature1_output = argv[1] # "../log_output/hosts.txt"
	feature2_output = argv[2] # "../log_output/resources.txt"
	feature3_output = argv[3] # "../log_output/hours.txt"
	feature4_output = argv[4] #"../log_output/blocked.txt"		

	host_list = Counter()
	top_k_host = []

	resources_list = Counter()
	top_k_resources = []

	timestamps = deque([datetime(1900, 01, 01, 00, 00, 00)])
	curr_win_length = len(timestamps)
	top_k_busiest_windows = []

	flagged_hosts_list = {}
	blocked_hosts_list = {}
	blocked_attempts = []
	no_of_logs = 0
	blocked_log = 0

	with open(argv[1],"r") as input_file:		
		for server_log in iter(input_file.readline,''):
			no_of_logs += 1
			decomposed_log = decompose_server_log(server_log)
			if not decomposed_log:
				print server_log
				print "Unable to parse above log, Error in format"
				continue
			else:
				host = decomposed_log[0]
				str_timestamp = decomposed_log[1]
				timestamp = decomposed_log[2]
				request_type = decomposed_log[3]
				resource = decomposed_log[4]
				response_code = decomposed_log[5]
				bytes = decomposed_log[6]

			host_list[host] += 1
			top_k_host = top_k_elements({host: host_list[host]}, top_k_host)

			resources_list[resource] = resources_list.get(resource,0) + bytes			
			top_k_resources = top_k_elements({resource: resources_list[resource]}, 
												top_k_resources)
			
			timestamps, top_k_busiest_windows, \
			 curr_win_length = find_busiest_windows(timestamp, timestamps,
													top_k_busiest_windows,
													curr_win_length)	

			blocked_status, flagged_hosts_list, \
			 blocked_hosts_list = blocked(host, timestamp, 
			 								resource, response_code, 
			 								flagged_hosts_list, 
											blocked_hosts_list)

			if blocked_status:
				blocked_log += 1
				blocked_attempts.append(server_log)
			if no_of_logs % 100000 == 0:
				print no_of_logs," logs processed"
	
	if curr_win_length >= top_k_busiest_windows[0][0]:
		k = 1
		for i in xrange(curr_win_length-1):
			c = k - i
			if timestamps[i] != timestamps[i-1]:
				for j in xrange(k,curr_win_length):
					if timestamps[j] - timestamps[i] < timedelta(minutes=60):
						c += 1
					else:
						break
				
				top_k_busiest_windows = top_k_elements(
									{ts_to_str(timestamps[i]): c},
									top_k_busiest_windows)
				k = j
		top_k_busiest_windows = top_k_elements(
									{ts_to_str(timestamps[i]): 1},
									top_k_busiest_windows)
		if top_k_busiest_windows[0][1] == ts_to_str(datetime(1900, 01, 01, 00, 00, 00)):
			heapq.heappop(top_k_busiest_windows)

	with open(feature1_output,"w") as f1_file:
		top_k_host = sort_heap(top_k_host)
		for pair in iter(top_k_host):
			f1_file.write(''.join([pair[1],',',str(pair[0]),"\n"]))

	with open(feature2_output,"w") as f2_file:
		top_k_resources = sort_heap(top_k_resources)
		for pair in iter(top_k_resources):
			f2_file.write(''.join([pair[1],"\n"]))

	with open(feature3_output,"w") as f3_file:
		top_k_busiest_windows = sort_heap(top_k_busiest_windows)
		for pair in iter(top_k_busiest_windows):
			f3_file.write(''.join([pair[1],',',str(pair[0]),"\n"]))

	with open(feature4_output,"w") as f4_file:
		for blocked_log in iter(blocked_attempts):
			f4_file.write(blocked_log)
			
if __name__ == "__main__":
	analyze_server_logs()
