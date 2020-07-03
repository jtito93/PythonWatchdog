import sys
import time
import logging
import datetime
import shutil
import subprocess
import os
from threading import Thread
from queue import Queue
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import PatternMatchingEventHandler


class LoaderWatchdog(PatternMatchingEventHandler):
	def __init__(self,queue):
		PatternMatchingEventHandler.__init__(self, patterns=['*.txt'],
			ignore_directories=True, case_sensitive=False)
		self.queue = queue

	def on_modified(self, event):
		now = datetime.datetime.utcnow()
		date = now.strftime("%Y/%m/%d %H:%M:%S")
		print('{0} - File added to queue: {1}'.format(date, event.src_path.lower()))
		self.queue.put(event)

	on_created = on_modified
	

def process_load_queue(q):
	'''This is the worker thread function. It is run as a daemon 
	   threads that only exit when the main thread ends.

	   Args
	   ==========
		 q:  Queue() object
	'''
	while True:
		if not q.empty():
			event = q.get()
			now = datetime.datetime.utcnow()
			date = now.strftime("%Y/%m/%d %H:%M:%S")
			
			print("{0} -- File pulled off the queue: {1}".format(date, event.src_path))
			
			time.sleep(2.5)
			date = now.strftime("%Y/%m/%d %H:%M:%S")
			print("{0} -- Processing file: {1}".format(date, event.src_path))
			time.sleep(2.5)
						
			# once done, remove the trigger file
			os.remove(event.src_path)
			date = now.strftime("%Y/%m/%d %H:%M:%S")
			
			# log the operation has been completed successfully
			print("{0} -- Finished processing file: {1}".format(date, event.src_path))
			
			# pending queue
			date = now.strftime("%Y/%m/%d %H:%M:%S")
			print( "{0} - Pending queue: {1} file(s).".format(date, len(list(q.queue))))
			
		else:
			time.sleep(1)

if __name__ == '__main__':
	
	# create queue
	watchdog_queue = Queue()

	# Set up a worker thread to process database load
	worker = Thread(target=process_load_queue, args=(watchdog_queue,))
	worker.setDaemon(True)
	worker.start()

	# setup watchdog to monitor directory for trigger files
	# path = '/Users/MyMac/Downloads/Test/'
	paths = ['/Users/MyMac/Downloads/Test/','/Users/MyMac/Downloads/Test2/']
	event_handler = LoaderWatchdog(watchdog_queue)
	observer = Observer()
	
	for path in paths:
		observer.schedule(event_handler, path)
	
	# observer.schedule(event_handler, path)
	observer.start()

	try:
		while True:
			time.sleep(2)
	except KeyboardInterrupt:
		observer.stop()

	observer.join()
