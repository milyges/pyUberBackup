#!/usr/bin/env python3

import configparser
import subprocess
import time
import datetime
import os
import threading
import sys

class UberBackupJob:
	def __init__(self):
		self.name = ''
		self.host = ''
		self.path = ''
		self.excludes = []
		self.isRunning = False
		self.lastBackup = '1970-01-01'
		
class UberBackup:
	COLOR_GREEN = '\033[1;32m'
	COLOR_RED = '\033[1;31m'
	COLOR_YELLOW = '\033[1;33m'
	COLOR_CYAN = '\033[1;36m'
	
	def __init__(self, basepath):
		self._basePath = basepath
		self._configParser = configparser.ConfigParser()
		self._jobs = []
		self._ssh_user = ''
		self._date_format = '%Y-%m-%d'
		
	def _log(self, line, color = ''):		
		print('\r\033[K' + time.strftime("%d-%m-%Y %H:%M:%S") + ': ' + color + line + '\033[0m')
		
	def _loadConfig(self):
		self._configParser.read([ self._basePath + '/conf/uberbackup.conf' ])
		
		try:
			self._ssh_user = self._configParser['GLOBAL']['ssh_user']
			self._ssh_key = self._configParser['GLOBAL']['ssh_key']
			self._ssh_opts = self._configParser['GLOBAL']['ssh_opts']
			self._rsync_opts = self._configParser['GLOBAL']['rsync_opts']
			self._mailto = self._configParser['GLOBAL']['mailto']
			self._max_backups = int(self._configParser['GLOBAL']['max_backups'])
			self._max_jobs = int(self._configParser['GLOBAL']['max_jobs'])
		except KeyError as e:
			self._log("Fatal: missing config options: %s" % (str(e)), self.COLOR_RED)
			return False
		
		self._jobs = [ ]
		
		for sect in self._configParser.sections():
			if sect == 'GLOBAL':
				continue
				
			job = UberBackupJob()
			job.name = sect
			
			try:
				job.host = self._configParser[sect]['host']
				job.path = self._configParser[sect]['path']
			except KeyError as e:
				self._log("Warning: ignoring job: %s: missing config options: %s" % (sect, str(e)), self.COLOR_YELLOW)
				continue
			
			try:
				job.excludes = self._configParser[sect]['exclude'].split("\n")
			except KeyError:
				pass
			
			self._jobs.append(job)
			
		self._rescheduleJobs();
		
		return True
		
		
	# Sortujemy zadania - pierwsze są te które mają najstarsze kopie
	def _rescheduleJobs(self):
		for job in self._jobs:
			list = self._getBackups(job)
			
			if list:
				job.lastBackup = list[-1]

		self._jobs.sort(key=lambda x: x.lastBackup)
		
	
	def _getBackups(self,job):
		list = []
		if not os.path.exists(self._basePath + '/data/' + job.name):
			return list

		for item in os.listdir(self._basePath + '/data/' + job.name):
			if os.path.isdir(self._basePath + '/data/' + job.name + '/' + item) and not item == 'current':
				list.append(item)
				
		return sorted(list)
		
	def _checkJob(self, job):
		# Sprawdzamy czy mamy backup z dzis
		return os.path.exists(self._basePath + '/data/' + job.name + '/' + time.strftime(self._date_format))
		
	def _prepareJob(self, job):
		self._log('Preparing job ' + job.name + '...', self.COLOR_CYAN)
		# Upewniamy sie ze katalog istnieje
		if not os.path.exists(self._basePath + '/data/' + job.name):
			os.mkdir(self._basePath + '/data/' + job.name)
			
		# Jezeli katalog current istnieje znaczy ze kopia nie wykonala sie do konca
		if not os.path.exists(self._basePath + '/data/' + job.name + '/current'):
			# Rotowanie katalogow
			list = self._getBackups(job)
			
			if list:
				# Kasowanie starych kopii
				while len(list) >= self._max_backups:
					item = list.pop(0)
					self._log('Removing directory ' + job.name + '/' + item + '...', self.COLOR_CYAN)
					subprocess.call(['rm', '-rf', self._basePath + '/data/' + job.name + '/' + item], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
					
				# Linkujemy ostatni katalog jako bierząca kopia
				self._log('Linking directory ' + job.name + '/' + list[-1] + ' -> ' + job.name + '/current...', self.COLOR_CYAN)
				lastDir = self._basePath + '/data/' + job.name + '/' + list[-1]			
				cp_cmd = ['cp', '-al', lastDir, self._basePath + '/data/' + job.name + '/current']			
				code = subprocess.call(cp_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
				
				# TODO: Sprawdz kod błędu
			else:
				os.mkdir(self._basePath + '/data/' + job.name + '/current')
				
				
	def _execJob(self, job):	
		self._prepareJob(job)

		self._log('Starting job ' + job.name + '...', self.COLOR_CYAN)
		excludes = ''
		for e in job.excludes:
			excludes = excludes + '--exclude=' + e + ' '
			
		rsync_cmd = [ 'rsync' ] +  self._rsync_opts.split() + [ '-e', 'ssh ' + self._ssh_opts + ' -i ' + self._ssh_key, '--rsync-path=sudo rsync ' + excludes, self._ssh_user + '@' + job.host + ':' + job.path, self._basePath + '/data/' + job.name + '/current' ]

		code = subprocess.call(rsync_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
		
		# Transfer bezbłędny lub częściowy z usuniętymi plikami źródłowymi
		if code == 0 or code == 24:
			# Zmieniamy nazwe katalogu na dzisiejsza date
			os.rename(self._basePath + '/data/' + job.name + '/current', self._basePath + '/data/' + job.name + '/' + time.strftime(self._date_format))			
			self._log('Job ' + job.name + ' finished successfully', self.COLOR_GREEN)
		else:
			self._log('Job ' + job.name + ' failed (code = ' + str(code) + ')', self.COLOR_RED)
			
		job.isRunning = False
		
		
	def service(self):
		if not self._loadConfig():
			self._log("Can't load config file", self.COLOR_RED)
			return -1
			
		idx = 0
		while True:
			if threading.active_count() - 1 < self._max_jobs:
				job = self._jobs[idx]
				idx = idx + 1
				if idx >= len(self._jobs):
					self._rescheduleJobs()
					idx = 0
					
				if job.isRunning or self._checkJob(job):
					continue
					
				job.isRunning = True
				threading.Thread(target = self._execJob, args=(job,)).start()
			
			time.sleep(15)
			
		return 0
			
	def status(self):
		if not self._loadConfig():
			self._log("Can't load config file", self.COLOR_RED)
			return -1
			
		print('----- UberBackup Status -----')
		print('Last backups sucessfull backups:')
		currentDate = datetime.datetime.now().date()		
		for job in self._jobs:	
			color = ''
			finishColor = ''
			lastDate = datetime.datetime.strptime(job.lastBackup, self._date_format).date()				
			delta = currentDate - lastDate
			if sys.stdout.isatty():			
				finishColor = '\033[0m'								
				if delta.days <= 1:
					color = self.COLOR_GREEN
				elif delta.days <= 5:
					color = self.COLOR_YELLOW
				else:
					color = self.COLOR_RED
				
			print("%s%s: %s (%d days ago)%s" % (color, job.name.ljust(48), job.lastBackup, delta.days, finishColor))
			
		return 0
		
if __name__ == '__main__':
	basepath = os.path.dirname(os.getcwd() + '/' + sys.argv[0]) + '/..'
	ub = UberBackup(basepath)
	
	if len(sys.argv) < 2:
		print("Usage: %s [service | status]" % (sys.argv[0]))
#		print("  service   start service in background")
		print("  status    show backup status")
		print("  debug     start service in debug mode")
		sys.exit(1)
	
	if sys.argv[1] == 'debug':
		sys.exit(ub.service())		
	elif sys.argv[1] == 'status':		
		sys.exit(ub.status())
	else:
		print("%s: Unknown command '%s'" % (sys.argv[0], sys.argv[1]))
		sys.exit(2)
		