#!/usr/bin/env python3

import configparser
import subprocess
import time
import os
import threading

BASE_PATH = '/mnt/backup/uberbackup'

class UberBackupJob:
	def __init__(self):
		self.name = ''
		self.host = ''
		self.path = ''
		self.excludes = []
		self.isRunning = False
		self.lastBackup = '1970-01-01'
		
class UberBackup:
	def __init__(self, basepath):
		self._basePath = basepath
		self._configParser = configparser.ConfigParser()
		self._jobs = []
		self._ssh_user = ''
		self._date_format = '%Y-%m-%d'
		
	def _log(self, line):
		print(time.strftime("%d-%m-%Y %H:%M:%S") + ': ' + line)
		
	def _loadConfig(self):
		self._log('Loading config...')
		
		self._configParser.read([ self._basePath + '/conf/uberbackup.conf' ])
		
		self._ssh_user = self._configParser['GLOBAL']['ssh_user']
		self._ssh_key = self._configParser['GLOBAL']['ssh_key']
		self._ssh_opts = self._configParser['GLOBAL']['ssh_opts']
		self._rsync_opts = self._configParser['GLOBAL']['rsync_opts']
		self._mailto = self._configParser['GLOBAL']['mailto']
		self._max_backups = int(self._configParser['GLOBAL']['max_backups'])
		self._max_jobs = int(self._configParser['GLOBAL']['max_jobs'])
		
		for sect in self._configParser.sections():
			if sect == 'GLOBAL':
				continue
				
			job = UberBackupJob()
			job.name = sect
			job.host = self._configParser[sect]['host']
			job.path = self._configParser[sect]['path']
			job.excludes = self._configParser[sect]['exclude'].split("\n")
			
			list = self._getBackups(job)
			if list:
				job.lastBackup = list[-1]
				
			self._jobs.append(job)
			
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
		self._log('Preparing job ' + job.name + '...')
		# Upewniamy sie ze katalog istnieje
		if not os.path.exists(self._basePath + '/data/' + job.name):
			os.mkdir(self._basePath + '/data/' + job.name)
			
		# Jezeli katalog current istnieje znaczy ze kopia nie wykonala sie do konca
		if not os.path.exists(self._basePath + '/data/' + job.name + '/current'):
			# Rotowanie katalogow
			list = self._getBackups(job)
			
			if list:
				# TODO: Kasowanie starych kopii
				while len(list) >= self._max_backups:
					item = list.pop(0)
					self._log('Removing directory ' + job.name + '/' + item + '...')
					subprocess.call(['rm', '-rf', self._basePath + '/data/' + job.name + '/' + item], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
					
				# Linkujemy ostatni katalog jako bierząca kopia
				self._log('Linking directory ' + job.name + '/' + list[-1] + ' -> ' + job.name + '/current...')
				lastDir = self._basePath + '/data/' + job.name + '/' + list[-1]			
				cp_cmd = ['cp', '-al', lastDir, self._basePath + '/data/' + job.name + '/current']			
				code = subprocess.call(cp_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
				
				# TODO: Sprawdz kod błędu
			else:
				os.mkdir(self._basePath + '/data/' + job.name + '/current')
				
				
	def _execJob(self, job):	
		self._prepareJob(job)

		self._log('Starting job ' + job.name + '...')
		excludes = ''
		for e in job.excludes:
			excludes = excludes + '--exclude=' + e + ' '
			
		rsync_cmd = [ 'rsync' ] +  self._rsync_opts.split() + [ '-e', 'ssh ' + self._ssh_opts + ' -i ' + self._ssh_key, '--rsync-path=sudo rsync ' + excludes, self._ssh_user + '@' + job.host + ':' + job.path, self._basePath + '/data/' + job.name + '/current' ]

		code = subprocess.call(rsync_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
		
		# Transfer bezbłędny lub częściowy z usuniętymi plikami źródłowymi
		if code == 0 or code == 24:
			# Zmieniamy nazwe katalogu na dzisiejsza date
			os.rename(self._basePath + '/data/' + job.name + '/current', self._basePath + '/data/' + job.name + '/' + time.strftime(self._date_format))			
			self._log('Job ' + job.name + ' finished successfully')			
		else:
			self._log('Job ' + job.name + ' failed (code = ' + str(code) + ')')
			
		job.isRunning = False
		
					
	def start(self):
		self._loadConfig()
		
		idx = 0		
		while True:
			if threading.active_count() - 1 < self._max_jobs:
				job = self._jobs[idx]
				idx = idx + 1
				if idx >= len(self._jobs):
					self._loadConfig() # Przeladowywujemy konfiguracje
					idx = 0
					
				if job.isRunning or self._checkJob(job):
					continue
					
				job.isRunning = True
				threading.Thread(target = self._execJob, args=(job,)).start()
								
			time.sleep(30)
			
		
if __name__ == '__main__':
	ub = UberBackup(BASE_PATH)
	ub.start()
	