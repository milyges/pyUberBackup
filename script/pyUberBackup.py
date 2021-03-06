#!/usr/bin/env python3

import configparser
import subprocess
import time
import datetime
import os
import threading
import sys
import signal
import errno

class UberBackupJob:
	def __init__(self):
		self.name = ''
		self.host = ''
		self.path = ''
		self.excludes = []
		self.includes = []
		self.isRunning = False
		self.lastBackup = '1970-01-01'
		self.enabled = False
		
class UberBackup:
	COLOR_GREEN = '\033[1;32m'
	COLOR_RED = '\033[1;31m'
	COLOR_YELLOW = '\033[1;33m'
	COLOR_CYAN = '\033[1;36m'
	PID_FILE = '/run/pyUberBackup.pid'
	
	def __init__(self, basepath):
		self._basePath = basepath
		self._configParser = configparser.ConfigParser()
		self._jobs = []
		self._date_format = '%Y-%m-%d'
		self._pidFile = None
		self._serviceRunning = False
		self._mailto = ''
		self._logFileName = ''
		self._logLock = threading.Lock()
		self._jobsSemaphore = None

	def _log(self, line, color = ''):
		self._logLock.acquire()
		print('\r\033[K' + time.strftime("%d-%m-%Y %H:%M:%S") + ': ' + color + line + '\033[0m')

		self._logLock.release()

	def _loadConfig(self):
		self._configParser.read([ self._basePath + '/conf/uberbackup.conf' ])
		
		# Opcje które muszą być określone
		try:
			self._ssh_user = self._configParser['GLOBAL']['ssh_user']
			self._ssh_key = self._configParser['GLOBAL']['ssh_key']
			self._ssh_opts = self._configParser['GLOBAL']['ssh_opts']
			self._rsync_opts = self._configParser['GLOBAL']['rsync_opts']			
			self._max_backups = int(self._configParser['GLOBAL']['max_backups'])
			self._max_jobs = int(self._configParser['GLOBAL']['max_jobs'])
		except KeyError as e:
			self._log("Fatal: missing config option: %s" % (str(e)), self.COLOR_RED)
			return False
		
		# Opcje opcjonalne
		try:
			self._mailto = self._configParser['GLOBAL']['mailto']
			self._logFileName = self._configParser['GLOBAL']['log']
		except KeyError:
			pass
		
		self._jobs = [ ]
		
		for sect in self._configParser.sections():
			if sect == 'GLOBAL':
				continue
				
			job = UberBackupJob()
			job.name = sect
			
			try:
				job.host = self._configParser[sect]['host']
				job.path = self._configParser[sect]['path']
				job.enabled = self._configParser.getboolean(sect, 'enabled')
			except KeyError as e:
				self._log("Warning: ignoring job: %s: missing config option: %s" % (sect, str(e)), self.COLOR_YELLOW)
				continue
			
			try:				
				job.excludes = self._configParser[sect]['exclude'].split("\n")
			except KeyError:
				pass
			
			try:				
				job.includes = self._configParser[sect]['include'].split("\n")
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

		while True:
			# Sprawdzamy czy host odpowiada na pingi
			code = subprocess.call(['ping', '-c', '1', '-q', job.host ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
			if code != 0:
				self._log('Job ' + job.name + ' failed (host not found)', self.COLOR_YELLOW)
				break

			excludes = ''
			for e in job.excludes:
				excludes = excludes + '--exclude=' + e + ' '
			
			includes = ''
			for i in job.includes:
				includes = includes + '--include=' + i + ' '
			
			rsync_cmd = [ 'rsync' ] +  self._rsync_opts.split() + [ '-e', 'ssh ' + self._ssh_opts + ' -i ' + self._ssh_key, '--rsync-path=sudo rsync ' + excludes + includes, self._ssh_user + '@' + job.host + ':' + job.path, self._basePath + '/data/' + job.name + '/current' ]

			code = subprocess.call(rsync_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
		
			# Transfer bezbłędny lub częściowy z usuniętymi plikami źródłowymi
			if code == 0 or code == 24:
				# Zmieniamy nazwe katalogu na dzisiejsza date
				os.rename(self._basePath + '/data/' + job.name + '/current', self._basePath + '/data/' + job.name + '/' + time.strftime(self._date_format))			
				self._log('Job ' + job.name + ' finished successfully', self.COLOR_GREEN)
			elif code == 30: # Timeout, restartujemy zadanie
				self._log('I/O timeout, restarting job ' + job.name + '...', self.COLOR_CYAN)
				continue
			else:
				self._log('Job ' + job.name + ' failed (code = ' + str(code) + ')', self.COLOR_RED)
			
			break

		# Zwalniamy semafor zadan
		job.isRunning = False
		self._jobsSemaphore.release()

	def getServicePID(self):
		try:
			self._pidFile = open(self.PID_FILE, 'r')
		except FileNotFoundError:
			return -1
		return int(self._pidFile.read())
		

	def service(self):
		try:
			pidFD = os.open(self.PID_FILE, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
		except OSError as e:
			if e.errno == errno.EEXIST:
				self._log("Service already running (or PID file not removed)", self.COLOR_RED)
				return 1
			else:
				self._log("Can not create PID file, exiting...");
				return 2

		self._pidFile = os.fdopen(pidFD, 'w')
		self._pidFile.write("%d\n" % (os.getpid()))
		self._pidFile.flush()
		
		if not self._loadConfig():
			self._log("Can't load config file", self.COLOR_RED)
			return -1

		idx = 0

		self._jobsSemaphore = threading.Semaphore(self._max_jobs)
		self._serviceRunning = True
		while self._serviceRunning:
			job = self._jobs[idx]
			idx = idx + 1
			if idx >= len(self._jobs):
				self._rescheduleJobs()
				idx = 0
					
			if not job.enabled or job.isRunning or self._checkJob(job):
				continue
			
			# Podnosimy semafor zadan
			self._jobsSemaphore.acquire()
			if not self._serviceRunning:
				break

			job.isRunning = True
			threading.Thread(target = self._execJob, args=(job,)).start()
			time.sleep(10)
			
		self._pidFile.close()
		try:
			os.unlink(self.PID_FILE);
		except:
			pass
		
		return 0

	def serviceExit(self, num, *kwargs):
		self._log("Received signal %d, exiting..." % (num));
		self._serviceRunning = False

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
				if not job.enabled:
					color = self.COLOR_CYAN
				elif delta.days <= 1:
					color = self.COLOR_GREEN
				elif delta.days <= 7:
					color = self.COLOR_YELLOW
				else:
					color = self.COLOR_RED
				
			if job.enabled:
				print("%s%s: %s (%d days ago)%s" % (color, job.name.ljust(48), job.lastBackup, delta.days, finishColor))
			else:
				print("%s%s: %s (%d days ago, job disabled)%s" % (color, job.name.ljust(48), job.lastBackup, delta.days, finishColor))
			
		return 0

if __name__ == '__main__':
	basepath = os.path.dirname(os.getcwd() + '/' + sys.argv[0]) + '/..'
	ub = UberBackup(basepath)
	
	if len(sys.argv) < 2:
		print("Usage: %s command" % (sys.argv[0]))
		print("Supported commands:")
#		print("  start        start service in background")		
		print("  stop         stop service running in background")		
		print("  debug        start service in debug mode")
		print("  status       show backup status")
		sys.exit(1)

	if sys.argv[1] == 'stop':
		pid = ub.getServicePID()
		if pid == -1:
			print('Stop failed: Service not running (or PID file removed).')
			sys.exit(1)
		os.kill(pid, signal.SIGTERM)
		sys.exit(0)
	elif sys.argv[1] == 'debug':
		signal.signal(signal.SIGTERM, ub.serviceExit)
		signal.signal(signal.SIGINT, ub.serviceExit)
		code = ub.service()
		# Zabijamy wszystkie dzieci
		signal.signal(signal.SIGTERM, signal.SIG_IGN)
		os.kill(0, signal.SIGTERM)
		sys.exit(code)
	elif sys.argv[1] == 'status':
		sys.exit(ub.status())
	else:
		print("%s: Unknown command '%s'" % (sys.argv[0], sys.argv[1]))
		sys.exit(2)

