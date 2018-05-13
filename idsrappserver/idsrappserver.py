#!/usr/bin/env python

import requests
#import time
import string
import random
import json
import datetime
import pandas as pd
import numpy as np

import moment
from operator import itemgetter

class IdsrAppServer:
	def __init__(self):
		self.dataStore = "ugxzr_idsr_app"
		self.period = "LAST_52_WEEKS"
		self.ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
		self.ID_LENGTH = 11
		self.today = moment.now().format('YYYY-MM-DD')
		print("Detection script started on %s" %self.today)


		self.url = "https://hmis.moh.gov.rw/hietest/api/27/"
		self.username = 'talexie'
		self.password = 'Pa55w0rd'
		# orgUnits
		self.rootUid = 'Hjw70Lodtf2'

		# programs
		self.programUid = 'U86iDWxDek8'
		self.outbreakProgram = 'y9LMXyKYXSI'
		# Program Stages
		self.labResultStage = 'cYEsxIe5jxL'
		self.caseMonitoringStage = ''

		# TE Attributes
		self.dateOfOnsetUid = 'adJ527HOTea'
		self.conditionOrDiseaseUid = 'uOTHyxNv2W4'

		# Lab Result Stage Data Elements
		self.newConditionOrDiseaseUid = ""

		self.epidemics = {}

		self.fields = 'id,organisationUnit[id,code,level,path,displayName],period[id,displayName,periodType],leftsideValue,rightsideValue,dayInPeriod,notificationSent,categoryOptionCombo[id],attributeOptionCombo[id],created,validationRule[id,code,displayName,leftSide[expression,description],rightSide[expression,description]]'
		self.eventEndPoint = 'analytics/events/query/'

	def checkInt(self,s):
		try:
			return int(float(s))
		except ValueError:
			return ""

	def checkValue(self,s):
		try:
			return s
		except ValueError:
			return ""

	def checkKeyDate(self,key,values):
		try:
			if key in values:
				return self.getIsoWeek(key)
			else:
				return ""
		except	ValueError:
			return ""

	def checkDate(self,key,values):
		try:
			if key in values:
				return key
			else:
				return ""
		except ValueError:
			return ""

	def checkKey(self,key,values):
		try:
			if key in values:
				return key
			else:
				return ""
		except KeyError:
			return "KeyError"

	def checkKeyFormatDate(self,key,values):
		try:
			if key in values:
				return self.formatIsoDate(key)
			else:
				return ""
		except	ValueError:
			return ""

	def getIsoWeek(self,d):
		ddate = datetime.datetime.strptime(d,'%Y-%m-%d')
		return datetime.datetime.strftime(ddate, '%YW%W')

	def formatIsoDate(self,d):
		return moment.date(d).format('YYYY-MM-DD')

	def getDateDifference(self,d1,d2):
		if d1 and d2 :			
			delta = moment.date(d1) - moment.date(d2)
			return delta.days
		else:
			return ""

	def addDays(self,d1,days):
		if d1:
			newDay = moment.date(d1).add(days=days)
			return newDay.format('YYYY-MM-DD')
		else:
			return ""
	# create aggregate threshold period
	# @param n number of years
	# @param m number of periods 
	# @param type seasonal (SEASONAL) or Non-seasonal (NON_SEASONAL) or case based (CASE_BASED)
	def createAggThresholdPeriod(self,n,m,type):
		periods = []
		currentDate = moment.now().format('YYYY-MM-DD')
		currentYear = self.getIsoWeek(currentDate)		
		if(type == 'SEASONAL'):

			for year in range(0,n,1):
				currentYDate = moment.date(currentDate).subtract(months=((year +1)*12)).format('YYYY-MM-DD')
				for week in range(0,m,1):
					currentWDate = moment.date(currentYDate).subtract(weeks=week).format('YYYY-MM-DD')
					pe = self.getIsoWeek(currentWDate)					
					periods.append(pe)
		elif(type == 'NON_SEASONAL'):
			for week in range(0,m,1):
				currentWDate = moment.date(currentDate).subtract(weeks=week).format('YYYY-MM-DD')
				pe = self.getIsoWeek(currentWDate)					
				periods.append(pe)
		else:
			pe = 'LAST_7_DAYS'
			periods.append(pe)
		return periods

	def getHttpData(self,url,fields,username,password,params):
		url = url+fields+".json"
		data = requests.get(url, auth=(username, password),params=params)
		if(data.status_code == 200):
			return data.json()
		else:
			return 'HTTP_ERROR'

	def getHttpDataWithId(self,url,fields,idx,username,password,params):
		url = url + fields + "/"+ idx + ".json"
		data = requests.get(url, auth=(username, password),params=params)

		if(data.status_code == 200):
			return data.json()
		else:
			return 'HTTP_ERROR'

	# Post data
	def postJsonData(self,url,endPoint,username,password,data):
		url = url+endPoint
		submittedData = requests.post(url, auth=(username, password),json=data)
		return submittedData

	# Post data with parameters
	def postJsonDataWithParams(self,url,endPoint,username,password,data,params):
		url = url+endPoint
		submittedData = requests.post(url, auth=(username, password),json=data,params=params)
		return submittedData

	# Update data
	def updateJsonData(self,url,endPoint,username,password,data):
		url = url+endPoint
		submittedData = requests.put(url, auth=(username, password),json=data)
		return submittedData

	# Get array from Object Array

	def getArrayFromObject(self,arrayObject):
		arrayObj = []
		for obj in arrayObject:
			arrayObj.append(obj['id'])
		return arrayObj

	# Check datastore existance

	def checkDataStore(self,url,fields,username,password,params):
		url = url+fields+".json"
		storesValues = {"exists": "false", "stores": []}
		httpData = requests.get(url, auth=(username, password),params=params)
		if(httpData.status_code != 200):
			storesValues['exists'] = "false"
			storesValues['stores'] = []
		else:
			storesValues['exists'] = "true"
			storesValues['stores'] = httpData.json()
		return storesValues

	# Get orgUnit
	def getOrgUnit(self,detectionOu,ous):
		ou = []
		if((ous !='undefined') and len(ous) > 0):
			for oux in ous:
				if(oux['id'] == detectionOu):
					return oux['ancestors']
		else:
			return ou

	# Get orgUnit UID 
	def getOrgUnitId(self,detectionOu,ous,level):
		ou = []
		if((ous !='undefined') and len(ous) > 0):
			for oux in ous:
				if(oux['id'] == detectionOu):
					return oux['ancestors'][level]['id']
		else:
			return ou


	# filter datastore self.epidemics by orgUnit,disease without enddate
	# @param validationRuleId represents a disease (see disease meta data in datastore)

	def filterEpidemicsByOrgUnitAndDiseaseNoEndDate(self,values,disease,orgUnit):
		valueResults = []
		if((values != 'undefined') and len(values) > 0):
			for key in values:
				found = "false"
				if len(key.keys()) > 0:
					if((key['disease'] == disease) and (key['orgUnit'] == orgUnit ) and (key['endDate'] == "")):
						found = "true"
						key['exists'] = "true"
						valueResults.append(key)
		return valueResults

	# filter datastore self.epidemics by orgUnit,disease and not active
	# @param 

	def findClosedEpidemics(self,values,disease,orgUnit):
		closedResults = []
		if((values != 'undefined') and len(values) > 0):
			for key in values:
				if len(key.keys()) > 0:
					if((key['disease'] == disease) and (key['orgUnit'] == orgUnit ) and (key['active'] == "false")):
						closedResults.append(key)

		return closedResults


	# Get events by orgUnit

	def getEventsByOrgUnit(self,events,orgUnit):
		eventsByOrgUnit = []
		sortedEventsByOrgUnit = []
		if(events != 'undefined'):
			if(len(events['rows']) > 0):
				for event in events['rows']:
					
					if( event != 'undefined'):
						if(event[7] == orgUnit):
							if(event[8] == ''):
								event[8] = event[2]
								eventsByOrgUnit.append(event)
							else:
								eventsByOrgUnit.append(event)

			sortedEventsByOrgUnit = sorted(eventsByOrgUnit, key=itemgetter(8))
			
		return sortedEventsByOrgUnit

	# Get existing epidemics and update them
	def updateCurrent(self,old,current):
		if old !='undefined':
			for cur in current:					
				old.append(cur)			
		return old;

	# Generate code

	def generateCode(self):
		size = self.ID_LENGTH
		chars = string.ascii_uppercase + string.digits
		code = ''.join(random.choice(chars) for x in range(size))
		return code

	def formatMessage(self,usergroups,outbreak):
		message = {}
		message['organisationUnits'] = []
		print('outbreak',outbreak)
		message['subject'] = outbreak['disease'] + " outbreak in " + outbreak['orgUnitName']
		message['text'] = "Dear all, Epidemic threshold for " + outbreak['disease'] + "  is reached at " + outbreak['orgUnitName'] + " of " + self.checkKey('reportingOrgUnitName',outbreak)  + " in " + self.checkKeyDate('startDate',outbreak)

		message['userGroups'] = usergroups
		message['organisationUnits'].append({"id": outbreak['orgUnit']})
		message['organisationUnits'].append({"id": outbreak['reportingOrgUnit']})

		return (message)

	def formatAlertMessage(self,usergroups,outbreak):
		message = {}
		message['organisationUnits'] = []

		message['subject'] = outbreak['disease'] + " alert"
		message['text'] = "Dear all, Alert threshold for " + outbreak['disease'] + "  is reached at " + outbreak['orgUnitName'] + " of " + self.checkKey('reportingOrgUnitName',outbreak)  + " in " + self.checkKeyDate('startDate',outbreak)
		message['userGroups'] = usergroups;
		message['organisationUnits'].append({"id": outbreak['orgUnit']})
		message['organisationUnits'].append({"id": outbreak['reportingOrgUnit']})

		return (message)

	def sendSmsAndEmailMessage(self,message):
		messageEndPoint = "messageConversations"
		sentMessages = self.postJsonData(self.url,messageEndPoint,self.username,self.password,message)
		return sentMessages
		#return 0

	# create alerts data

	def createAlerts(self,values):

		messageConversations = []
		for val in values:
			message = {}
			if(val['newEvent']):
				messageConversations.append(self.formatAlertMessage(self.checkKey('notifiableUserGroups',val),val))
			else:
				print("Error sending alerts")

		self.sendSmsAndEmailMessage(messageConversations)
		return messageConversations
	# create colums from event data
	def createColumns(self,headers):
		cols = []
		for header in headers:
			cols.append(header['column'])
		return cols




	# Group last 7 days cases by orgunit
	def groupByOrgUnit(self,eventDataFrame):
		groupedByOrgUnit = eventDataFrame.groupby('organisationunitid').sum()
		return groupedByOrgUnit

	# get Epidemic by Threshold
	#
	def getEpiByThreshold(self,detectionObject,pi,detectionLevel,reportingLevel,count,diseaseMeta,ou):

		epiObject = {}
		if((pi[detectionLevel+4]) and (pi[detectionLevel+4]).strip()):
			if(int(float(pi[detectionLevel+4])) >= int(diseaseMeta['epiThreshold'])):
				count = count+1
				detectionObject['newEvent'] = "true"
				detectionObject['reportingOrgUnitName'] = pi[reportingLevel-1]

				detectionObject['reportingOrgUnit'] = (self.getOrgUnit(pi[detectionLevel],ou))[reportingLevel-1]['id']

				detectionObject['orgUnit'] = pi[detectionLevel]
				detectionObject['orgUnitName'] = pi[detectionLevel+1]
				detectionObject['orgUnitCode'] = pi[detectionLevel+2]
				detectionObject['epidemic'] = "true"
				detectionObject['confirmedValue'] = self.checkInt(pi[detectionLevel+4])
				detectionObject['deathValue'] = self.checkInt(pi[detectionLevel+5])
				detectionObject['suspectedValue'] = self.checkInt(pi[detectionLevel+6])
				detectionObject['disease'] = diseaseMeta['disease']
				detectionObject['startDate'] = self.today
				detectionObject['endDate'] = ""
				detectionObject['sendReminder'] = "false"
				if(((pi[detectionLevel+2]) != 'undefined') and (pi[detectionLevel+2] !="")):
					detectionObject['epicode'] = "E_" + pi[detectionLevel+2] + "_" + self.generateCode()
				else:
					detectionObject['epicode'] = "E_" + self.generateCode()
				detectionObject['incubationDays'] = int(diseaseMeta['incubationDays'])
				detectionObject['notifiableUserGroups'] = diseaseMeta['notifiableUserGroups']
				# Add the epidemics				
			#else:
				# Do nothing
				
		#else:
			#print("No outbreaks detected for ",pi[detectionLevel+4])
		epiObject['count'] = count
		epiObject['event'] = detectionObject
		return epiObject

	# get Alert by Threshold
	#
	def getAlertByThreshold(self,alertDetectionObject,pi,detectionLevel,reportingLevel,diseaseMeta,ou):
		if (pi[detectionLevel+4]) and (pi[detectionLevel+4]).strip():
			if((int(float(pi[detectionLevel+4])) >= int(diseaseMeta['alertThreshold'])) and (int(float(pi[detectionLevel+4])) < int(diseaseMeta['epiThreshold']))):
				alertDetectionObject['reportingOrgUnit'] = (self.getOrgUnit(pi[detectionLevel],ou))[reportingLevel-1]['id']
				alertDetectionObject['sendAlert'] = "true"
				alertDetectionObject['orgUnit'] = pi[detectionLevel]
				alertDetectionObject['orgUnitName'] = pi[detectionLevel+1]
				alertDetectionObject['orgUnitCode'] = pi[detectionLevel+2]
				alertDetectionObject['sendReminder'] = "false"
				alertDetectionObject['confirmedValue'] = self.checkInt(pi[detectionLevel+4])
				alertDetectionObject['deathValue'] = self.checkInt(pi[detectionLevel+5])
				alertDetectionObject['suspectedValue'] = self.checkInt(pi[detectionLevel+6])
				alertDetectionObject['disease'] = diseaseMeta['disease']
				alertDetectionObject['incubationDays'] = diseaseMeta['incubationDays']
				alertDetectionObject['notifiableUserGroups'] = diseaseMeta['notifiableUserGroups']
				alertDetectionObject['newEvent'] = "true"
				alertDetectionObject['startDate'] = self.today
			#else:
				# Do nothing
				#print("No outbreaks or alerts detected")			
		#else:
			#print("No outbreak alerts detected",pi[detectionLevel+4])
		return alertDetectionObject

	# Check if aggregate threshold is meant
	def checkAggThreshold(self,current,meanValue):
		if current >= meanValue:
			return 'true'
		else:
			return 'false'

	# create Panda Data Frame from event data
	def createDataFrame(self,events):
		#eventObj = {}
		#for row in events['rows']:
		#	for header in events['headers']:
		#		eventObj[header['column']].push(row[events['headers'].index(header)])
		cols = self.createColumns(events['headers'])
		dataFrame = pd.DataFrame.from_records(events['rows'],columns=cols)
		return dataFrame

	# Detect using aggregated indicators
	# Confirmed, Deaths,Suspected
	def detectOnAggregateIndicators(self,aggData,diseaseMeta,epidemics,ou,periods):
		detectionObjectArray = []
		alerts = []
		detectionObjects = {}
		countNewEpidemics = 0
		newEpidemics = []
		updatedEpidemics = []
		existingEpidemics = epidemics
		detectionLevel = int(diseaseMeta['detectionLevel'])
		reportingLevel = int(diseaseMeta['reportingLevel'])
		m=5
		n=3
		if(aggData != 'HTTP_ERROR'):
			if((aggData != 'undefined') and (aggData['rows'] != 'undefined') and len(aggData['rows']) >0):
				df = self.createDataFrame(aggData)				
				
				dfColLength = len(df.columns)
				df1 = df.iloc[:,(detectionLevel+4):dfColLength]
				df.iloc[:,(detectionLevel+4):dfColLength] = df1.apply(pd.to_numeric,errors='ignore',downcast='integer')
				df.fillna(0.0,axis=1,inplace=True)	
				# print(df.iloc[:,(detectionLevel+4):(detectionLevel+4+m)])	# cases, deaths

				### Make generic functions for math
				df['mean_current_cases'] = df.iloc[:,(detectionLevel+4):(detectionLevel+3+m)].mean(axis=1)
				df['mean_mn_cases'] = df.iloc[:,(detectionLevel+3+m):(detectionLevel+3+m+(m*n))].mean(axis=1)
				df['stddev_mn_cases'] = df.iloc[:,(detectionLevel+3+m):(detectionLevel+3+m+(m*n))].std(axis=1)
				df['mean20std_mn_cases'] = (df.mean_mn_cases + (2*df.stddev_mn_cases))
				df['mean15std_mn_cases'] = (df.mean_mn_cases + (1.5*df.stddev_mn_cases))
				print('d',detectionLevel+4,detectionLevel+3+m,detectionLevel+3+m+(m*n),detectionLevel+3+m+(m*n),detectionLevel+3+(2*m)+(m*n),dfColLength-1)
				df['mean_current_deaths'] = df.iloc[:,(detectionLevel+3+m+(m*n)):(detectionLevel+3+(2*m)+(m*n))].mean(axis=1)
				df['mean_mn_deaths'] = df.iloc[:,(detectionLevel+3+(2*m)+(m*n)):dfColLength-1].mean(axis=1)
				df['stddev_mn_deaths'] = df.iloc[:,(detectionLevel+3+(2*m)+(m*n)):dfColLength-1].std(axis=1)
				df['mean20std_mn_deaths'] = (df.mean_mn_deaths + (2*df.stddev_mn_deaths))
				df['mean15std_mn_deaths'] = (df.mean_mn_deaths + (1.5*df.stddev_mn_deaths))	
				
				df['reportingOrgUnitName'] = df.iloc[:,reportingLevel-1]
				df['reportingOrgUnit'] = df.iloc[:,detectionLevel].apply(self.getOrgUnitId,args=(ou,(reportingLevel-1)))
				df['orgUnit'] = df.iloc[:,detectionLevel]
				df['orgUnitName'] = df.iloc[:,detectionLevel+1]
				df['orgUnitCode'] = df.iloc[:,detectionLevel+2]
				df['epidemic'] = np.where(df['mean_current_cases'] >= df['mean20std_mn_cases'],'true','false')
				# Filter out those greater or equal to threshold
				df = df[df['epidemic'] == 'true']
				df['confirmedValue'] = df.loc[:,'mean_current_cases']
				df['deathValue'] = df.loc[:,'mean_current_deaths']
				df['suspectedValue'] = df.loc[:,'mean_current_cases']
				df['startDate'] = self.today
				# Mid period for seasonal = mean of range(1,(m+1)) where m = number of periods
				midPeriod = int(np.median(range(1,(m+1))))
				df['period']= periods[midPeriod]
				df['endDate'] = ""	
				df['disease'] = diseaseMeta['disease']
				df['incubationDays'] = diseaseMeta['incubationDays']

				df['notifiableUserGroups'] = str(diseaseMeta['notifiableUserGroups'])

			detectionObjects['epidemics'] = df.to_json(orient='records')
			detectionObjects['alerts'] = alerts
			
			detectionObjects['numberOfEpidemics'] = countNewEpidemics
			return detectionObjects
		else:
			print("No outbreaks/epidemics for " + diseaseMeta['disease'])
			detectionObjects['epidemics'] = detectionObjectArray
			detectionObjects['alerts'] = alertDetectionObjectArray
			
			detectionObjects['numberOfEpidemics'] = 0
			return detectionObjects

	# detect self.epidemics
	# Confirmed, Deaths,Suspected
	def detectBasedOnProgramIndicators(self,piData,diseaseMeta,epidemics,ou):
		detectionObjectArray = []
		alerts = []
		detectionObjects = {}
		countNewEpidemics = 0
		newEpidemics = []
		updatedEpidemics = []
		existingEpidemics = epidemics
		n = 3
		m = 5
		detectionLevel = int(diseaseMeta['detectionLevel'])
		reportingLevel = int(diseaseMeta['reportingLevel'])
		if(piData != 'HTTP_ERROR'):
			if((piData != 'undefined') and (piData['rows'] != 'undefined') and len(piData['rows']) >0):
				df = self.createDataFrame(piData)
				dfColLength = len(df.columns)
				df1 = df.iloc[:,(detectionLevel+4):dfColLength]
				df.iloc[:,(detectionLevel+4):dfColLength] = df1.apply(pd.to_numeric,errors='ignore',downcast='integer')
				df.fillna(0.0,axis=1,inplace=True)
				
				df['reportingOrgUnitName'] = df.iloc[:,reportingLevel-1]
				df['reportingOrgUnit'] = df.iloc[:,detectionLevel].apply(self.getOrgUnitId,args=(ou,(reportingLevel-1)))
				df['orgUnit'] = df.iloc[:,detectionLevel]
				df['orgUnitName'] = df.iloc[:,detectionLevel+1]
				df['orgUnitCode'] = df.iloc[:,detectionLevel+2]
				df['confirmedValue'] = df.iloc[:,detectionLevel+4].apply(np.int64)
				df['deathValue'] = df.iloc[:,detectionLevel+5].apply(np.int64)
				df['suspectedValue'] = df.iloc[:,detectionLevel+6].apply(np.int64)
				df['alertThreshold'] = int(diseaseMeta['alertThreshold'])
				df['epiThreshold'] = int(diseaseMeta['epiThreshold'])
				# df['confirmed_suspected_cases'] = df.iloc[:,detectionLevel+6].apply(np.int64) + df.iloc[:,detectionLevel+4].apply(np.int64)
				df['epidemic'] = np.where(df['confirmedValue'] >= df['epiThreshold'],'true','false')
				#df['alert'] = np.where(df['confirmed_suspected_cases'] >= df['alertThreshold'],'true','false')
				# Filter out those greater or equal to threshold
				df = df[df['epidemic'] == 'true']
				# lab confirmed = true
				# type of emergency = outbreak or other
				

				df['startDate'] = self.today
				df['endDate'] = ""	
				df['disease'] = diseaseMeta['disease']
				df['incubationDays'] = diseaseMeta['incubationDays']
				print("df",df.head())
				# piRows = self.groupByOrgUnit(df)
				# for pi in piRows:
				# append alert/epi threshold as columns,then compare and create outbreak
				# filter only those which has passed threshold and the do check and create objects
				for pi in piData['rows']:
					

					detectionObject = {}
					alertDetectionObject = {}
					epidemicExists = self.filterEpidemicsByOrgUnitAndDiseaseNoEndDate(epidemics,diseaseMeta['disease'],pi[detectionLevel])

					if((epidemicExists == 'undefined') or len(epidemicExists) == 0):
						detectedEpi = self.getEpiByThreshold(detectionObject,pi,detectionLevel,reportingLevel,countNewEpidemics,diseaseMeta,ou)
						detectionObject = detectedEpi['event']
						countNewEpidemics = detectedEpi['count']
						# Add the epidemics
						newEpidemics.append(detectionObject)
						alertDetectionObject = self.getAlertByThreshold(alertDetectionObject,pi,detectionLevel,reportingLevel,diseaseMeta,ou)
					else:
						detectionObject = epidemicExists[0]
						detectionObject['confirmedValue'] = self.checkInt(pi[detectionLevel+4])
						detectionObject['deathValue'] = self.checkInt(pi[detectionLevel+5])
						detectionObject['suspectedValue'] = self.checkInt(pi[detectionLevel+6])
						detectionObject['newEvent'] = "false"
						detectionObject['updated'] = "true"
						detectionObject['orgUnitName'] = pi[detectionLevel+1]
						detectionObject['orgUnitCode'] = pi[detectionLevel+2]
						detectionObject['reportingOrgUnitName'] = pi[reportingLevel-1]
						detectionObject['incubationDays'] = diseaseMeta['incubationDays']

						#alertDetectionObject = epidemicExists[0]
						#alertDetectionObject['newEvent'] = "false"
						#alertDetectionObject['updated'] = "true"
						#alertDetectionObject['incubationDays'] = diseaseMeta['incubationDays']
						

					if(len(list(alertDetectionObject.keys())) > 0):
						alerts = self.updateExistingAlerts(alerts,alertDetectionObject)

					if(len(list(detectionObject.keys())) > 0):
						detectionObjectArray.append(detectionObject)

			detectionObjects['epidemics'] = detectionObjectArray
			detectionObjects['alerts'] = alerts
			
			detectionObjects['numberOfEpidemics'] = countNewEpidemics
			#Update datastore for alerts

			###updateDataStoreEndPoint  = 'dataStore/' + self.dataStore + '/alerts'
			###self.updateJsonData(self.url,updateDataStoreEndPoint,self.username,self.password,detectionObjects['alerts'])
			###self.createAlerts(detectionObjects['alerts'])
			return detectionObjects
		else:
			print("No outbreaks/epidemics for " + diseaseMeta['disease'])
			detectionObjects['epidemics'] = detectionObjectArray
			detectionObjects['alerts'] = alertDetectionObjectArray
			
			detectionObjects['numberOfEpidemics'] = 0
			return detectionObjects

	# Get first case date
	def getFirstCaseDate(self,caseLastDatePerOrgUnit,val):
		if(len(caseLastDatePerOrgUnit) > 0):
			dateFirstCase = self.formatIsoDate(caseLastDatePerOrgUnit[(len(caseLastDatePerOrgUnit) -1)][8])	
		else:
			dateFirstCase = val['startDate']			
		return dateFirstCase

	# Get last case date
	def getLastCaseDate(self,caseLastDatePerOrgUnit,val):
		if(len(caseLastDatePerOrgUnit) > 0):
			dateForLastCase = caseLastDatePerOrgUnit[0][8]			
		else:
			dateForLastCase = val['startDate']
		return dateForLastCase

	# Get last case date for existing case
	def getExistingLastCaseDate(self,caseLastDatePerOrgUnit,val):
		if(len(caseLastDatePerOrgUnit) > 0):
			deltaDays = self.getDateDifference(val['firstCaseDate'],self.formatIsoDate(caseLastDatePerOrgUnit[0][8]))
			if((deltaDays) < 0):
				dateForLastCase = self.formatIsoDate(caseLastDatePerOrgUnit[0][8])					
			else:
				dateForLastCase = val['lastCaseDate']			
		else:
			dateForLastCase = val['lastCaseDate']			
		return dateForLastCase

	def generateEvent(self,val,programMeta,dataElements,uids,events,dateFirstCase,dateForLastCase,caseLastDatePerOrgUnit,countEventUsed):

		data = {}
		trackMessage = []
		epiResult = {}
		epiDeResult = {}

		epiResult['dataValues'] = []
		epiResult['status'] = "COMPLETED"
		epiResult['storedBy'] = "admin"
		epiResult['orgUnit'] = val['reportingOrgUnit']
		epiResult['program'] = programMeta['id']
		epiResult['programStage'] = programMeta['programStages'][0]['id']
		epiResult['orgUnitName'] = val['orgUnitName']

		epiDeResult['status'] = "COMPLETED"
		epiDeResult['storedBy'] = "admin"
		epiDeResult['reportingOrgUnit'] = val['reportingOrgUnit']
		epiDeResult['reportingOrgUnitName'] = val['reportingOrgUnitName']
		epiDeResult['program'] = programMeta['id']
		epiDeResult['programStage'] = programMeta['programStages'][0]['id']
		epiDeResult['orgUnitName'] = val['orgUnitName']


		if(val['newEvent'] == 'true'):

			epiResult['event'] = uids[countEventUsed]
			epiResult['eventDate'] = val['startDate']
			epiResult['dataValues'] = []
			epiDeResult['event'] = uids[countEventUsed]
			
			epiDeResult['dataValues'] = []		
			epiDeResult['eventDate'] = val['startDate']				
			epiDeResult['lastCaseDate'] = self.formatIsoDate(dateForLastCase)
			epiDeResult['firstCaseDate'] = self.formatIsoDate(dateFirstCase)
			countEventUsed = countEventUsed + 1

			trackMessage.append(epiResult['event'])

		elif(val['newEvent'] != 'true'):
			epiResult['event'] = val['event']
			epiResult['eventDate'] = val['eventDate']

			epiDeResult['event'] = val['event']			
			epiDeResult['eventDate'] = val['eventDate']
			epiDeResult['firstCaseDate'] = val['firstCaseDate']
			epiDeResult['lastCaseDate'] = dateForLastCase
		
		dataValues = self.createDataValues(dataElements,val,epiResult,epiDeResult,dateFirstCase,dateForLastCase,trackMessage)
		epiResult['dataValues'].extend(dataValues['epi'])
		
		epiDeResult = dataValues['epiDe']
		trackMessage.extend(dataValues['msg'])

		data['epi'] = epiResult				
		data['epiDe'] = epiDeResult
		data['msg'] = trackMessage
		data['count'] = countEventUsed 
				
		return data

	def createDataValues(self,dataElements,val,epiResult,epiDeResult,dateFirstCase,dateForLastCase,trackMessage):

		data = {}
		sxDays = self.addDays(dateForLastCase,(2*val['incubationDays']))
		sxDelta = self.getDateDifference(self.today,sxDays)
		cvDays = self.addDays(dateForLastCase,((2*val['incubationDays'])-7))
		cvDelta = self.getDateDifference(self.today,cvDays)
		arrayDeResult = []
		for de in dataElements:			
			deResult = {}
			if(((de['dataElement']['name']).lower() == 'outbreak start date') or ((de['dataElement']['name']).lower() == 'outbreak_start_date')):
				deResult['dataElement'] = de['dataElement']['id']
				deResult['value'] = val['startDate']
				epiDeResult['startDate'] = val['startDate']

			elif(((de['dataElement']['name']).lower() == 'Date index case (first case) was suspected (based on date of symptom onset)') or ((de['dataElement']['name']).lower() == 'outbreak_date_index_case')):
				deResult['dataElement'] = de['dataElement']['id']
				deResult['value'] = dateFirstCase

			elif(((de['dataElement']['name']).lower() == 'outbreak end date') or ((de['dataElement']['name']).lower() == 'outbreak_end_date')):
				if((str(sxDelta) == '0') or ((str(sxDelta) > '0') and (str(sxDelta) != ''))):
					deResult['dataElement'] = de['dataElement']['id']
					deResult['value'] = self.today
					epiDeResult['endDate'] = self.today

				else:
					deResult['dataElement'] = de['dataElement']['id']
					deResult['value'] = ""
					epiDeResult['endDate'] = ""

			elif(((de['dataElement']['name']).lower() == 'outbreak disease') or ((de['dataElement']['name']).lower() == 'outbreak_disease')):
				deResult['dataElement'] = de['dataElement']['id']
				deResult['value'] = val['disease']
				epiDeResult['disease'] = val['disease']

			elif(((de['dataElement']['name']).lower() == 'origin of outbreak') or ((de['dataElement']['name']).lower() == 'origin_of_outbreak')):
				deResult['dataElement'] = de['dataElement']['id']
				deResult['value'] = val['orgUnit']
				epiDeResult['orgUnit'] = val['orgUnit']
				

			elif(((de['dataElement']['name']).lower() == 'outbreak status') or ((de['dataElement']['name']).lower() == 'outbreak_status')):
				deResult['dataElement'] = de['dataElement']['id']
				# Check if the outbreaks are confirmed/suspected

				if((str(sxDelta) == '0') or ((str(sxDelta) > '0') and (str(sxDelta) != ''))):
					deResult['value'] = "Closed"
					epiDeResult['status'] = "Closed"
					epiDeResult['active'] = "false"
					epiDeResult['closeDate'] = self.today
					epiDeResult['reminderSent'] = "false"
					epiDeResult['dateReminderSent'] = ""
					trackMessage.append(epiDeResult['event'])

				elif(cvDelta == 0):
					# Send reminder for closure
					deResult['value'] = "Closed Vigilance"
					epiDeResult['status'] = "Closed Vigilance"
					#epiDeResult['dateReminderSent'] = arrow.now().format("YYYY-MM-DD")
					epiDeResult['dateReminderSent'] = self.today
					epiDeResult['reminderSent'] = "true"
					epiDeResult['active'] = "true"
					trackMessage.append(epiDeResult['event'])
				else:
					deResult['value'] = "Confirmed"
					epiDeResult['status'] = "Confirmed"
					epiDeResult['active'] = "true"
					epiDeResult['reminderSent'] = "false"
					epiDeResult['dateReminderSent'] = ""
					epiDeResult['closeDate'] = ""

			elif(((de['dataElement']['name']).lower() == 'outbreak id') or ((de['dataElement']['name']).lower() == 'outbreak_id')):
				
				deResult['dataElement'] = de['dataElement']['id']
				deResult['value'] = val['epicode']
				epiDeResult['epicode'] = val['epicode']

			elif(((de['dataElement']['name']).lower() == 'outbreak cases confirmed') or ((de['dataElement']['name']).lower() == 'outbreak_cases_confirmed')):
				
				deResult['dataElement'] = de['dataElement']['id']
				deResult['value'] = val['confirmedValue']
				epiDeResult['confirmed'] = val['confirmedValue']

			elif(((de['dataElement']['name']).lower() == 'outbreak cases notified') or ((de['dataElement']['name']).lower() == 'outbreak_cases_notified')):
				deResult['dataElement'] = de['dataElement']['id']
				deResult['value'] = val['suspectedValue']
				epiDeResult['suspected'] = val['suspectedValue']

			elif(((de['dataElement']['name']).lower() == 'outbreak deaths') or ((de['dataElement']['name']).lower() == 'outbreak_deaths')):
				deResult['dataElement'] = de['dataElement']['id']
				deResult['value'] = val['deathValue']
				epiDeResult['deaths'] = val['deathValue']
			else:
				deResult['dataElement'] = de['dataElement']['id']
				deResult['value'] = ""
			arrayDeResult.append(deResult)
		# End of for loop
		
		data['epi'] = arrayDeResult
		data['epiDe'] = epiDeResult
		data['msg'] = trackMessage
		return data

	# Remove existing from data store epidemics
	def removeExistingEpidemics(self,epidemics,event):
		for epi in epidemics:
			if(len(epi.keys()) > 0 and len(event.keys()) > 0):
				if( epi['event'] == event['event']):
					epidemics.remove(epi)
		return epidemics

	# Remove existing from data store alerts
	def updateExistingAlerts(self,alerts,alertE):
		for alert in alerts:
			if(len(alert.keys()) > 0 and len(alertE.keys()) > 0):
				if( alert['orgUnit'] == alertE['orgUnit']):
					alerts.remove(alert)
				else:
					alerts.append(alertE)
		return alerts

	# create data store alerts
	def createAlerts(self,alerts,alertE):
		for alert in alerts:
			if len(alert.keys()) > 0 :
				for al in alertE:
					if len(al.keys()) > 0:
						if( alert['orgUnit'] == alertE['orgUnit']):
							alerts.remove(alert)
		return alerts
	# create event data

	def createEvents(self,epidemics,values,programMeta,dataElements,uids,events):
		print("Start creating Event #")
		epis = {}
		# ---events = [];
		epiDataValues = []
		epiDeDataValues = []
		messageConversations = []
		countEventUsed = 0
		#print("values ",values)

		for val in values:
			message = {}
			trackMessage = []
			epiResult = {}

			

			epiDeResult = {}
			
			caseLastDatePerOrgUnit =  self.getEventsByOrgUnit(events,val['orgUnit'])			
			if(val['newEvent'] == "true"):
				dateForLastCase = self.getLastCaseDate(caseLastDatePerOrgUnit,val)		
				dateFirstCase = self.getFirstCaseDate(caseLastDatePerOrgUnit,val)
				dateFirstCase = self.formatIsoDate(dateFirstCase)
				# Check for any closed outbreak pulled back
				closedEpis = self.findClosedEpidemics(epidemics,val['disease'],val['orgUnit'])
				if ((len(closedEpis) > 0) and (closedEpis != 'undefined')):
					for closedEvent in closedEpis:
						if self.getDateDifference(dateFirstCase,closedEvent['endDate']) > 0:
							# check now
							ev = self.generateEvent(val,programMeta,dataElements,uids,events,dateFirstCase,dateForLastCase,caseLastDatePerOrgUnit,countEventUsed)
							epiResult = ev['epi']
							epiDeResult = ev['epiDe']
							countEventUsed = ev['count']
							trackMessage.extend(ev['msg'])
						#else:
							# do nothing
							#print("Nothing to be done")
				else:
					# Completely new epidemics		
					evNew = self.generateEvent(val,programMeta,dataElements,uids,events,dateFirstCase,dateForLastCase,caseLastDatePerOrgUnit,countEventUsed)
					epiResult = evNew['epi']
					epiDeResult = evNew['epiDe']
					countEventUsed = evNew['count']
					trackMessage.extend(evNew['msg'])

			elif(val['newEvent']!= "true"):
				dateFirstCase = val['firstCaseDate']
				dateForLastCase = self.getExistingLastCaseDate(caseLastDatePerOrgUnit,val)
				evExist = self.generateEvent(val,programMeta,dataElements,uids,events,dateFirstCase,dateForLastCase,caseLastDatePerOrgUnit,countEventUsed)


				epiResult = evExist['epi']
				epidemics = self.removeExistingEpidemics(epidemics,evExist['epiDe'])
				epiDeResult = evExist['epiDe']
				countEventUsed = evExist['count']
				trackMessage.extend(evExist['msg'])

			# epiResult['orgUnit = reportingOrgUnit.reporting
			# This should be the Subdistrict of reporting level orgUnit

			if len(epiResult.keys()) > 0:
				epiDataValues.append(epiResult)
				epis['events'] = { "events": epiDataValues }
				# Post events
				postEventEndPoint = "events"
				postEventParams = { "strategy": "CREATE_AND_UPDATE" }
				postValues = self.postJsonDataWithParams(self.url,postEventEndPoint,self.username,self.password,epis['events'],postEventParams)
				print("Post event resultx: ",epis['events'])
				print("Post event result: ",postValues)
			if len(epiDeResult.keys())> 0:
				epiDeDataValues.append(epiDeResult)
				epidemics.append(epiDeResult)
			if(len(trackMessage) > 0 ):
				
				print('epiDeResult',epiDeResult)
				messageConversations.append(self.formatMessage(self.checkKey('notifiableUserGroups',val),epiDeResult))

						
			epis['messages'] = messageConversations

			# Re-enable to send emails and sms
			self.sendSmsAndEmailMessage(epis['messages'])
		epis['epidemics'] = epidemics
		return epis

	def iterateDiseases(self,diseasesMeta,epidemics,dataElements,epiMeta,alerts):
		newUpdatedEpis = []
		existingEpidemics = epidemics;

		for diseaseMeta in diseasesMeta:
			ouLevel = 'LEVEL-' + str(diseaseMeta['detectionLevel'])
			detectionXLevel = diseaseMeta['detectionLevel']
			ouFields = 'organisationUnits'
			ouParams = {"fields": "id,ancestors[id]","paging":"false","filter":"level:eq:"+ str(detectionXLevel)}
			epiReportingOrgUnit	= self.getHttpData(self.url,ouFields,self.username,self.password,params=ouParams)
			piSeparator = ";"
			piIndicatorsArray = self.getArrayFromObject(diseaseMeta['programIndicators'])
			piIndicators = piSeparator.join(piIndicatorsArray)
			piFields = 'analytics'

			if diseaseMeta['epiAlgorithm'] == "CASE_BASED":
				print("Detecting for case based diseases")
				print ("Start outbreak detection for %s" %diseaseMeta['disease'])
				#LAST_7_DAYS
				# check this period -- THIS_WEEK shd return aggregated
				piPeriod = self.period		
				
				lastCaseDateParams= { "dimension": ['pe:' + self.period,'ou:' + ouLevel,self.dateOfOnsetUid,self.conditionOrDiseaseUid + ":IN:" + diseaseMeta["code"]],"stage": self.labResultStage,"displayProperty":"NAME","outputType":"EVENT","pageSize":"1000","page":"1"}
				
				eventsFields = 'analytics/events/query/' + self.programUid
				events = self.getHttpData(self.url,eventsFields,self.username,self.password,params=lastCaseDateParams)				
				
				piParams = {"dimension": ["dx:"+ piIndicators,"ou:" + ouLevel],"filter": "pe:" + piPeriod,"displayProperty":"NAME","tableLayout":"true","columns":"dx","rows":"ou","skipMeta":"false","hideEmptyRows":"true","skipRounding":"false","showHierarchy":"true"}
				programIndicators = self.getHttpData(self.url,piFields,self.username,self.password,params=piParams)
				
				if(( programIndicators != 'HTTP_ERROR') and (epiReportingOrgUnit != 'HTTP_ERROR')):
					piData = programIndicators
					ou = epiReportingOrgUnit['organisationUnits']
					detectedEpidemics = self.detectBasedOnProgramIndicators(piData,diseaseMeta,epidemics,ou)
					# Creating epidemics alerts
					alerts = self.createAlerts(alerts,detectedEpidemics['alerts'])
					# Get Uids for identifying epidemics
					print("Number of New Epidemics ", detectedEpidemics['numberOfEpidemics'])
					if( detectedEpidemics['numberOfEpidemics'] > 0):
						epiCodesFields = "system/id"
						epiCodesParams = { "limit" : detectedEpidemics['numberOfEpidemics'] }
						
						epiCodes = self.getHttpData(self.url,epiCodesFields,self.username,self.password,params=epiCodesParams)
						if(epiCodes != 'HTTP_ERROR'):
							epiCodesUids = epiCodes['codes']
							print("Detecting Outbreaks .... ")
							event = self.createEvents(epidemics,detectedEpidemics['epidemics'],epiMeta,dataElements,epiCodesUids,events);
							print ("Finished creating Outbreaks for %s" %diseaseMeta['disease'])
						else:
							print("Failed to generated DHIS2 UID codes")
					else:
						print("Exiting no new outbreaks detected")
				else:
					print("Failed to load program indicators")
					
			elif diseaseMeta['epiAlgorithm'] == "SEASONAL":
				print("Detecting for seasonal")
				print ("Start outbreak detection for %s" %diseaseMeta['disease'])
				# periods are aggregate generated
				aggPeriod = self.createAggThresholdPeriod(3,5,'SEASONAL')
				aggPeriods = piSeparator.join(aggPeriod)			
				print("Periods ", aggPeriods)		
				aggParams = {"dimension": ["dx:"+ piIndicators,"ou:" + ouLevel,"pe:" + aggPeriods],"displayProperty":"NAME","tableLayout":"true","columns":"dx;pe","rows":"ou","skipMeta":"false","hideEmptyRows":"true","skipRounding":"false","showHierarchy":"true"}

				aggIndicators = self.getHttpData(self.url,piFields,self.username,self.password,params=aggParams)
				
				if(( aggIndicators != 'HTTP_ERROR') and (epiReportingOrgUnit != 'HTTP_ERROR')):
					aggData = aggIndicators
					aggOrgUnit = epiReportingOrgUnit['organisationUnits']
					detectedAggEpidemics = self.detectOnAggregateIndicators(aggData,diseaseMeta,epidemics,aggOrgUnit,aggPeriod)
					# Creating epidemics alerts
					#print("Dere", detectedAggEpidemics)
					
				else:
					print("Failed to load program indicators")
			else:
				print("Detecting for non seasonal")

		print("Updating epidemics in the datastore online")
		epiUpdateDataStoreEndPoint  = 'dataStore/' + self.dataStore + '/epidemics'
		self.updateJsonData(self.url,epiUpdateDataStoreEndPoint,self.username,self.password,epidemics)

		print("Updating alerts in the datastore online")
		epiUpdateDataStoreEndPointAlert  = 'dataStore/' + self.dataStore + '/alerts'
		self.updateJsonData(self.url,epiUpdateDataStoreEndPointAlert,self.username,self.password,alerts)
			
		return "Done processing"

		# Start epidemic detection
	def startEpidemics(self):
		# appIdsr = IdsrAppServer()
		print ("Started detection for outbreaks/epidemics")
		#	get Outbreak program meta data

		epiParams = {"paging":"false","fields": "id,name,programStages[id,programStageDataElements[dataElement[id,name,formName]]]"}

		epiMeta = self.getHttpDataWithId(self.url,'programs',self.outbreakProgram,self.username,self.password,epiParams)

		# get self.epidemics/outbreak program's data elements
		if(epiMeta != 'HTTP_ERROR'):
			dataElements = epiMeta['programStages'][0]['programStageDataElements']

			# Get Disease Metadata
			diseaseFields = 'dataStore/' + self.dataStore + '/diseases'

			diseasesMeta = self.getHttpData(self.url,diseaseFields,self.username,self.password,{})

			# Get Epidemics
			if(diseasesMeta != 'HTTP_ERROR'):
				epidemicsFields = 'dataStore/' + self.dataStore + '/epidemics'
				epidemicsData = self.getHttpData(self.url,epidemicsFields,self.username,self.password,{})
				
				alertsFields = 'dataStore/' + self.dataStore + '/alerts'
				alertsData = self.getHttpData(self.url,alertsFields,self.username,self.password,{})

				if(epidemicsData != 'HTTP_ERROR'):
					epidemicsProcessed = self.iterateDiseases(diseasesMeta['diseases'],epidemicsData,dataElements,epiMeta,alertsData)
					print(epidemicsProcessed)
				else:
					print("Failed to load epidemics datastores")

					#loggedin = self.getHttpData(self.url,'me',self.username,self.password,{})
			else:
				print("Failed to get disease meta data")
		else:
			print("Failed to load meta data for program")

# Start the idsr processing
if __name__ == "__main__":
	idsrAppSerlvet = IdsrAppServer()
	idsrAppSerlvet.startEpidemics()
#main()
