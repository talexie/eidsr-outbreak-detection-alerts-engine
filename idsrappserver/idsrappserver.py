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
		self.period = "LAST_12_MONTHS"
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
		self.patientStatusStage = 'M2sHSxP9W7G'
		self.caseMonitoringStage = ''

		# TE Attributes
		self.dateOfOnsetUid = 'adJ527HOTea'
		self.conditionOrDiseaseUid = 'uOTHyxNv2W4'
		self.patientStatusOutcome = 'xGUYkoLv0oN'
		self.regPatientStatusOutcome = 'i6A3z9QQEBt'
		self.caseClassification = 'bt06ynPCyFd'
		self.testResult='kSvvTuSoUhy'
		self.testResultClassification='Rl2G8xoVczk'

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

	# Get orgUnit value
	# @param type = { id,name,code} 
	def getOrgUnitValue(self,detectionOu,ous,level,type):
		ou = []
		if((ous !='undefined') and len(ous) > 0):
			for oux in ous:
				if(oux['id'] == detectionOu):
					return oux['ancestors'][level][type]
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
	def createColumns(self,headers,type):
		cols = []
		for header in headers:
			if(type == 'EVENT'):
				if header['name'] == self.dateOfOnsetUid:
					cols.append('onSetDate')
				elif header['name'] == self.conditionOrDiseaseUid:
					cols.append('disease')
				elif header['name'] == self.regPatientStatusOutcome:
					cols.append('immediateOutcome')
				elif header['name'] == self.patientStatusOutcome:
					cols.append('statusOutcome')
				elif header['name'] == self.testResult:
					cols.append('testResult')
				elif header['name'] == self.testResultClassification:
					cols.append('testResultClassification')
				elif header['name'] == self.caseClassification:
					cols.append('caseClassification')
				else:	
					cols.append(header['name'])
			else:
				cols.append(header['column'])
		
		return cols

	# Group last 7 days cases by orgunit
	def groupByOrgUnit(self,eventDataFrame):
		groupedByOrgUnit = eventDataFrame.groupby('organisationunitid').sum()
		return groupedByOrgUnit
	
	# Check if aggregate threshold is meant
	def checkAggThreshold(self,current,meanValue):
		if current >= meanValue:
			return 'true'
		else:
			return 'false'

	# create Panda Data Frame from event data
	def createDataFrame(self,events,type):
		cols = self.createColumns(events['headers'],type)
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
				df = self.createDataFrame(aggData,'AGGREGATE')				
				
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
				
				df['mean_current_deaths'] = df.iloc[:,(detectionLevel+3+m+(m*n)):(detectionLevel+3+(2*m)+(m*n))].mean(axis=1)
				df['mean_mn_deaths'] = df.iloc[:,(detectionLevel+3+(2*m)+(m*n)):dfColLength-1].mean(axis=1)
				df['stddev_mn_deaths'] = df.iloc[:,(detectionLevel+3+(2*m)+(m*n)):dfColLength-1].std(axis=1)
				df['mean20std_mn_deaths'] = (df.mean_mn_deaths + (2*df.stddev_mn_deaths))
				df['mean15std_mn_deaths'] = (df.mean_mn_deaths + (1.5*df.stddev_mn_deaths))	
				
				df['reportingOrgUnitName'] = df.iloc[:,reportingLevel-1]
				df['reportingOrgUnit'] = df.iloc[:,detectionLevel].apply(self.getOrgUnitValue,args=(ou,(reportingLevel-1),'id'))
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

	# Replace all values with standard text
	def replaceText(self,df):

		df.replace(to_replace='Confirmed case',value='confirmed',regex=True,inplace=True)
		df.replace(to_replace='Suspected case',value='suspected',regex=True,inplace=True)
		df.replace(to_replace='Confirmed',value='confirmed',regex=True,inplace=True)
		df.replace(to_replace='Suspected',value='suspected',regex=True,inplace=True)
		df.replace(to_replace='confirmed case',value='confirmed',regex=True,inplace=True)
		df.replace(to_replace='suspected case',value='suspected',regex=True,inplace=True)
		df.replace(to_replace='died',value='dead',regex=True,inplace=True)
		df.replace(to_replace='Died case',value='dead',regex=True,inplace=True)
		return df

	# Get Confirmed cases
	def getConfirmed(self,row):
		if row['confirmed_x'] <= row['confirmed_y']:
			return row['confirmed_y']
		else:
			return row['confirmed_x']

	# Get suspected cases
	def getSuspected(self,row,columns):
		if set(['suspected','confirmedValue']).issubset(columns):
			if row['suspected'] <= row['confirmedValue']:
				return row['confirmedValue']
			else:
				return row['suspected']
		elif set(['suspected_x','suspected_y','confirmedValue']).issubset(columns):
			if row['suspected_x'] <= row['confirmedValue']:
				return row['confirmedValue']
			elif row['suspected_x'] <= row['suspected_y']:
				return row['suspected_y']
			else:
				return row['suspected_x']
		else:
			return "Column missing"

	# Get Deaths
	def getDeaths(self,row,columns):
		if set(['dead_x','dead_y']).issubset(columns):
			if row['dead_x'] <= row['dead_y']:
				return row['dead_y']
			else:
				return row['dead_x']
		elif set(['dead','deathValue']).issubset(columns):
			if row['dead'] <= row['deathValue']:
				return row['deathValue']
			else:
				return row['dead']
		else:
			return '0'
	# Check if epedimic is active or ended
	def getActive(self,row):
		if pd.to_datetime(self.today) < pd.to_datetime(row['endDate']):
			return 'active'
		elif pd.to_datetime(row['endDate']) == (pd.to_datetime(self.today)):
			return 'true'
		else:
			return 'false'
	# Check if reminder is to be sent
	def getReminder(self,row):
		if row['reminderDate'] == pd.to_datetime(self.today):
			return 'true'
		else:
			return 'false'
	# replace data of onset with event dates
	def replaceDatesWithEventData(self,row):
		
		if row['onSetDate'] == '':
			return pd.to_datetime(row['eventdate'])
		else:
			return pd.to_datetime(row['onSetDate'])

	# Transform df for DHIS2 JSON events format
	# @param dataFrame df
	# @return df array
	def transformDf(self,df):
		df = json.loads(df)
		events = [];
		if len(df) > 0:
			for row in df:
				row['period'] = row['dateOfOnSetWeek']
				row['suspectedValue'] = round(row['suspectedValue'])
				row['confirmedValue'] = round(row['confirmedValue'])
				#row['deathValue'] = round(row['deathValue'])				
				#### Check epidemic closure
				if row['epidemic'] == "true" and row['active'] == "true":
				 	row['status']='Closed'
				 	row['active']='false'
				 	row['closeDate']=self.today
				 	row['reminderSent']='false'
				 	row['dateReminderSent']=''
				 	# Send closure message

				elif row['reminder'] == "true" and row['alert'] == "true":
				 	row['status']= 'Closed Vigilance'
				 	row['active']='true'
				 	row['reminderSent']='true'
				 	row['dateReminderSent']=self.today
					# Send Reminder for closure
				else:
					row['status']='Confirmed'
					row['active']='true'
					row['closeDate']=''
					row['reminderSent']='false'
					row['dateReminderSent']=''
		else:
			pass
		
		return df
	# Get key id from dataelements
	def getDataElement(self,dataElements,key):
		for de in dataElements:
			if de['name'] == key:
				return de['id']
			else:
				pass

	# Transform updated to DHIS2 JSON events format
	# @param dataFrame df
	# @return dhis2Events object { 'events', 'datastore Events'}
	def createDHIS2Events(self,updatedEpidemics,config):
		dataElements = config['reportingProgram']['programStage']['dataElements']		
		savedEvents = {'events':[]}
		events = [];
		if len(updatedEpidemics) > 0:
			for row in updatedEpidemics:
				event = {'dataValues':[]}
				event['orgUnit'] = row['reportingOrgUnit']
				event['eventDate'] = row['firstCaseDate']
				event['status'] = 'COMPLETED'
				event['program'] = config['reportingProgram']['id']
				event['programStage'] = config['reportingProgram']['programStage']['id']
				event['storedBy'] = 'idsr'
				
				for key in [*row]:
					if key == 'suspectedValue':
				 		event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'suspected'),'value':round(row['suspectedValue'])})
					elif key == 'confirmedValue':
				 		event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'confirmed'),'value':round(row['confirmedValue'])})
					elif key == 'firstCaseDate':
					 	event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'firstCaseDate'),'value':row['firstCaseDate']})
					elif key == 'orgUnit':
					 	event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'origin'),'value':row['orgUnit']})
					 	event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'outbreakId'),'value':row['orgUnit']})
					elif key == 'disease':
					 	event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'disease'),'value':row['disease']})
					elif key == 'endDate':
					 	event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'endDate'),'value':row['endDate']})					 
					else:
					 	pass
				#### Check epidemic closure
				if row['closeDate'] == self.today and row['status']=='Closed':
				 	event['dataValues'].append({'dataElement': key,'value':'Closed'})
				 	# Send closure message

				elif row['dateReminderSent']==self.today and row['status']== 'Closed Vigilance':
				 	event['dataValues'].append({'dataElement': key,'value':'Closed Vigilance'})
					# Send Reminder for closure
				else:
					event['dataValues'].append({'dataElement': key,'value':'Confirmed'})					
				# Add event to list
				events.append(event)
		else:
			pass
		savedEvents['events'] = events
		return savedEvents

	# detect self.epidemics
	# Confirmed, Deaths,Suspected
	def detectBasedOnProgramIndicators(self,caseEvents,diseaseMeta,orgUnits):
		dhis2Events = []
		detectionLevel = int(diseaseMeta['detectionLevel'])
		reportingLevel = int(diseaseMeta['reportingLevel'])
		if(caseEvents != 'HTTP_ERROR'):
			if((caseEvents != 'undefined') and (caseEvents['rows'] != 'undefined') and len(caseEvents['rows']) >0):
				df = self.createDataFrame(caseEvents,'EVENT')
				caseEventsColumnsById = df.columns 
				dfColLength = len(df.columns)				

				# If date of onset is null, use eventdate
				df['dateOfOnSet'] = np.where(df['onSetDate']== '',pd.to_datetime(df['eventdate']).dt.strftime('%Y-%m-%d'),df['onSetDate'])
				# Replace all text with standard text
				
				df = self.replaceText(df)
				df.to_csv('df.csv', sep=',', encoding='utf-8')
				# Transpose and Aggregate values
				
				dfCaseClassification = df.groupby(['ouname','ou','disease','dateOfOnSet'])['caseClassification'].value_counts().unstack().fillna(0).reset_index()
				
				dfCaseImmediateOutcome = df.groupby(['ouname','ou','disease','dateOfOnSet'])['immediateOutcome'].value_counts().unstack().fillna(0).reset_index()
				
				dfTestResult = df.groupby(['ouname','ou','disease','dateOfOnSet'])['testResult'].value_counts().unstack().fillna(0).reset_index()
				
				dfTestResultClassification = df.groupby(['ouname','ou','disease','dateOfOnSet'])['testResultClassification'].value_counts().unstack().fillna(0).reset_index()
				
				dfStatusOutcome = df.groupby(['ouname','ou','disease','dateOfOnSet'])['statusOutcome'].value_counts().unstack().fillna(0).reset_index()

				combinedDf = pd.merge(dfCaseClassification,dfCaseImmediateOutcome,on=['ou','ouname','disease','dateOfOnSet'],how='left').merge(dfTestResultClassification,on=['ou','ouname','disease','dateOfOnSet'],how='left').merge(dfTestResult,on=['ou','ouname','disease','dateOfOnSet'],how='left').merge(dfStatusOutcome,on=['ou','ouname','disease','dateOfOnSet'],how='left')
				combinedDf.sort_values(['ouname','disease','dateOfOnSet'],ascending=[True,True,True])
				combinedDf['dateOfOnSetWeek'] = pd.to_datetime(combinedDf['dateOfOnSet']).dt.strftime('%YW%V')
				combinedDf['confirmedValue'] = combinedDf.apply(self.getConfirmed,axis=1)			
				combinedDf['suspectedValue'] = combinedDf.apply(self.getSuspected,args=([combinedDf.columns]),axis=1)

				#combinedDf['deathValue'] = combinedDf.apply(self.getDeaths,args=([combinedDf.columns]),axis=1)
				
				combinedDf.to_csv('combined.csv', sep=',', encoding='utf-8')
				dfConfirmed = combinedDf.groupby(['ouname','ou','disease','dateOfOnSetWeek'])['confirmedValue'].agg(['sum']).reset_index()
				
				dfConfirmed.rename(columns={'sum':'confirmedValue' },inplace=True)
				dfSuspected = combinedDf.groupby(['ouname','ou','disease','dateOfOnSetWeek'])['suspectedValue'].agg(['sum']).reset_index()
				dfSuspected.rename(columns={'sum':'suspectedValue' },inplace=True)
				dfFirstAndLastCaseDate = df.groupby(['ouname','ou','disease'])['dateOfOnSet'].agg(['min','max']).reset_index()
				dfFirstAndLastCaseDate.rename(columns={'min':'firstCaseDate','max':'lastCaseDate'},inplace=True)

				aggDf = pd.merge(dfConfirmed,dfSuspected,on=['ouname','ou','disease','dateOfOnSetWeek'],how='left').merge(dfFirstAndLastCaseDate,on=['ouname','ou','disease'],how='left')

				aggDf['reportingOrgUnitName'] = aggDf.loc[:,'ou'].apply(self.getOrgUnitValue,args=(orgUnits,(reportingLevel-1),'name'))
				aggDf['reportingOrgUnit'] = aggDf.loc[:,'ou'].apply(self.getOrgUnitValue,args=(orgUnits,(reportingLevel-1),'id'))
				aggDf['alertThreshold'] = int(diseaseMeta['alertThreshold'])
				aggDf['epiThreshold'] = int(diseaseMeta['epiThreshold'])
				aggDf['incubationDays'] = int(diseaseMeta['incubationDays'])
				aggDf['endDate'] = pd.to_datetime(pd.to_datetime(aggDf['lastCaseDate']) + pd.to_timedelta(pd.np.ceil(2*aggDf['incubationDays']), unit="D")).dt.strftime('%Y-%m-%d')
				aggDf['reminderDate'] = pd.to_datetime(pd.to_datetime(aggDf['lastCaseDate']) + pd.to_timedelta(pd.np.ceil(aggDf['incubationDays']-7), unit="D")).dt.strftime('%Y-%m-%d')
				aggDf['active'] =  aggDf.apply(self.getActive,axis=1)
				aggDf['reminder'] =  aggDf.apply(self.getReminder,axis=1)
				aggDf.rename(columns={'ouname':'orgUnitName','ou':'orgUnit'},inplace=True);	
				
								
				#df['confirmed_suspected_cases'] = df[['confirmedValue','suspectedValue']].sum(axis=1)
				
				aggDf['epidemic'] = np.where(aggDf['confirmedValue'] >= aggDf['epiThreshold'],'true','false')
				alertQuery = (aggDf['confirmedValue'] < aggDf['epiThreshold']) & (aggDf['suspectedValue'].astype(np.int64) >= aggDf['alertThreshold'].astype(np.int64))
				aggDf['alert'] = np.where(alertQuery,'true','false')
				# Filter out those greater or equal to threshold
				aggDf.to_csv('combinedagg1.csv', sep=',', encoding='utf-8')
				aggDf = aggDf[aggDf['epidemic'] == 'true']
				df_alert = aggDf[aggDf['alert'] == 'true']
				aggDf.to_csv('combinedagg.csv', sep=',', encoding='utf-8')
				# lab confirmed = true
				# type of emergency = outbreak or other			

				dhis2Events = self.transformDf(aggDf.to_json(orient='records'))
				
			else:
				# No data for cases found
				pass
			return dhis2Events
		else:
			print("No outbreaks/epidemics for " + diseaseMeta['disease'])
			return dhis2Events

	# Remove existing  and update with new from data store epidemics
	def getUpdatedEpidemics(self,epidemics,events,uids):
		updatedEpidemics = []
		newEpidemics = []
		allEpidemics = []
		for epi in epidemics:
			for event in events:				
				if(len(epi.keys()) > 0 and len(event.keys()) > 0):
					if (epi['orgUnit'] == event['orgUnit']) and (epi['disease'] == event['disease']):
						# Existing and updates
						if(epi['active'] == 'true') and (event['active'] == 'true'):
							epi['confirmed'] = event['confirmedValue']
							epi['suspected'] = event['suspectedValue']
							#epi['deaths'] = event['deathValue']
							epi['status'] = event['status']
							epi['dateReminderSent'] = event['dateReminderSent']
							epi['reminderSent'] = event['reminderSent']
							epi['lastCaseDate'] = event['lastCaseDate']
							epi['updated'] = 'true'
						elif(epi['active'] == 'false') and (event['active'] == 'true'):
							event['event'] = uids[0]
							del uids[0]
							epidemics.append(event)
						else:
							## Remove existing with no update from payload
							epidemics.remove(epi)
					else:
						pass
				else:
					pass
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

	def iterateDiseases(self,diseasesMeta,epidemics,alerts):
		newUpdatedEpis = []
		existingEpidemics = epidemics
		programConfig = diseasesMeta['config']

		for diseaseMeta in diseasesMeta['diseases']:
			ouLevel = 'LEVEL-' + str(diseaseMeta['detectionLevel'])
			detectionXLevel = diseaseMeta['detectionLevel']
			ouFields = 'organisationUnits'
			ouParams = {"fields": "id,code,ancestors[id,code,name]","paging":"false","filter":"level:eq:"+ str(detectionXLevel)}
			epiReportingOrgUnit	= self.getHttpData(self.url,ouFields,self.username,self.password,params=ouParams)
			piSeparator =';'
			
			if diseaseMeta['epiAlgorithm'] == "CASE_BASED":
				print("Detecting for case based diseases")
				print ("Start outbreak detection for %s" %diseaseMeta['disease'])
				#LAST_7_DAYS	
				
				eventsFields = 'analytics/events/query/' + self.programUid					
				
				### Get Cases or Disease Events
				#
				caseEventParams = { "dimension": ['pe:' + self.period,'ou:' + ouLevel,self.dateOfOnsetUid,self.conditionOrDiseaseUid + ":IN:" + diseaseMeta["code"],self.patientStatusOutcome,self.regPatientStatusOutcome,self.caseClassification,self.testResult,self.testResultClassification],"displayProperty":"NAME"}
				caseEvents = self.getHttpData(self.url,eventsFields,self.username,self.password,params=caseEventParams)

				if(( caseEvents != 'HTTP_ERROR') and (epiReportingOrgUnit != 'HTTP_ERROR')):
					orgUnits = epiReportingOrgUnit['organisationUnits']
					detectedEpidemics = self.detectBasedOnProgramIndicators(caseEvents,diseaseMeta,orgUnits)
					# Creating epidemics alerts
					####alerts = self.createAlerts(alerts,detectedEpidemics['alerts'])
					# Get Uids for identifying epidemics
					print("Number of New Epidemics ", len(detectedEpidemics))
					if( len(detectedEpidemics) > 0):
						epiCodesFields = "system/id"
						epiCodesParams = { "limit" : len(detectedEpidemics) }
						
						epiCodes = self.getHttpData(self.url,epiCodesFields,self.username,self.password,params=epiCodesParams)
						if(epiCodes != 'HTTP_ERROR'):
							epiCodesUids = epiCodes['codes']
							print("Detecting Outbreaks .... ")
							updatedEpidemics = self.getUpdatedEpidemics(epidemics,detectedEpidemics,epiCodesUids)
							event = self.createDHIS2Events(updatedEpidemics,programConfig);
							print("Event",event)
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
		print ("Started detection for outbreaks/epidemics")
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
				epidemicsProcessed = self.iterateDiseases(diseasesMeta,epidemicsData,alertsData)
				print(epidemicsProcessed)
			else:
				print("Failed to load epidemics datastores")

				#loggedin = self.getHttpData(self.url,'me',self.username,self.password,{})
		else:
			print("Failed to get disease meta data")

# Start the idsr processing
if __name__ == "__main__":
	idsrAppSerlvet = IdsrAppServer()
	idsrAppSerlvet.startEpidemics()
#main()
