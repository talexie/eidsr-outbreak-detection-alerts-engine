#!/usr/bin/env python

import requests
import os
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
		self.period = "LAST_7_DAYS"
		self.ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
		self.ID_LENGTH = 11
		self.today = moment.now().format('YYYY-MM-DD')
		print("Epidemic/Outbreak Detection script started on %s" %self.today)

		self.path = os.path.abspath(os.path.dirname(__file__))
		newPath = self.path.split('/')
		newPath.pop(-1)
		newPath.pop(-1)

		self.fileDirectory = '/'.join(newPath)
		self.url = ""
		self.username = ''
		self.password = ''
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

	# Get Authentication details
	def getAuth(self):
		with open(os.path.join(self.fileDirectory,'.idsr.json'),'r') as jsonfile:
			auth = json.load(jsonfile)
			return auth
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
	def createAggThresholdPeriod(self,m,n,type):
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
		message['subject'] = outbreak['disease'] + " outbreak in " + outbreak['orgUnitName']
		message['text'] = "Dear all, Epidemic threshold for " + outbreak['disease'] + "  is reached at " + outbreak['orgUnitName'] + " of " + outbreak['reportingOrgUnitName']  + " on " + outbreak['eventDate']

		message['userGroups'] = usergroups
		message['organisationUnits'].append({"id": outbreak['orgUnit']})
		message['organisationUnits'].append({"id": outbreak['reportingOrgUnit']})

		return (message)

	def formatAlertMessage(self,usergroups,outbreak):
		message = {}
		message['organisationUnits'] = []

		message['subject'] = outbreak['disease'] + " alert"
		message['text'] = "Dear all, Alert threshold for " + outbreak['disease'] + "  is reached at " + outbreak['orgUnitName'] + " of " + outbreak['reportingOrgUnitName'] + " on " + self.today
		message['userGroups'] = usergroups;
		message['organisationUnits'].append({"id": outbreak['orgUnit']})
		message['organisationUnits'].append({"id": outbreak['reportingOrgUnit']})

		return (message)
	def formatReminderMessage(self,usergroups,outbreak):
		message = {}
		message['organisationUnits'] = []

		message['subject'] = outbreak['disease'] + " reminder"
		message['text'] = "Dear all," + outbreak['disease'] + " outbreak at " + outbreak['orgUnitName'] + " of " + outbreak['reportingOrgUnitName'] + " is closing in 7 days"
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

	def createAlerts(self,userGroup,values,type):
        
		messageConversations = []
		messages = { "messageConversations": []}
		if type == 'EPI':
			for val in values:
				messageConversations.append(self.formatMessage(userGroup,val))
			messages['messageConversations'] = messageConversations
		elif type == 'ALERT':
			for val in values:
				messageConversations.append(self.formatAlertMessage(userGroup,val))
			messages['messageConversations'] = messageConversations
		elif type == 'REMINDER':
			for val in values:
				messageConversations.append(self.formatReminderMessage(userGroup,val))
			messages['messageConversations'] = messageConversations
		else:
			pass

		for message in messageConversations:
			msgSent = self.sendSmsAndEmailMessage(message)
			print("Message Sent status",msgSent)
		return messages

	# create columns from event data
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
			elif (type == 'DATES'):
				cols.append(header['name'])
			else:
				cols.append(header['column'])
		
		return cols

	# create Panda Data Frame from event data
	def createDataFrame(self,events,type):
		cols = self.createColumns(events['headers'],type)
		dataFrame = pd.DataFrame.from_records(events['rows'],columns=cols)
		return dataFrame
    
	# Detect using aggregated indicators
	# Confirmed, Deaths,Suspected
	def detectOnAggregateIndicators(self,aggData,diseaseMeta,epidemics,ou,periods,mPeriods,nPeriods):
		dhis2Events = []
		detectionLevel = int(diseaseMeta['detectionLevel'])
		reportingLevel = int(diseaseMeta['reportingLevel'])
		m=mPeriods
		n=nPeriods
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
				df['firstCaseDate'] = self.today
				# Mid period for seasonal = mean of range(1,(m+1)) where m = number of periods
				midPeriod = int(np.median(range(1,(m+1))))
				df['period']= periods[midPeriod]
				df['endDate'] = ""	
				df['disease'] = diseaseMeta['disease']
				df['incubationDays'] = diseaseMeta['incubationDays']
				df['active'] = "true"	
				df['reminder'] = "false"	

				dhis2Events = {'alerts': self.transformDf(df.to_json(orient='records'),'ALERT') ,'events': self.transformDf(df.to_json(orient='records'),'EPI')}
				
			else:
				# No data for cases found
				dhis2Events = {'alerts':[],'events':[]}
				pass
			return dhis2Events
		else:
			print("No outbreaks/epidemics for " + diseaseMeta['disease'])
			return dhis2Events
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
	def getConfirmed(self,row,columns):
		if set(['confirmed','confirmedValue']).issubset(columns):
			return row['confirmedValue']
		elif set(['confirmed_x','confirmed_y','confirmedValue']).issubset(columns):
			if int(row['confirmed_x']) <= int(row['confirmed_y']):
				return row['confirmed_y']
			else:
				return row['confirmed_x']
		else:
			return 0

	# Get suspected cases
	def getSuspected(self,row,columns):
		if set(['suspected','confirmedValue']).issubset(columns):
			if int(row['suspected']) <= int(row['confirmedValue']):
				return row['confirmedValue']
			else:
				return row['suspected']
		elif set(['suspected_x','suspected_y','confirmedValue']).issubset(columns):
			if int(row['suspected_x']) <= int(row['confirmedValue']):
				return row['confirmedValue']
			elif int(row['suspected_x']) <= int(row['suspected_y']):
				return row['suspected_y']
			else:
				return row['suspected_x']
		else:
			return 0

	# Get Deaths
	def getDeaths(self,row,columns):
		if set(['dead_x','dead_y']).issubset(columns):
			if int(row['dead_x']) <= int(row['dead_y']):
				return row['dead_y']
			else:
				return row['dead_x']
		elif set(['dead','deathValue']).issubset(columns):
			if int(row['dead']) <= int(row['deathValue']):
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
	# get onset date
	def getOnSetDate(self,row):
		if row['eventdate'] == '':
			return row['onSetDate']
		else:
			return moment.date(row['eventdate']).format('YYYY-MM-DD')
	# Get onset for TrackedEntityInstances
	def getTeiOnSetDate(self,row):
		if row['dateOfOnSet'] == '':
			return row['dateOfOnSet']
		else:
			return moment.date(row['created']).format('YYYY-MM-DD')
	# Check if reminder is to be sent
	def getReminder(self,row):
		if row['reminderDate'] == pd.to_datetime(self.today):
			return 'true'
		else:
			return 'false'
	def getAlert(self,row):
		print(row)
		if (row['confirmedValue'] < row['epiThreshold']) & (row['suspectedValue'] >= row['alertThreshold']) & str(row['endDate']) > str(self.today):
			return "true"
		else:
			return "false"
	# replace data of onset with event dates
	def replaceDatesWithEventData(self,row):
		
		if row['onSetDate'] == '':
			return pd.to_datetime(row['eventdate'])
		else:
			return pd.to_datetime(row['onSetDate'])

	# Transform df for DHIS2 JSON events format
	# @param dataFrame df
	# @return df array
	def transformDf(self,df,type):
		df = json.loads(df)
		if type == 'EPI':
			if len(df) > 0:
				for row in df:
					row['period'] = row['dateOfOnSetWeek']
					row['epicode'] = 'E_'+ self.generateCode()
					row['suspectedValue'] = round(row['suspectedValue'])
					row['confirmedValue'] = round(row['confirmedValue'])
					row['deathValue'] = round(row['deathValue'])				
					#### Check epidemic closure
					if row['epidemic'] == "true" and row['active'] == "true" and row['reminder'] == "false":
					 	row['status']='Closed'
					 	row['active']='false'
					 	row['closeDate']=self.today
					 	row['reminderSent']='false'
					 	row['dateReminderSent']=''
					 	# Send closure message

					elif row['epidemic'] == "true" and row['active'] == "true" and row['reminder'] == "true":
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
		else:
			if len(df) > 0:
				df = df ;
			else:
				df = []
		
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
				event['event'] = row['event']
				event['orgUnit'] = row['reportingOrgUnit']
				event['eventDate'] = row['firstCaseDate']
				event['status'] = 'COMPLETED'
				event['program'] = config['reportingProgram']['id']
				event['programStage'] = config['reportingProgram']['programStage']['id']
				event['storedBy'] = 'idsr'
				
				for key,value in row.items(): # for key in [*row]
					if key == 'suspectedValue':
				 		event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'suspected'),'value':row['suspectedValue']})
					elif key == 'confirmedValue':
				 		event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'confirmed'),'value':row['confirmedValue']})
					elif key == 'firstCaseDate':
					 	event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'firstCaseDate'),'value':row['firstCaseDate']})
					elif key == 'orgUnit':
					 	event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'origin'),'value':row['orgUnit']})
					 	event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'outbreakId'),'value':row['epicode']})
					elif key == 'disease':
					 	event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'disease'),'value':row['disease']})
					elif key == 'endDate':
					 	event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'endDate'),'value':row['endDate']})	
					elif key == 'status':
					 	event['dataValues'].append({'dataElement': self.getDataElement(dataElements,'status'),'value':row['status']})					 
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
	def detectBasedOnProgramIndicators(self,caseEvents,diseaseMeta,orgUnits,type,dateData):
		dhis2Events = []
		detectionLevel = int(diseaseMeta['detectionLevel'])
		reportingLevel = int(diseaseMeta['reportingLevel'])
		if(caseEvents != 'HTTP_ERROR'):
			if((caseEvents != 'undefined') and (caseEvents['rows'] != 'undefined') and caseEvents['height'] >0):
				df = self.createDataFrame(caseEvents,type)

				caseEventsColumnsById = df.columns 
				dfColLength = len(df.columns)				

				if(type =='EVENT'):
					# If date of onset is null, use eventdate
					#df['dateOfOnSet'] = np.where(df['onSetDate']== '',pd.to_datetime(df['eventdate']).dt.strftime('%Y-%m-%d'),df['onSetDate'])
					df['dateOfOnSet'] = df.apply(self.getOnSetDate,axis=1)
					# Replace all text with standard text
					
					df = self.replaceText(df)
					
					# Transpose and Aggregate values
					
					dfCaseClassification = df.groupby(['ouname','ou','disease','dateOfOnSet'])['caseClassification'].value_counts().unstack().fillna(0).reset_index()
					
					dfCaseImmediateOutcome = df.groupby(['ouname','ou','disease','dateOfOnSet'])['immediateOutcome'].value_counts().unstack().fillna(0).reset_index()
					
					dfTestResult = df.groupby(['ouname','ou','disease','dateOfOnSet'])['testResult'].value_counts().unstack().fillna(0).reset_index()
					
					dfTestResultClassification = df.groupby(['ouname','ou','disease','dateOfOnSet'])['testResultClassification'].value_counts().unstack().fillna(0).reset_index()
					
					dfStatusOutcome = df.groupby(['ouname','ou','disease','dateOfOnSet'])['statusOutcome'].value_counts().unstack().fillna(0).reset_index()

					combinedDf = pd.merge(dfCaseClassification,dfCaseImmediateOutcome,on=['ou','ouname','disease','dateOfOnSet'],how='left').merge(dfTestResultClassification,on=['ou','ouname','disease','dateOfOnSet'],how='left').merge(dfTestResult,on=['ou','ouname','disease','dateOfOnSet'],how='left').merge(dfStatusOutcome,on=['ou','ouname','disease','dateOfOnSet'],how='left')
					combinedDf.sort_values(['ouname','disease','dateOfOnSet'],ascending=[True,True,True])
					combinedDf['dateOfOnSetWeek'] = pd.to_datetime(combinedDf['dateOfOnSet']).dt.strftime('%YW%V')
					combinedDf['confirmedValue'] = combinedDf.apply(self.getConfirmed,args=([combinedDf.columns]),axis=1)			
					combinedDf['suspectedValue'] = combinedDf.apply(self.getSuspected,args=([combinedDf.columns]),axis=1)

					#combinedDf['deathValue'] = combinedDf.apply(self.getDeaths,args=([combinedDf.columns]),axis=1)
					
					dfConfirmed = combinedDf.groupby(['ouname','ou','disease','dateOfOnSetWeek'])['confirmedValue'].agg(['sum']).reset_index()
					
					dfConfirmed.rename(columns={'sum':'confirmedValue' },inplace=True)
					dfSuspected = combinedDf.groupby(['ouname','ou','disease','dateOfOnSetWeek'])['suspectedValue'].agg(['sum']).reset_index()
					dfSuspected.rename(columns={'sum':'suspectedValue' },inplace=True)
					dfFirstAndLastCaseDate = df.groupby(['ouname','ou','disease'])['dateOfOnSet'].agg(['min','max']).reset_index()
					dfFirstAndLastCaseDate.rename(columns={'min':'firstCaseDate','max':'lastCaseDate'},inplace=True)

					aggDf = pd.merge(dfConfirmed,dfSuspected,on=['ouname','ou','disease','dateOfOnSetWeek'],how='left').merge(dfFirstAndLastCaseDate,on=['ouname','ou','disease'],how='left')
					aggDf['reportingOrgUnitName'] = aggDf.loc[:,'ou'].apply(self.getOrgUnitValue,args=(orgUnits,(reportingLevel-1),'name'))
					aggDf['reportingOrgUnit'] = aggDf.loc[:,'ou'].apply(self.getOrgUnitValue,args=(orgUnits,(reportingLevel-1),'id'))
					aggDf['incubationDays'] = int(diseaseMeta['incubationDays'])
					aggDf['endDate'] = pd.to_datetime(pd.to_datetime(dfDates['lastCaseDate']) + pd.to_timedelta(pd.np.ceil(2*aggDf['incubationDays']), unit="D")).dt.strftime('%Y-%m-%d')
					aggDf['reminderDate'] = pd.to_datetime(pd.to_datetime(aggDf['lastCaseDate']) + pd.to_timedelta(pd.np.ceil(2*aggDf['incubationDays']-7), unit="D")).dt.strftime('%Y-%m-%d')
					aggDf.rename(columns={'ouname':'orgUnitName','ou':'orgUnit'},inplace=True);
					aggDf['active'] =  aggDf.apply(self.getActive,axis=1)
					aggDf['reminder'] =  aggDf.apply(self.getReminder,axis=1)
	
				else:
					df1 = df.iloc[:,(detectionLevel+4):dfColLength]
					df.iloc[:,(detectionLevel+4):dfColLength] = df1.apply(pd.to_numeric,errors='ignore',downcast='integer')
					df.fillna(0.0,axis=1,inplace=True)
					if(dateData['height'] > 0):
						dfDates = self.createDataFrame(dateData,'DATES')
						dfDates.rename(columns={dfDates.columns[7]:'fldisease',dfDates.columns[8]:'dateOfOnSet'},inplace=True)
						dfDates['dateOfOnSet'] = dfDates.apply(self.getTeiOnSetDate,axis=1)
						dfDates = dfDates.groupby(['ou','fldisease'])['dateOfOnSet'].agg(['min','max']).reset_index()
						dfDates.rename(columns={'min':'firstCaseDate','max':'lastCaseDate'},inplace=True)
						df['incubationDays'] = int(diseaseMeta['incubationDays'])
						df['endDate'] = pd.to_datetime(pd.to_datetime(dfDates['lastCaseDate']) + pd.to_timedelta(pd.np.ceil(2*df['incubationDays']), unit="D")).dt.strftime('%Y-%m-%d')
						df['reminderDate'] = pd.to_datetime(pd.to_datetime(dfDates['lastCaseDate']) + pd.to_timedelta(pd.np.ceil(2*df['incubationDays']-7), unit="D")).dt.strftime('%Y-%m-%d')
						df['active'] =  df.apply(self.getActive,axis=1)
						df['reminder'] =  df.apply(self.getReminder,axis=1)
						df = pd.merge(df,dfDates,right_on=['ou'],left_on=['organisationunitid'],how='left')
					else:
						df['active'] = "false"
						df['endDate'] =""
						df['firstCaseDate'] =""
						df['lastCaseDate'] =""
						df['reminderDate']=""
						df['reminder'] ="false"
					df.rename(columns={df.columns[10]:'confirmedValue' },inplace=True)
					df.rename(columns={df.columns[11]:'deathValue' },inplace=True)	
					df.rename(columns={df.columns[12]:'suspectedValue' },inplace=True)
					df['reportingOrgUnitName'] = df.iloc[:,reportingLevel-1]
					df['reportingOrgUnit'] = df.loc[:,'organisationunitid'].apply(self.getOrgUnitValue,args=(orgUnits,(reportingLevel-1),'id'))
					df.rename(columns={'organisationunitname':'orgUnitName','organisationunitid':'orgUnit'},inplace=True);
					df['dateOfOnSetWeek'] = self.getIsoWeek(self.today)
					df['disease'] = diseaseMeta['disease']
					aggDf = df
				
				aggDf['alertThreshold'] = int(diseaseMeta['alertThreshold'])
				aggDf['epiThreshold'] = int(diseaseMeta['epiThreshold'])
		
				#df['confirmed_suspected_cases'] = df[['confirmedValue','suspectedValue']].sum(axis=1)
				
				aggDf['epidemic'] = np.where(aggDf['confirmedValue'] >= aggDf['epiThreshold'],'true','false')
	
				alertQuery = (aggDf['confirmedValue'] < aggDf['epiThreshold']) & (aggDf['suspectedValue'].astype(np.int64) >= aggDf['alertThreshold'].astype(np.int64)) & (aggDf['endDate'] > self.today)
				aggDf['alert'] = np.where(alertQuery,'true','false')
				
				# Filter out those greater or equal to threshold
				trialname = diseaseMeta['disease'] + "_dftrial.csv"
				#aggDf.to_csv(trialname, sep=',', encoding='utf-8')

				df_epidemics = aggDf[aggDf['epidemic'] == 'true']
				df_alert = aggDf[aggDf['alert'] == 'true']
				atrialname = diseaseMeta['disease'] + "_dftrial_alert.csv"
				#df_alert.to_csv(atrialname, sep=',', encoding='utf-8')
				# lab confirmed = true
				# type of emergency = outbreak or other			

				dhis2Events = {'alerts': self.transformDf(df_alert.to_json(orient='records'),'ALERT') ,'events': self.transformDf(df_epidemics.to_json(orient='records'),'EPI')}
				
			else:
				# No data for cases found
				dhis2Events = {'alerts':[],'events':[]}
				pass
			return dhis2Events
		else:
			print("No outbreaks/epidemics for " + diseaseMeta['disease'])
			return dhis2Events
	# Add DHIS2 UIDS to new epidemics
	def assignUids(self,epidemics,uids):
		for epi in epidemics:
			epi['event'] = uids[0]
			del uids[0]
		return epidemics

	## Get Alerts
	def getAlerts(self,events):
		alerts = []
		for event in events:
			if event['alert'] == 'true':
				event['period'] = event['dateOfOnSetWeek']
				alerts.append(event)
			else:
				pass
		return alerts
	# check existing value
	def checkExistingAlert(self,values,val):
		exists = False
		if(len(values) > 0):
			for v in values:
				if v['period'] == val['period'] and v['disease'] == val['disease'] and v['orgUnit'] == val['orgUnit']:
					return True
		return exists

	# Remove already sent alerts
	def getNewAlerts(self,events,alerts):
		newAlerts = []
		if(len(alerts) > 0):
			for alert in alerts:
				if(len(events) > 0):
					for event in events:
						exists = self.checkExistingAlert(events,alert)
						if(not exists):	
							exists = self.checkExistingAlert(newAlerts,alert)
							if(not exists):								
								newAlerts.append(alert)
							else:
								pass							
						else:
							pass
				else:
					newAlerts.append(alert)
		else:
			pass
			
		return newAlerts

	# Remove existing  and update with new from data store epidemics
	def getUpdatedEpidemics(self,epidemics,events):
		updatedEpidemics = {}
		newEpidemics = []
		reminders = []
		existEpidemics = []
		if(len(epidemics) > 0):
			for epi in epidemics:
				for event in events:				
					if (epi['orgUnit'] == event['orgUnit']) and (epi['disease'] == event['disease']) and (str(epi['endDate']) ==''):
						# Existing and updates
						epi['confirmedValue'] = event['confirmedValue']
						epi['suspectedValue'] = event['suspectedValue']
						epi['deaths'] = event['deathValue']
						epi['status'] = event['status']
						epi['dateReminderSent'] = event['dateReminderSent']
						epi['reminderSent'] = event['reminderSent']
						epi['lastCaseDate'] = event['lastCaseDate']
						epi['updated'] = 'true'
						epi['endDate'] = event['endDate']
						epi['active'] = event['active']
						epi['closeDate'] = event['closeDate']
						#existEpidemics.append(epi)

					elif(epi['orgUnit'] == event['orgUnit']) and (epi['disease'] == event['disease']) and (str(epi['endDate']) != ''):
						event['updated'] = 'false'
						newEpidemics.append(event)
					elif(epi['orgUnit'] == event['orgUnit']) and (epi['disease'] == event['disease']) and (event['reminderDate'] == self.today):
						event['updated'] = 'false'
						epi['dateReminderSent'] = self.today
						epi['reminderSent'] = 'true'
						reminders.append(event)
					else:
						## Remove existing with no update from payload
						#epidemics.remove(epi)
						pass
		else:
			for event in events:
				event['updated'] = 'false'
				newEpidemics.append(event)
		updatedEpidemics['newEvents'] = newEpidemics
		updatedEpidemics['reminders'] = reminders
		updatedEpidemics['existEvents'] = epidemics
		return updatedEpidemics

	def getRootOrgUnit(self):
		root = {};
		root = self.getHttpData(self.url,'organisationUnits',self.username,self.password,params={"paging":"false","filter":"level:eq:1"})	
		return root['organisationUnits']

	def iterateDiseases(self,diseasesMeta,epidemics,alerts,type):
		newUpdatedEpis = []
		existingAlerts = alerts
		existingEpidemics = epidemics
		programConfig = diseasesMeta['config']
		mPeriods = programConfig['mPeriods']
		nPeriods = programConfig['nPeriods']
		rootOrgUnit = self.getRootOrgUnit()
		programStartDate = moment.date(self.today).subtract(days=8)
		programStartDate = moment.date(programStartDate).format('YYYY-MM-DD')
		for diseaseMeta in diseasesMeta['diseases']:
			
			ouLevel = 'LEVEL-' + str(diseaseMeta['detectionLevel'])
			detectionXLevel = diseaseMeta['detectionLevel']
			ouFields = 'organisationUnits'
			ouParams = {"fields": "id,code,ancestors[id,code,name]","paging":"false","filter":"level:eq:"+ str(detectionXLevel)}
			epiReportingOrgUnit	= self.getHttpData(self.url,ouFields,self.username,self.password,params=ouParams)
			piSeparator =';'
			piIndicatorsArray = self.getArrayFromObject(diseaseMeta['programIndicators'])
			piIndicators = piSeparator.join(piIndicatorsArray)
			piFields = 'analytics'

			if diseaseMeta['epiAlgorithm'] == "CASE_BASED":
				print("Detecting for case based diseases")
				print ("Start outbreak detection for %s" %diseaseMeta['disease'])
				#LAST_7_DAYS	
				
				eventsFields = 'analytics/events/query/' + self.programUid					
				piFields = 'analytics'
				teiFields = 'trackedEntityInstances/query'
				# Get first case date: min and max is always the last case date registered
				### Get Cases or Disease Events
				#
				caseEventParams = { "dimension": ['pe:' + self.period,'ou:' + ouLevel,self.dateOfOnsetUid,self.conditionOrDiseaseUid + ":IN:" + diseaseMeta["code"],self.patientStatusOutcome,self.regPatientStatusOutcome,self.caseClassification,self.testResult,self.testResultClassification],"displayProperty":"NAME"}
				piEventParams = {"dimension": ["dx:"+ piIndicators,"ou:" + ouLevel],"filter": "pe:" + self.period,"displayProperty":"NAME","columns":"dx","rows":"ou","skipMeta":"false","hideEmptyRows":"true","skipRounding":"false","showHierarchy":"true"}
				
				cDisease = programConfig["notificationProgram"]["disease"]["id"]+":IN:" + diseaseMeta["code"]
				cOnset = programConfig["notificationProgram"]["dateOfOnSet"]["id"]
				rootOrgUnitId = rootOrgUnit[0]["id"]

				teiParams = {"ou":rootOrgUnitId,"program":self.programUid,"ouMode":"DESCENDANTS","programStatus":"ACTIVE","attribute": [cDisease,cOnset] ,"programStartDate":programStartDate,"skipPaging":"true"}

				if(type =='EVENT'):
					caseEvents = self.getHttpData(self.url,eventsFields,self.username,self.password,params=caseEventParams)			
				if(type =='ANALYTICS'):
					caseEvents = self.getHttpData(self.url,piFields,self.username,self.password,params=piEventParams)
				if(( caseEvents != 'HTTP_ERROR') and (epiReportingOrgUnit != 'HTTP_ERROR')):
					orgUnits = epiReportingOrgUnit['organisationUnits']
					dateData = self.getHttpData(self.url,teiFields,self.username,self.password,params=teiParams)
					detectedEpidemics = self.detectBasedOnProgramIndicators(caseEvents,diseaseMeta,orgUnits,type,dateData)
					# Creating threshold alerts
					detectedAlerts = self.getAlerts(detectedEpidemics['alerts'])
					
					newAlerts = self.getNewAlerts(alerts,detectedAlerts)
					print("Number of alerts ",len(newAlerts))
					existingAlerts.extend(newAlerts)
					# Get Uids for identifying epidemics
					allUpdatedEpidemics = self.getUpdatedEpidemics(epidemics,detectedEpidemics['events'])
					newEpidemics = allUpdatedEpidemics['newEvents']							
					updatedEpidemics = allUpdatedEpidemics['existEvents']
					reminders = allUpdatedEpidemics['reminders']
					print("Number of New Epidemics ", len(newEpidemics))
					if( len(newEpidemics) > 0):
						epiCodesFields = "system/id"
						epiCodesParams = { "limit" : len(newEpidemics) }
						
						epiCodes = self.getHttpData(self.url,epiCodesFields,self.username,self.password,params=epiCodesParams)
						if(epiCodes != 'HTTP_ERROR'):
							epiCodesUids = epiCodes['codes']
							newEpidemics = self.assignUids(newEpidemics,epiCodesUids)
							updatedEpidemics.extend(newEpidemics)
						else:
							print("Failed to generated DHIS2 UID codes")
					else:
						print("Exiting no new outbreaks detected")
					print("Detecting and updating Outbreaks .... ")
					events = self.createDHIS2Events(updatedEpidemics,programConfig);
					print("Updating epidemics in the datastore online for %s" %diseaseMeta['disease'])
					epiUpdateDataStoreEndPoint  = 'dataStore/' + self.dataStore + '/epidemics'
					self.updateJsonData(self.url,epiUpdateDataStoreEndPoint,self.username,self.password,updatedEpidemics)
					print("Updating epidemics in the events online for %s" %diseaseMeta['disease'])
					epiUpdateEventEndPoint  = 'events?importStrategy=CREATE_AND_UPDATE'
					self.postJsonData(self.url,epiUpdateEventEndPoint,self.username,self.password,events)
					print ("Finished creating Outbreaks for %s" %diseaseMeta['disease'])
					### Send outbreak messages
					print("Sending outbreak messages")
					self.createAlerts(diseaseMeta['notifiableUserGroups'],newEpidemics,'EPI')
					self.createAlerts(diseaseMeta['notifiableUserGroups'],reminders,'REMINDER')
					self.createAlerts(diseaseMeta['notifiableUserGroups'],newAlerts,'ALERT')
					

				else:
					print("Failed to retrieve case events from analytics")
					
			elif diseaseMeta['epiAlgorithm'] == "SEASONAL":
				print("Detecting for seasonal")
				print ("Start outbreak detection for %s" %diseaseMeta['disease'])
				# periods are aggregate generated
				aggPeriod = self.createAggThresholdPeriod(mPeriods,nPeriods,'SEASONAL')
				aggPeriods = piSeparator.join(aggPeriod)			
						
				aggParams = {"dimension": ["dx:"+ piIndicators,"ou:" + ouLevel,"pe:" + aggPeriods],"displayProperty":"NAME","tableLayout":"true","columns":"dx;pe","rows":"ou","skipMeta":"false","hideEmptyRows":"true","skipRounding":"false","showHierarchy":"true"}

				aggIndicators = self.getHttpData(self.url,piFields,self.username,self.password,params=aggParams)
				
				if(( aggIndicators != 'HTTP_ERROR') and (epiReportingOrgUnit != 'HTTP_ERROR')):
					aggData = aggIndicators
					aggOrgUnit = epiReportingOrgUnit['organisationUnits']
					detectedAggEpidemics = self.detectOnAggregateIndicators(aggData,diseaseMeta,epidemics,aggOrgUnit,aggPeriod,mPeriods,nPeriods)
					# Creating epidemics alerts
					# Creating threshold alerts
					detectedAggAlerts = self.getAlerts(detectedAggEpidemics['alerts'])
					
					newAggAlerts = self.getNewAlerts(alerts,detectedAggAlerts)
					print("Number of alerts ",len(newAggAlerts))
					existingAlerts.extend(newAggAlerts)
					# Get Uids for identifying epidemics
					allUpdatedAggEpidemics = self.getUpdatedEpidemics(epidemics,detectedAggEpidemics['events'])
					newEpidemics = allUpdatedAggEpidemics['newEvents']							
					updatedEpidemics = allUpdatedAggEpidemics['existEvents']
					reminders = allUpdatedAggEpidemics['reminders']
					print("Number of New Epidemics ", len(newEpidemics))
					if( len(newEpidemics) > 0):
						epiCodesFields = "system/id"
						epiCodesParams = { "limit" : len(newEpidemics) }
						
						epiCodes = self.getHttpData(self.url,epiCodesFields,self.username,self.password,params=epiCodesParams)
						if(epiCodes != 'HTTP_ERROR'):
							epiCodesUids = epiCodes['codes']
							newEpidemics = self.assignUids(newEpidemics,epiCodesUids)
							updatedEpidemics.extend(newEpidemics)
						else:
							print("Failed to generated DHIS2 UID codes")
					else:
						print("Exiting no new outbreaks detected")
					print("Detecting and updating Outbreaks .... ")
					events = self.createDHIS2Events(updatedEpidemics,programConfig);
					print("Updating epidemics in the datastore online for %s" %diseaseMeta['disease'])
					epiUpdateDataStoreEndPoint  = 'dataStore/' + self.dataStore + '/epidemics'
					self.updateJsonData(self.url,epiUpdateDataStoreEndPoint,self.username,self.password,updatedEpidemics)
					print("Updating epidemics in the events online for %s" %diseaseMeta['disease'])
					epiUpdateEventEndPoint  = 'events?importStrategy=CREATE_AND_UPDATE'
					self.postJsonData(self.url,epiUpdateEventEndPoint,self.username,self.password,events)
					print ("Finished creating Outbreaks for %s" %diseaseMeta['disease'])
					### Send outbreak messages
					print("Sending outbreak messages")
					self.createAlerts(diseaseMeta['notifiableUserGroups'],newEpidemics,'EPI')
					self.createAlerts(diseaseMeta['notifiableUserGroups'],reminders,'REMINDER')
					self.createAlerts(diseaseMeta['notifiableUserGroups'],newAlerts,'ALERT')
					

				else:
					print("Failed to retrieve case events from analytics")
			else:
				print("Detecting for non seasonal")
		

		print("Save alerts in the datastore online",len(existingAlerts))
		epiUpdateDataStoreEndPointAlert  = 'dataStore/' + self.dataStore + '/alerts'
		self.updateJsonData(self.url,epiUpdateDataStoreEndPointAlert,self.username,self.password,existingAlerts)
			
		return "Done processing"

		# Start epidemic detection
	def startEpidemics(self):
		print ("Started detection for outbreaks/epidemics")
		# Get Disease Metadata
		diseaseFields = 'dataStore/' + self.dataStore + '/diseases'
		auth = self.getAuth()
		self.username = auth['username']
		self.password = auth['password']
		self.url = auth['url']
		diseasesMeta = self.getHttpData(self.url,diseaseFields,self.username,self.password,{})

		# Get Epidemics
		if(diseasesMeta != 'HTTP_ERROR'):
			epidemicsFields = 'dataStore/' + self.dataStore + '/epidemics'
			epidemicsData = self.getHttpData(self.url,epidemicsFields,self.username,self.password,{})
			
			alertsFields = 'dataStore/' + self.dataStore + '/alerts'
			alertsData = self.getHttpData(self.url,alertsFields,self.username,self.password,{})

			if(epidemicsData != 'HTTP_ERROR'):
				epidemicsProcessed = self.iterateDiseases(diseasesMeta,epidemicsData,alertsData,'ANALYTICS')
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
