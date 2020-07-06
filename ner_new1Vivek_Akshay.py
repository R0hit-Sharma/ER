# -*- coding: utf-8 -*-
import pprint
import requests
import ast
from pymongo import MongoClient
from storage import storetext
import sys
import config
import logging
import logging.config
from emails import sendemail
from string import Template
import traceback
sys.path.append('./district_extractor')
from districtExtractor import districtFinder
sys.path.append('./category')
from Category import category_finder

#logging.config.fileConfig('logging.conf')
#logger = logging.getLogger('entExtrL')

pp=pprint.PrettyPrinter(indent=4)
calais_url = 'https://api.thomsonreuters.com/permid/calais'
client = MongoClient(config.mongoConfigs['host'],config.mongoConfigs['port'])
db=client[config.mongoConfigs['db']]
collection=db['articles']
collName ='articles'
error_codes={
             'INVALID_KEY':401,
             'BAD_HEADER_LANGUAGE_CONTENT':400,
             'UNSUPPORTED_OUTPUT':406,
             'LARGE_REQUEST':413,
             'UNSUPPORTED_INPUT_FORMAT':415,
             'TOO_MANY_REQUESTS':429,
             'SERVER_ERROR1':500,
             'SERVER_ERROR2':503
             }

error_types={
             'NETWORK':1,
             'BAD_CONTENT':2,
             'OTHER':3
             }

open_calais_keys=['8g5LzrQt3MLfYUpGKPENv3BEuncndZ2q',
                  'GSUQO0mV6iLAB2iaRGiDA5YkWZ8SWxJe',
                  'BZS6QrGtQ1fnlwf9GqaNl58AAz4DQB52',
                  'KporSMosEEnJEgGWgdy7p8w2v1le3Agb',
                  'WzCY4Wqx6RCNpE21Blsid40sGMt7lwlP',
                  'gmKgxH7r2OAuFt5SqPd2wAFwXX1UviS3',
                  'AROMgSEnb1rFqJKZ8p8E1SJqKA49a2Nw',
                  '4DHODvWeTyAXjmyNNkxKceAULkmhtKHs',
                  '9DTABMrZkW59KbAG372S9W9p4A0hYTwh',
                  'ZQxnKX1uTFqzX7BEuhqw1yWy0G90DKG4',
                  'iitB04PtigSo9e7Dj5VOgr2kTZGI8tro',
                  'olqw1mNmzjsYmYrQkiNvjNJ1jzVf1mes',
                  '4mJoQuvOPYwUYdGChyjksz9GdAdipBoG',
                  'tNaor6PBUQy4jbVvGGwDHoGGFANfRXMq',
                  'qArT2nakUwfeGAyyQJtAIJdoBPUYksUj',
                  'zAVMnuluwuG9xA93GPasZ3uGayUsJ32G',
                  'mGeYJSvdqmGlCqseGZtmy5G2LNXJ8cmV'

                  ]

'''
Entity types :
Anniversary City Continent Country Currency Editor FaxNumber
Holiday IndustryTerm EmailAddress EntertainmentAwardEvent
Facility Journalist MarketIndex MedicalCondition MedicalTreatment
Movie MusicAlbum MusicGroup NaturalFeature OperatingSystem PhoneNumber
Position ProgrammingLanguage ProvinceOrState PublishedMedium
RadioStation Region RadioProgram SportsEvent SportsGame SportsLeague
Technology TVShow TVStation URL
'''
def mail(errType,desc):
    global error_types
    if errType==error_types['NETWORK']:
        subject='Entity extraction script stopped working'
    elif errType==error_types['BAD_CONTENT']:
        subject='Entity extraction : Bad input'
    elif errType==error_types['OTHER']:
        subject='Entity extraction : Bad response'

    filein = open('./emails/common/network_error.txt')
    src = Template( filein.read() )
    d={'desc':desc}
    mesg = src.substitute(d)
    sendemail.sendEmail(subject, mesg)

disam_feature_category_list =["City","Continent","Country","Company","Currency","Deal","Organization","Person","Product","ProvinceOrState","Region","TopmostPublicParentCompany"]
def rotate(l, n):
    return l[-n:] + l[:-n]

def isPresent(item,prop):
    return (item.get(prop) and item[prop]!='N\\/A')


count=0

def extractEntity(article):
    global open_calais_keys,error_types, collName
    accessToken=open_calais_keys[0]
    open_calais_keys = rotate(open_calais_keys, 1)
    global count
    count+=1
    text=article['text']
    #-----------------------------------------------------
    # Vivek and Akshay
    # print(text)
    districtList = districtFinder(text)
    print(districtList)

    articleCategories = category_finder(text)
    print(articleCategories)

    #-----------------------------------------------------

    entities =[]
    socialTags=[]

    headers = {'X-AG-Access-Token' : accessToken, 'Content-Type' : 'text/raw','omitOutputtingOriginalText':'true','x-calais-contentClass':'news', 'x-calais-language':'English','outputFormat':'application/json'}
    response=None

    try:
        response = requests.post(calais_url, data=text, headers=headers, timeout=80,proxies={'http':config.proxy_server,'https':config.proxy_server})
    except:
        print(sys.exc_info()[0])
        #logger.error('Network error '+ str(sys.exc_info()[0]))
        return

    if response.status_code==429:
        #logger.error('Cannot extract entities for '+ str(article['_id'])+' .Too many requests. Status code : '+ str(response.status_code))
        return
    elif response.status_code!=200:
        print("status not 200")
        desc= 'Cannot extract entities of article '+ str(article['_id'])+'. Status code : '+str(response.status_code)
        #logger.error(desc)
        #mail(error_types['BAD_CONTENT'],desc)
        return
    try:
        text3 =ast.literal_eval(response.text)
        val = text3.values()
        for item in val:
            #print(item)
            #print("\n")
            keys_list =item.keys()
            if('_typeGroup' in keys_list):

                if(item['_typeGroup'] == 'entities'):
                    dict1 ={}
                    dict1['type'] = item['_type']
                    dict1['instances']=item['instances']
                    dict1['forenduserdisplay'] = item.get('forenduserdisplay')
                    dict1['name'] = item['name']

                    dict1['aliases']=[item['name'].upper()]
                    dict1['relevance']=item['relevance']# rel of entity with the doc. 0.2, 0.5, 0.8 . 1-reserved for the
                                                        # company identified as the reporting company in a document with a predefined format such as an SEC report.
                                                        #0 - reserved for  mentions of companies as rating agencies, reporting agencies, stock exchanges, and social applications

                    if (item['_type'] in disam_feature_category_list):
                        dict1['resolutions'] = item.get('resolutions') #clean

                    elif (item['_type'] == 'Product'):
                        if isPresent(item,'producttype'):
                            dict1['producttype'] =item['producttype']
                        else:
                            dict1['producttype'] = None;

                    elif (item['_type'] == 'PoliticalEvent'):
                        if isPresent(item,'date'):
                            dict1['date'] = item['date']
                        else:
                            dict1['date'] =None;

                        if isPresent(item,'datestring'):
                            dict1['datestring'] =item.get('datestring')
                        else:
                            dict1['datestring'] =None;

                        if isPresent(item,'location'):
                            dict1['location'] =item.get('location')
                        else:
                            dict1['location'] =None;

                        if isPresent(item,'politicaleventtype'):
                            dict1['politicaleventtype'] = item.get('politicaleventtype')
                        else:
                            dict1['politicaleventtype'] =None;

                    elif(item['_type'] == 'Organization'):
                        if isPresent(item,'nationality'):
                            dict1['nationality'] = item.get('nationality')
                        else:
                            dict1['nationality'] =None;

                        if isPresent(item,'organizationtype'):
                            dict1['organizationtype'] = item.get('organizationtype') #ports, governmental military, governmental civilian,political party, N/A
                        else:
                            dict1['organizationtype'] =None;

                    elif(item['_type'] == 'Person'):
                        if isPresent(item, 'nationality'):
                            dict1['nationality'] = item.get('nationality')
                        else:
                            dict1['nationality'] =None;

                        if isPresent(item, 'persontype'):          #sports, entertainment, political, economic, military, NA.
                            dict1['persontype'] = item.get('persontype')
                        else:
                            dict1['persontype'] =None;

                        dict1['confidencelevel'] = item['confidencelevel'] #0 to 1. Confidence that the extracted name is indeed a person

                    elif(item['_type']=='Company'):
                        if isPresent(item, 'nationality'):
                            dict1['nationality'] = item.get('nationality')
                        else:
                            dict1['nationality'] =None;

                        dict1['confidencelevel']=item['confidencelevel'] #0 to 1. Confidence that the extracted name is indeed a company
                        dict1['relevancecont']=item['relevancecont'] #check
                    entities.append(dict1)

                elif(item['_typeGroup'] == 'socialTag'):
                    dict1={}
                    dict1['type']='socialTag'
                    dict1['forenduserdisplay'] = item['forenduserdisplay']
                    dict1['name']=item['name']
                    dict1['importance']=item['importance'] # 1(very centric) 2(somewhat centric)3(less centric)
                    dict1['originalvalue']=item.get('originalvalue') # The original title of the associated Wikipedia article, if the title has changed.
                    socialTags.append(dict1)

        print("No of entities :"+str(len(entities))+':'+str(article['_id']))
        # {'districtNames':districtList}
        storetext.updateArticle(collName,article['_id'], {'entities':entities,'socialTags':socialTags,'districtNames':districtList,'articleCategories':articleCategories})
        print('---------------------------'+str(count)+'-----------------------------')
    except:
        print("Unexpected error while parsing response:", sys.exc_info()[0])
        desc='Error while parsing response of article id : '+str(article['_id'])+'. Error : '+ str(sys.exc_info()[0])
        #logger.error(desc)
        #mail(error_types['OTHER'],desc)

def fetchEn():
    global collection
    '''
    while(1):
        #cursor=collection.find({'$and':[{'entities':{"$exists":False}},{'publishedDate':{'$gte':'2018-01-01'}},{'publishedDate':{'$lte':'2018-12-31'}}]}).limit(1000)
        cursor=collection.find({'$and':[{'entities':{"$exists":False}},{'publishedDate':{'$gte':'2019-09-25'}}]}).limit(1000)
        for art in cursor:
            extractEntity(art)
        cursor.close()
    '''
    #cursor=collection.find({'$and':[{'entities':{"$exists":False}},{'publishedDate':{'$gte':'2019-09-25'}}]})
    cursor=collection.find({'$and':[{'entities':{"$exists":False}},{'publishedDate':{'$gte':'2019-02-11'}},{'publishedDate':{'$lte':'2020-02-12'}}]})
    # mondo shell query db.articles.find({'$and':[{'publishedDate':{'$gte':'2020-02-16'}},{'publishedDate':{'$lte':'2020-02-16'}}]}, {"districtNames":1, "_id":0}).pretty()
    #cursor=collection.find({'$and':[{'entities':{"$exists":False}},{'publishedDate':{'$gte':'2019-10-09'}},{'publishedDate':{'$lte':'2019-10-11'}}]})
    for art in cursor:
        extractEntity(art)
    cursor.close()

if __name__=='__main__':
	try:
		fetchEn()
	except:
		print(sys.exc_info()[0])
        #logger.error('Exception : '+ str(sys.exc_info()[0]))
        #mail(error_types['NETWORK'],'Exception : '+ str(sys.exc_info()[0]))
	client.close()
