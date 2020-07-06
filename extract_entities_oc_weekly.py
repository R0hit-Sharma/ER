from pymongo import MongoClient
import re  
import traceback

#client = MongoClient(config.mongoConfigs['host'], config.mongoConfigs['port'], j=True)
#db = client[config.mongoConfigs['db']] 

#for mongodb gem2
#read collection from gem2
client1 = MongoClient('10.237.26.159', 27017, j=True)
db1 = client1['media-db']# to change--Rohit

#for mongod on gem3
#create extracted entities collection on gem3
entityCount = 0
client = MongoClient('10.208.23.165', 27017, j=True)
db = client['media-db'] # to change--Rohit

def modifyName(entity):
    titles=[]
    titlePattern= '(Mr\.?|Ms\.?|Mrs\.?|Dr\.?|Prof\.?|prof\.?|Sir|Shri)\s+'
    wordPattern='.*(Chairman|Principal|General|Commissioner|Minister|Ambassador|Superintendent|Commander|Justice|Speaker|Professor|President|Mayor|Inspector|Police|Officer|Collector|(t|T)rustee|Dean|Magistrate|Director|Secretary|Judge|Engineer|Governor|Chancellor|Vice-Chancellor)(\s+of)?'
    compiledTitle=re.compile(titlePattern)
    compiledWord=re.compile(wordPattern)

    name=entity['stdName']
    matchedTitle=compiledTitle.match(name)
    
    if matchedTitle:
        t=matchedTitle.group(0)
        name=name.replace(t,'')
   
    title=''
    m=compiledWord.match(name)
    
    if m:
        title=m.group(0)
        name=name.replace(title,'')
        
    entity['stdName']=name.strip()
    
    if not (title.strip()=='' or (title in titles)):
        titles.append(title)
    
    #clean aliases
    for i in range(len(entity['aliases'])):
        name=entity['aliases'][i]
        matchedTitle=compiledTitle.match(name)
        if matchedTitle:
            t=matchedTitle.group(0)
            name=name.replace(t,'')
            
        title=''
        m=compiledWord.match(name)
    
        if m:
            title=m.group(0)
            name=name.replace(title,'')
            
        entity['aliases'][i]=name.strip()
        
        if not (title.strip()=='' or (title in titles)):
            titles.append(title)

        
    if titles and not entity.get('title'):
            entity['title']=[]
    
    for title in titles:
        entity['title'].append({'text':title,'articleIds':[entity['articleIds'][0]]})

def extractPersonEntity(article,matchedEntity,entityJson):
    modifyName(entityJson)
    
    articleText=article['text']
    articleId=article['_id']
 
    enText=entityJson['stdName'] #assumption that the stdName contains the name extracted from the first line and cleaned by modifyName method
    enText=enText.replace('(','')
    enText=enText.replace(')','')
    
    #add associated entities with count    
    assEn=filter(lambda x:x['type'] in ['Person','Company','Organization'] and x['name']!=matchedEntity['name'],article['entities'])
    shrtAssEn=[{'text':a['name'],'count':1} for a in assEn]
    entityJson['associatedEntities']=shrtAssEn

    #title
    leftTitlePattern='(([A-Z]([\w\.\-]*)(\s+))((([A-Z]([\w\.\-(),]*))|(for|of|the|and|an|a))(\s+))*?)(,\s+)?'+enText
    rightPattern=enText+',(([\s]+)([A-Z]([\w\.]*)(\s+))((([A-Z]([\w\.\-,]*))|(for|of|the|and|an|at|a))(\s*))+)'
    weakLeftPattern='(said\s+([\w,\-().]+\s+){1,5}'+enText+')'
    tenWords='([\w,\-.()]+\s+){0,5}'+enText+'\s*([\w,\-.()]+\s*){0,5}'
   
    m = re.search(leftTitlePattern,articleText) or re.search(rightPattern,articleText) or re.search(weakLeftPattern,articleText) or re.search(tenWords,articleText)
    
    title=m.group(0).strip() if m else None
    if title:
        if not entityJson.get('title'):
            entityJson['title']=[]
                
        tFound=False
        for t in entityJson.get('title'):
            if title == t['text'].strip():
                tFound=True
                break
        
        if not tFound:
            entityJson['title'].append({
                                  'text':title,
                                  'articleIds':[articleId]
                                  }
                                )           
    return entityJson
    
def save(entityJson):
    #print("Saving...")
    collection = db['unresolvedCollection_Weekly']
    collection.insert_one(entityJson)

def extract():
    global entityCount
    collection = db1['articles']
    cursor=collection.find({'$and':[{'entities':{'$exists':True}},{'publishedDate':{'$gte':'2019-12-31'}}]})
    #cursor=collection.find({'$and':[{'entities':{'$exists':True}},{'extracted':{'$exists':False}}]})
    for article in cursor:
        entities=article['entities']
        articleId=article['_id']
        #print articleId
        try:
            for matchedEntity in entities:
                if matchedEntity['type'] not in ['Person','City','Country','Continent','Company','Organization','ProvinceOrState']:
                    continue
                #print 'processing: ',matchedEntity['name']
                entityJson={
                            'stdName':matchedEntity['name'],
                            'type':matchedEntity['type'],
                            'aliases':matchedEntity['aliases'],
                            'articleIds':[articleId],
                            'resolved':False
                            }
                if matchedEntity.get('resolutions') and matchedEntity['type']!='Person':
                    entityJson['resolutions']=matchedEntity['resolutions']

                if matchedEntity['type']=='Person':
                    extractPersonEntity(article,matchedEntity,entityJson)
                save(entityJson)
		entityCount = entityCount + 1
        except Exception:
	    #traceback.print_exc()
            print("Some error!!\n\n")
            continue
        collection.update_one({'_id':articleId},{'$set':{'extracted':True}})
    cursor.close()                
          
if __name__=='__main__':
    # while(1):
    import datetime
    today = datetime.date.today()
    timenow = datetime.datetime.now()
    print("Started on ")
    print(today)
    print(timenow)
    extract()
    print("entities extracted:"+str(entityCount))
    
    client1.close()
    client.close()             
                
