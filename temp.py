from pymongo import MongoClient
import re  

#client = MongoClient(config.mongoConfigs['host'], config.mongoConfigs['port'], j=True)
#db = client[config.mongoConfigs['db']] 

client = MongoClient('mongodb://10.237.26.159:27017/')
db = client['media-db'] 

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
    print("Saving...")
    collection = db['tech_articles_unresolved_entities']
    collection.insert_one(entityJson)

def extract():
    collection = db['articles']
    cursor=collection.find({'$and':[{'entities':{'$exists':True}},{'extracted':{'$exists':False}}]}).count()
    #cursor=collection.find({'entities':{'$exists':True}})#.limit(1000)
    print(cursor)
          
if __name__=='__main__':
    # while(1):
    extract() 
    
                
