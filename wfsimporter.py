import geopandas as gpd
from requests import Request
import requests
from owslib.wfs import WebFeatureService
from xml.etree import ElementTree
import  xml.dom.minidom
import math
from sqlalchemy import create_engine 
from sqlalchemy.ext.declarative import declarative_base  
from geoalchemy2 import Geometry
from shapely.geometry import shape
import pandas as pd

# URL for WFS backend
url = "wfs adress"
db_string = "postgres://postgres:password@host:port/databasename"
db = create_engine(db_string)  
base = declarative_base()
con = db.connect()

# Initialize
wfs = WebFeatureService(url=url)

def getCount(layer):
    #WFS request parametreleri
    params_info = dict(service='WFS', version="2.0.0", request='GetFeature',
    typeName=layer, resulttype='hits') 
    #Request Urli oluşturma
    i = Request('GET', url, params=params_info).prepare().url
    #request 
    wfs_get = requests.get(i)
    #Servisten Dönen XML Parsel ediliyor
    xmlparse = xml.dom.minidom.parseString(wfs_get.text)
    #xml içerisinden wfs:FeatureCollection kısmı alınıyor
    access_points = xmlparse.getElementsByTagName('wfs:FeatureCollection')
    #wfs:FeatureCollection içerisindeki numberMatched aranıyor
    numberMatched=access_points[0].getAttribute('numberMatched')
    print(numberMatched)
    return numberMatched
   
def getData(layer,startIndex):
    #WFS request parametreleri
    params_data = dict(service='WFS', version="2.0.0", request='GetFeature',
        typeName=layer, startIndex=startIndex , count=1000)
    #Request Urli oluşturma
    q = Request('GET', url, params=params_data).prepare().url
    #verileri pandas ile tablo tipini çevirme
    data = gpd.read_file(q) 
    return data
    
def dbinsertdata(data,layer,srid):
     tablename=layer.split(':')
     print(data)
     geodataframe = gpd.GeoDataFrame(pd.DataFrame(data)) 
     print(geodataframe)
     geodataframe.set_crs(epsg=srid, inplace=True)    
     geodataframe.to_postgis(tablename[1], db, if_exists='append', index=False,dtype={  "geometry":Geometry(geometry_type='GEOMETRY', spatial_index=True, srid=srid)})
    
    

## WFS te bulunan katmanlarda sırayla geziyor.
for l in wfs.contents:
#her bir content aslında layername bu sebeple l=layer
    layer = l
    #ilk önce ilgili layerın içerisinde ne kadar veri olduğuna bakılır 
    count = getCount(layer) 
    #1000 er 1000 er veriler çekileceği için 1000 e bölünür ve yukarı yuvarlanır.
    count_range = math.ceil(int(count)/1000)
    #indexlere göre teker teker verilere istek atar  
    appended_data = []     
    for i in range(count_range):
        index= i*1000
        data = getData(layer,index)    
        appended_data.append(data)                             
    alldata = pd.concat(appended_data)
    dbinsertdata(alldata,layer,4326)
    
    
   
    
     
    