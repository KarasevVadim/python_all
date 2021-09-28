#!/usr/bin/env python
# coding: utf-8

# pip install pymssql 
# 
# 

# In[1]:


import pandas as pd
import pymssql 


# In[2]:


server="SQL-retail2.nikamed.local"
base="Retail2_shops"


# In[31]:


sql="""
with mag_iskl as (SELECT CAST(_IDRRef AS uniqueidentifier) AS Ссылка,
_IDRRef, 
_Code AS Код
FROM _Reference10
where _Code='0-002')

SELECT 
cast(dateadd(year, -2000, i._fld10076) as date) AS date,
sum(_fld10077) AS Трафик,
cast( r._Fld2907RRef as uniqueidentifier) AS cсМагазин

FROM _InfoRg10074 i 


left outer JOIN _reference142 r ON i._Fld10075RRef = r._IDRRef
left outer join mag_iskl mi on r._Fld2907RRef=mi._IDRRef

WHERE i._fld10076 > '08.01.4021'
AND _fld10077 > 0
and mi._IDRRef is null
group by cast(dateadd(year, -2000, i._fld10076) as date) , cast( r._Fld2907RRef as uniqueidentifier)
"""


# In[32]:


def read_sql(sql,base, serv):
    #with pymssql.connect(server=serv ,database = base ,charset =  'cp1251') as  conn:
    with pymssql.connect(server=serv ,charset =  'cp1251',database = base ) as  conn:                  
    
        cursor = conn.cursor()  
        df = pd.read_sql( sql,conn)
    return df


# conn.close()

# In[33]:


df_tr=read_sql(sql,base, server)


# In[34]:


sum(df_tr['Трафик'])


# In[35]:


df_tr


# In[63]:


sql="""



select top(1000)
       cast(dateadd(year, -2000, d21._date_time) as date) as 'Дата' , 
         
       
       iif(e._EnumOrder=1,'ВозвратДВД','Продажи') as 'ВидОперации'
       ,iif(e._EnumOrder=1, -d21t._Fld6384, d21t._Fld6384 ) as 'Количество'
       ,iif(e._EnumOrder=1, -d21t._Fld6397, d21t._Fld6397) as 'Сумма'
        
              , cast(r10._idrref as uniqueidentifier) as 'ccМагазин'
        
       , iif(r2_10._Code is null, cast(r10._idrref as uniqueidentifier), 
                                  cast(r2_10._idrref as uniqueidentifier) ) as 'ccМагазинМотивационный'
        

from _Document21_VT6378 d21t

left outer join _Document21 d21 on d21t._Document21_IDRRef = d21._IDRRef /*исх чек*/

LEFT OUTER JOIN _reference47 r47 ON d21._Fld6331RRef = r47._idrref /*вид документа*/
LEFT OUTER JOIN _Enum352 e ON d21._Fld6333RRef = e._idrref /*вид документа в перечислениях чека ккм - 0-продажа, 1-возврат*/

left outer join _Reference10 r10 on d21._Fld6339RRef = r10._idrref /*shop*/


left outer join  _Document21 d2_21 on d21._Fld6353RRef=d2_21._idrref /*поиск мотив маг*/
left outer join _Reference10 r2_10 on d2_21._Fld6339RRef = r2_10._idrref /*shop*/

where d21._marked = 0 and d21._Posted = 1
and d21._date_time >= '08.01.4021' 


union all

select top(1000)
       cast(dateadd(year, -2000, d._date_time) as date) as 'Дата', 
       
        N'Возврат'
       , -dt._Fld3984 as 'Количество'
       , -dt._Fld3988 as 'СуммаЧека'
       
       , cast(r10._idrref as uniqueidentifier) as 'ccМагазин'
        
       , iif(r2_10._Code is null, cast(r10._idrref as uniqueidentifier), cast(r2_10._idrref as uniqueidentifier) ) as 'ccМагазинМотивационный'
        

from _Document216_VT3980 dt

left outer join _Document216 d on dt._Document216_IDRRef = d._IDRRef /*исх возвратный чек*/

left outer join _Reference10 r10 on d._Fld3950RRef = r10._idrref /*shop*/
left outer join  _Reference172 r172  on dt._Fld4001RRef = r172._idrref
left outer join  _Reference23 r23  on dt._Fld3982RRef = r23._idrref


left outer join _Reference131 r131 on d._Fld11137RRef= r131._idrref --  склады
left outer join _Reference10 r2_10 on r131._Fld2686RRef = r2_10._idrref /*shop к складу*/

where d._marked = 0 and d._Posted = 1
and d._date_time >= '08.01.4021'


"""


# In[64]:


def read_sql(sql,base, serv):
    #with pymssql.connect(server=serv ,database = base ,charset =  'cp1251') as  conn:
    with pymssql.connect(server=serv ,database = base ) as  conn:                  
    
        cursor = conn.cursor()  
        df = pd.read_sql( sql,conn)
    return df


# In[65]:


get_ipython().run_cell_magic('time', '', 'df=read_sql(sql,base, server)')


# In[66]:


sum(df['Сумма'])


# In[67]:


df


# In[ ]:



