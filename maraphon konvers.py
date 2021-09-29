#!/usr/bin/env python
# coding: utf-8

# pip install pymssql 
# 
# 

# In[1]:


import pandas as pd
import pymssql 


# In[273]:


server="SQL-retail2.nikamed.local"
base="Retail2_shops"


# In[488]:


sql="""
with mag_iskl as (
SELECT CAST(_IDRRef AS uniqueidentifier) AS Ссылка, --исключение техн склада
_IDRRef, 
_Code AS Код
FROM _Reference10
where _Code='0-002'
),
--------------------------------------исх продажи-----------------------
sales_ish as (
select 
       cast(dateadd(year, -2000, d21._date_time) as date) as Дата , 
       cast(d21._number as varchar(20))+'_'+ cast(dateadd(year, -2000, d21._date_time) as varchar(50)) as  'НомерЧекаККМ',  
       iif(e._EnumOrder=1,-1,1) as 'ВидОперации'
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
select 
       cast(dateadd(year, -2000, d._date_time) as date) as 'Дата', 
        cast(d._number as varchar(20))+'_'+ cast(dateadd(year, -2000, d._date_time) as varchar(50)) as  'НомерЧекаККМ',      
        -1
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
),
-------------суммарные продажи------------------------------------

sales_sum2 as(
SELECT 

Дата as date_ss, 
sum(Сумма) as Summa, 
ccМагазин, 
ccМагазинМотивационный,
ВидОперации 

FROM sales_ish
group by Дата,  ccМагазин,ccМагазинМотивационный,ВидОперации 
),
-------------c кол чеков------------------------------------
sales_sum as(
SELECT  
        date_ss, 

        ccМагазин, 
        ccМагазинМотивационный,
        sum(Summa) as Summa, 
        sum(ВидОперации) as Kol4
        
FROM sales_sum2

group by date_ss,  ccМагазин, ccМагазинМотивационный
)



SELECT 
cast(dateadd(year, -2000, i._fld10076) as date) AS date_ii,
sum(_fld10077) AS Трафик,
cast( r._Fld2907RRef as uniqueidentifier) AS cсМагазин,
sum(sales_sum.Summa) as Summa_ii,
sum(Kol4) as Kol4
FROM _InfoRg10074 i 


left outer JOIN _reference142 r ON i._Fld10075RRef = r._IDRRef
left outer join mag_iskl mi on r._Fld2907RRef=mi._IDRRef
left outer join sales_sum  on sales_sum.date_ss=(cast(dateadd(year, -2000, i._fld10076) as date)) 
        and sales_sum.ccМагазин=cast( r._Fld2907RRef as uniqueidentifier)

WHERE i._fld10076 > '08.01.4021'
AND _fld10077 > 0
and mi._IDRRef is null
group by cast(dateadd(year, -2000, i._fld10076) as date) , cast( r._Fld2907RRef as uniqueidentifier)
"""


# In[489]:


def read_sql(sql,base, serv):
    #with pymssql.connect(server=serv ,database = base ,charset =  'cp1251') as  conn:
    with pymssql.connect(server=serv ,charset =  'cp1251',database = base ) as  conn:                  
    
        cursor = conn.cursor()  
        df = pd.read_sql( sql,conn)
    return df


# In[490]:


get_ipython().run_cell_magic('time', '', 'df_tr=read_sql(sql,base, server)')


# In[491]:


sum(df_tr['Трафик'].head())


# In[492]:


sum(df_tr['Kol4'].head(1000))


# In[493]:


df_tr.info()


# conn.close()

# In[95]:


get_ipython().run_cell_magic('time', '', 'df_tr=read_sql(sql,base, server)')


# In[96]:


sum(df_tr['Трафик'])


# In[458]:


df_tr.head()


# In[97]:


df_tr


# In[460]:


sql = """
with sales_ish as (
select 
       cast(dateadd(year, -2000, d21._date_time) as date) as Дата , 
       cast(d21._number as varchar(20))+'_'+ cast(dateadd(year, -2000, d21._date_time) as varchar(50)) as  'НомерЧекаККМ',  
       iif(e._EnumOrder=1,-1,1) as 'ВидОперации'
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
select 
       cast(dateadd(year, -2000, d._date_time) as date) as 'Дата', 
        cast(d._number as varchar(20))+'_'+ cast(dateadd(year, -2000, d._date_time) as varchar(50)) as  'НомерЧекаККМ',      
        -1
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
),
-------------суммарные продажи------------------------------------

sales_sum2 as(
SELECT Дата as date_ss, sum(Сумма) as Summa, ccМагазин, ccМагазинМотивационный,ВидОперации FROM sales_ish
group by Дата,  ccМагазин,ccМагазинМотивационный,ВидОперации 
)
-------------кол чеков------------------------------------
SELECT  date_ss, sum(Summa) as Summa, 
        ccМагазин, 
        ccМагазинМотивационный,
        sum(ВидОперации) as Kol4
        
FROM sales_sum2

group by date_ss,  ccМагазин,ccМагазинМотивационный

"""


# In[461]:


def read_sql(sql,base, serv):
    #with pymssql.connect(server=serv ,database = base ,charset =  'cp1251') as  conn:
    with pymssql.connect(server=serv ,database = base,charset =  'cp1251' ) as  conn:                  
    
        cursor = conn.cursor()  
        df = pd.read_sql( sql,conn)
    return df


# In[462]:


get_ipython().run_cell_magic('time', '', 'df=read_sql(sql,base, server)')


# In[466]:


df.info()


# In[463]:


sum(df['Summa'])


# In[464]:


df


# In[465]:


sum(df['Kol4'])


# In[ ]:


df


# In[ ]:




