import logging 
import os 
import shutil 
import time 
from datetime import datetime ,timedelta 
from typing import List ,Union 
import jwt 
import pandas as pd 
from fastapi import BackgroundTasks ,Depends ,FastAPI ,File ,HTTPException ,Query ,UploadFile ,status 
from fastapi .middleware .cors import CORSMiddleware 
from fastapi .security import APIKeyHeader 
from pydantic import BaseModel 
from sqlalchemy import select ,func 
from .config import settings 
from .db import AsyncSessionLocal ,Lead ,upsert_leads_batch ,create_import_log 
from .normalizer import normalize_city ,normalize_country ,normalize_email ,normalize_nationality ,normalize_phone 
logger =logging .getLogger ('leads_importer')
app =FastAPI (title ='Leads Importer API')
app .add_middleware (CORSMiddleware ,allow_origins =['*'],allow_credentials =True ,allow_methods =['*'],allow_headers =['*'])
api_key_header =APIKeyHeader (name ='API-Key',auto_error =False )
API_KEY =os .getenv ('API_KEY','gmp79b9qSN}&JWX')
DASHBOARD_TITLES ={15 :'Leads Overview',16 :'Campaigns & Sources',17 :'System & Imports'}

async def get_api_key (api_key :str =Depends (api_key_header )):
    if api_key !=API_KEY :
        raise HTTPException (status_code =status .HTTP_401_UNAUTHORIZED ,detail ='Invalid API Key')

class LeadSchema (BaseModel ):
    email :str 
    phone :str =None 
    first_name :str =None 
    last_name :str =None 
    country :str =None 
    city :str =None 
    is_buyer :bool =False 
    tags :List [str ]=[]
    metadata :dict ={}

    class Config :
        extra ='allow'

@app .get ('/health')
async def health ():
    return {'status':'ok'}

@app .post ('/api/v1/import/upload')
async def upload_leads (background_tasks :BackgroundTasks ,file :UploadFile =File (...),source_name :str =Query (None ),api_key :str =Depends (get_api_key )):
    MAX_SIZE =100 *1024 *1024 
    file_size =0 
    try :
        file .file .seek (0 ,2 )
        file_size =file .file .tell ()
        file .file .seek (0 )
    except :
        pass 
    if file_size >MAX_SIZE :
        raise HTTPException (status_code =status .HTTP_413_REQUEST_ENTITY_TOO_LARGE ,detail =f'File too large ({file_size } bytes). Max limit is 100 MB.')
    with open (tmp_path ,'wb')as buffer :
        shutil .copyfileobj (file .file ,buffer )
    try :
        ext =os .path .splitext (file .filename )[1 ].lower ()
        if ext in ('.xlsx','.xls'):
            df_info =pd .read_excel (tmp_path ,usecols =[0 ])
            row_count =len (df_info )
        else :
            df_info =pd .read_csv (tmp_path ,sep =None ,engine ='python',usecols =[0 ],on_bad_lines ='skip')
            row_count =len (df_info )
    except Exception as e :
        logger .warning (f'Metadata read failed for {file .filename }, assuming small file: {e }')
        row_count =0 
    from .cli import run_import 
    from .notifier import notifier 
    if row_count <50000 :
        try :
            results =await run_import (tmp_path ,source_name )
            await notifier .send_import_summary (results )
            return results 
        finally :
            if os .path .exists (tmp_path ):
                os .remove (tmp_path )
    else :
        import_id =await create_import_log (file .filename ,source_name ,row_count ,0 ,0 ,0 )
        background_tasks .add_task (run_and_clean ,tmp_path ,source_name ,file .filename )
        return {'import_id':import_id ,'filename':file .filename ,'rows_total':row_count ,'rows_inserted':0 ,'rows_updated':0 ,'rows_skipped':0 ,'status':'processing','message':'File > 50k rows. Processing in background.'}

async def run_and_clean (file_path :str ,source_name :str ,original_filename :str ):
    try :
        from .cli import run_import 
        from .notifier import notifier 
        results =await run_import (file_path ,source_name )
        await notifier .send_import_summary (results )
    except Exception as exc :
        logger .error ('Background import failed for %s: %s',file_path ,exc )
    finally :
        if os .path .exists (file_path ):
            os .remove (file_path )

@app .post ('/api/v1/notify/weekly-digest')
async def trigger_weekly_digest (api_key :str =Depends (get_api_key )):
    """
    T3 Part 10: Triggered by n8n schedule.
    """
    from .notifier import notifier 
    await notifier .send_weekly_digest ()
    return {'status':'success','message':'Weekly digest process triggered.'}

@app .post ('/api/v1/import/json')
async def import_json (leads :Union [LeadSchema ,List [LeadSchema ]],source_name :str ='api_v1',api_key :str =Depends (get_api_key )):
    if not isinstance (leads ,list ):
        leads =[leads ]
    normalized =[]
    for lead in leads :
        lead_data =lead .model_dump ()if hasattr (lead ,'model_dump')else lead .dict ()
        email =normalize_email (lead_data .get ('email',''))
        if not email :
            continue 
        known_keys ={'email','phone','first_name','last_name','country','city','is_buyer','tags','metadata'}
        extra_metadata =lead_data .get ('metadata',{})
        for k ,v in lead_data .items ():
            if k not in known_keys :
                extra_metadata [k ]=v 
        normalized .append ({'email':email ,'phone':normalize_phone (lead_data .get ('phone','')),'first_name':lead_data .get ('first_name'),'last_name':lead_data .get ('last_name'),'country_iso2':normalize_country (lead_data .get ('country','')),'city':normalize_city (lead_data .get ('city',''),lead_data .get ('country','')),'is_buyer':lead_data .get ('is_buyer',False ),'tags':lead_data .get ('tags',[]),'metadata':extra_metadata ,'source':source_name ,'status':'new'})
    if not normalized :
        return {'status':'error','message':'No valid leads provided'}
    async with AsyncSessionLocal ()as session :
        ins ,upd ,skp =await upsert_leads_batch (session ,normalized ,source_name )
        return {'status':'success','inserted':ins ,'updated':upd ,'skipped':skp }

@app .post ('/api/v1/import/webhook')
async def import_webhook (leads :Union [LeadSchema ,List [LeadSchema ]],source_name :str ='n8n_webhook',api_key :str =Depends (get_api_key )):
    return await import_json (leads ,source_name ,api_key )

@app .get ('/api/v1/metabase/dashboards')
async def get_available_dashboards (user_role :str =Query ('viewer'),city :str =Query (None )):
    """Return dashboards available for a given role with signed embed URLs."""
    role_map ={'admin':[15 ,16 ,17 ],'manager':[15 ,16 ],'viewer':[15 ]}
    dashboard_ids =role_map .get (user_role .lower (),[])
    if not dashboard_ids :
        return {'dashboards':[]}
    dashboards =[]
    import jwt as pyjwt 
    for d_id in dashboard_ids :
        metabase_params ={}
        if city and str (city ).strip ():
            metabase_params ['city']=str (city ).strip ()
        payload ={'resource':{'dashboard':d_id },'params':metabase_params ,'exp':round (time .time ())+600 }
        token =jwt .encode (payload ,settings .METABASE_EMBEDDING_SECRET_KEY ,algorithm ='HS256')
        dashboards .append ({'id':d_id ,'title':DASHBOARD_TITLES .get (d_id ,f'Dashboard {d_id }'),'url':f'{settings .METABASE_SITE_URL }/embed/dashboard/{token }#bordered=false&titled=false&theme=night&locale=en'})
    return {'dashboards':dashboards }

@app .get ('/api/v1/leads/count')
async def get_count ():
    async with AsyncSessionLocal ()as session :
        result =await session .execute (select (func .count (Lead .id )))
        total =result .scalar ()
        return {'total_leads':total }

@app .get ('/api/v1/dashboard/metrics')
async def get_dashboard_metrics ():
    async with AsyncSessionLocal ()as session :
        res_total =await session .execute (select (func .count ()).select_from (Lead ))
        total_leads =res_total .scalar ()or 0 
        seven_days_ago =datetime .utcnow ()-timedelta (days =7 )
        res_7d =await session .execute (select (func .count ()).select_from (Lead ).where (Lead .created_at >=seven_days_ago ))
        leads_7d =res_7d .scalar ()or 0 
        res_markets =await session .execute (select (func .count (func .distinct (Lead .city ))).where (Lead .city !=None ))
        markets_count =res_markets .scalar ()or 0 
        res_completed =await session .execute (select (func .count ()).select_from (Lead ).where (Lead .status =='done'))
        completed =res_completed .scalar ()or 0 
        res_upcoming =await session .execute (select (func .count ()).select_from (Lead ).where (Lead .status !='done'))
        upcoming =res_upcoming .scalar ()or 0 
        return {'totalLeads':total_leads ,'leads7d':leads_7d ,'markets':markets_count ,'completed':completed ,'upcoming':upcoming }
from sqlalchemy import text 

@app .get ('/api/v1/dashboard/overview')
async def get_dashboard_overview ():
    async with AsyncSessionLocal ()as session :
        query =text ("\n            SELECT\n                COALESCE(NULLIF(TRIM(city), ''), 'Unknown') as city,\n                MAX(country_iso2) as country_iso2,\n                COUNT(id) as leads,\n                COUNT(id) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') as leads_7d,\n                MAX(status) as max_status\n            FROM leads\n            GROUP BY COALESCE(NULLIF(TRIM(city), ''), 'Unknown')\n            ORDER BY leads DESC\n        ")
        result =await session .execute (query )
        rows =result .fetchall ()
        REGION_MAP ={'US':'USA','GB':'Europe','AE':'Middle East','CA':'Canada','AU':'Oceania','PH':'Asia','EG':'Middle East','TR':'Europe','GR':'Europe','ID':'Asia','MT':'Europe','MK':'Europe','IE':'Europe','NZ':'Oceania','MY':'Asia','SE':'Europe','PL':'Europe','DK':'Europe','IT':'Europe','MN':'Asia','ZA':'Middle East','MU':'Middle East','FR':'Europe','JP':'Asia','PT':'Europe','RO':'Europe','ES':'Europe','DE':'Europe','NO':'Europe'}
        data =[]
        for idx ,r in enumerate (rows ):
            city =r .city or 'Unknown'
            country =r .country_iso2 or 'XX'
            region =REGION_MAP .get (country .upper (),'Other')
            status_val ='done'if r .max_status =='done'else 'upcoming'
            data .append ({'id':idx ,'city':city ,'country':country ,'region':region ,'leads':r .leads ,'leads_7d':r .leads_7d or 0 ,'ld':'','ld_label':'','days':0 ,'lpd':0 ,'status':status_val ,'notes':''})
        return data 

@app .get ('/api/v1/dashboard/sources')
async def get_dashboard_sources ():
    async with AsyncSessionLocal ()as session :
        query =text ("\n            SELECT\n                source,\n                COUNT(id) as total_leads,\n                COUNT(id) FILTER (WHERE is_buyer = TRUE) as total_buyers,\n                COUNT(id) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') as new_leads_7d\n            FROM leads\n            GROUP BY source\n            ORDER BY total_leads DESC\n        ")
        result =await session .execute (query )
        rows =result .fetchall ()
        data =[]
        for idx ,r in enumerate (rows ):
            source =r .source if r .source else 'Unknown/Organic'
            buyers =r .total_buyers or 0 
            leads =r .total_leads or 0 
            conversion =round (buyers /leads *100 ,2 )if leads >0 else 0 
            data .append ({'id':idx ,'source':source ,'total_leads':leads ,'new_leads_7d':r .new_leads_7d or 0 ,'buyers':buyers ,'conversion':conversion })
        return data 

@app .get ('/api/v1/dashboard/imports')
async def get_dashboard_imports ():
    async with AsyncSessionLocal ()as session :
        query =text ('\n            SELECT\n                id,\n                filename,\n                source,\n                total_rows,\n                inserted,\n                updated,\n                skipped,\n                created_at\n            FROM import_logs\n            ORDER BY created_at DESC\n            LIMIT 100\n        ')
        result =await session .execute (query )
        rows =result .fetchall ()
        data =[]
        for r in rows :
            data .append ({'id':r .id ,'filename':r .filename ,'source':r .source ,'total_rows':r .total_rows ,'inserted':r .inserted ,'updated':r .updated ,'skipped':r .skipped ,'created_at':r .created_at .strftime ('%Y-%m-%d %H:%M:%S')if r .created_at else ''})
        return data 
