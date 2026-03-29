import os 
import json 
from datetime import datetime 
from sqlalchemy import Column ,Integer ,String ,Boolean ,DateTime ,JSON ,func ,text 
from sqlalchemy .ext .declarative import declarative_base 
from sqlalchemy .ext .asyncio import create_async_engine ,AsyncSession 
from sqlalchemy .orm import sessionmaker 
from sqlalchemy .dialects .postgresql import insert as pg_insert ,JSONB 
from sqlalchemy import Column ,Integer ,String ,Boolean ,DateTime ,JSON ,func ,text ,Index 
import logging 
logger =logging .getLogger ('leads_importer.db')
Base =declarative_base ()

class Lead (Base ):
    __tablename__ ='leads'
    id =Column (Integer ,primary_key =True )
    email =Column (String ,unique =True ,index =True ,nullable =False )
    phone =Column (String ,index =True )
    first_name =Column (String )
    last_name =Column (String )
    country_iso2 =Column (String (2 ))
    nationality =Column (String (100 ))
    city =Column (String )
    language =Column (String )
    source =Column (String )
    status =Column (String ,default ='new')
    is_buyer =Column (Boolean ,default =False )
    tags =Column (JSONB ,default =list )
    meta_info =Column ('metadata',JSONB ,default =dict )
    created_at =Column (DateTime ,default =datetime .utcnow )
    updated_at =Column (DateTime ,default =datetime .utcnow ,onupdate =datetime .utcnow )
idx_leads_metadata_gin =Index ('ix_leads_metadata_gin',Lead .meta_info ,postgresql_using ='gin')

class ImportLog (Base ):
    __tablename__ ='import_logs'
    id =Column (Integer ,primary_key =True )
    filename =Column (String )
    source =Column (String )
    total_rows =Column (Integer )
    inserted =Column (Integer )
    updated =Column (Integer )
    skipped =Column (Integer )
    errors =Column (JSON ,default =list )
    created_at =Column (DateTime ,default =datetime .utcnow )

class DashboardPermission (Base ):
    __tablename__ ='dashboard_permissions'
    id =Column (Integer ,primary_key =True )
    role =Column (String )
    dashboard_id =Column (Integer )

class LeadSourceMapping (Base ):
    __tablename__ ='lead_source_mappings'
    id =Column (Integer ,primary_key =True )
    source_name =Column (String )
    field_name =Column (String )
    file_column_name =Column (String )

class SourcePriority (Base ):
    __tablename__ ='source_priorities'
    id =Column (Integer ,primary_key =True )
    source_name =Column (String ,unique =True )
    priority =Column (Integer ,default =0 )
DATABASE_URL =os .getenv ('DATABASE_URL','postgresql+asyncpg://postgres:postgres@localhost:5432/leads')
engine =create_async_engine (DATABASE_URL )
AsyncSessionLocal =sessionmaker (engine ,class_ =AsyncSession ,expire_on_commit =False )

async def upsert_leads_batch (session :AsyncSession ,leads_data :list [dict ],filename :str ):
    """High-performance UPSERT using PostgreSQL ON CONFLICT DO UPDATE."""
    if not leads_data :
        return (0 ,0 ,0 )
    unique_batch ={}
    now_iso =datetime .utcnow ().isoformat ()
    for l in leads_data :
        email =l .get ('email','').lower ()
        if not email :
            continue 
        raw_row ={k :v for k ,v in l .items ()if k not in ('metadata','meta_info','email','processed_metadata')}
        history_entry ={'file':filename ,'imported_at':now_iso ,'raw_row':raw_row }
        meta =dict (l .get ('metadata',l .get ('meta_info',{})))
        if 'import_history'not in meta :
            meta ['import_history']=[]
        meta ['import_history'].append (history_entry )
        l ['processed_metadata']=meta 
        l ['email']=email 
        unique_batch [email ]=l 
    clean_leads =list (unique_batch .values ())
    if not clean_leads :
        return (0 ,0 ,0 )
    BATCH_SIZE =200 
    total_ok =0 
    total_skip =0 
    for i in range (0 ,len (clean_leads ),BATCH_SIZE ):
        chunk =clean_leads [i :i +BATCH_SIZE ]
        values =[{'email':d ['email'],'phone':d .get ('phone'),'first_name':d .get ('first_name'),'last_name':d .get ('last_name'),'country_iso2':d .get ('country_iso2'),'nationality':d .get ('nationality'),'city':d .get ('city'),'language':d .get ('language'),'source':d .get ('source'),'status':d .get ('status','new'),'is_buyer':d .get ('is_buyer',False ),'tags':d .get ('tags',[]),'meta_info':d .get ('processed_metadata',{}),'updated_at':datetime .utcnow ()}for d in chunk ]
        stmt =pg_insert (Lead ).values (values )
        update_stmt =stmt .on_conflict_do_update (index_elements =['email'],set_ ={'phone':func .coalesce (stmt .excluded .phone ,Lead .phone ),'first_name':func .coalesce (stmt .excluded .first_name ,Lead .first_name ),'last_name':func .coalesce (stmt .excluded .last_name ,Lead .last_name ),'country_iso2':func .coalesce (stmt .excluded .country_iso2 ,Lead .country_iso2 ),'nationality':func .coalesce (stmt .excluded .nationality ,Lead .nationality ),'city':func .coalesce (stmt .excluded .city ,Lead .city ),'is_buyer':Lead .is_buyer |stmt .excluded .is_buyer ,'updated_at':datetime .utcnow (),'metadata':Lead .meta_info .op ('||')(stmt .excluded ['metadata']),'tags':Lead .tags .op ('||')(stmt .excluded .tags )})
        try :
            await session .execute (update_stmt )
            await session .commit ()
            total_ok +=len (chunk )
        except Exception as e :
            await session .rollback ()
            logger .error (f'Batch upsert failed (chunk {i }-{i +len (chunk )}): {e }')
            total_skip +=len (chunk )
    return (total_ok ,0 ,total_skip )

class DigestRecipient (Base ):
    __tablename__ ='digest_recipients'
    id =Column (Integer ,primary_key =True )
    name =Column (String ,nullable =False )
    email =Column (String ,unique =True ,nullable =False )
    is_active =Column (Boolean ,default =True )
    created_at =Column (DateTime ,default =datetime .utcnow )

async def get_weekly_stats (session :AsyncSession ):
    now =datetime .utcnow ()
    seven_days_ago =now -timedelta (days =7 )
    total_leads =(await session .execute (select (func .count (Lead .id )))).scalar ()or 0 
    new_this_week =(await session .execute (select (func .count (Lead .id )).where (Lead .created_at >=seven_days_ago ))).scalar ()or 0 
    updated_this_week =(await session .execute (select (func .count (Lead .id )).where (Lead .updated_at >=seven_days_ago ,Lead .created_at <seven_days_ago ))).scalar ()or 0 

    def get_pct (count ):
        return round (count /total_leads *100 ,1 )if total_leads >0 else 0 
    has_phone =(await session .execute (select (func .count (Lead .id )).where (Lead .phone !=None ,Lead .phone !=''))).scalar ()or 0 
    has_name =(await session .execute (select (func .count (Lead .id )).where (Lead .first_name !=None ))).scalar ()or 0 
    has_country =(await session .execute (select (func .count (Lead .id )).where (Lead .country_iso2 !=None ))).scalar ()or 0 
    is_buyer =(await session .execute (select (func .count (Lead .id )).where (Lead .is_buyer ==True ))).scalar ()or 0 
    top_c_res =await session .execute (select (Lead .country_iso2 ,func .count (Lead .id )).group_by (Lead .country_iso2 ).order_by (func .count (Lead .id ).desc ()).limit (5 ))
    top_countries =top_c_res .fetchall ()
    top_s_res =await session .execute (select (Lead .source ,func .count (Lead .id )).where (Lead .created_at >=seven_days_ago ).group_by (Lead .source ).order_by (func .count (Lead .id ).desc ()).limit (3 ))
    top_sources =top_s_res .fetchall ()
    imports_res =await session .execute (select (func .count (ImportLog .id )).where (ImportLog .created_at >=seven_days_ago ))
    processed =imports_res .scalar ()or 0 
    return {'total_leads':total_leads ,'new_this_week':new_this_week ,'updated_this_week':updated_this_week ,'data_quality':{'phone_pct':get_pct (has_phone ),'phone_count':has_phone ,'name_pct':get_pct (has_name ),'name_count':has_name ,'country_pct':get_pct (has_country ),'country_count':has_country ,'buyer_pct':get_pct (is_buyer ),'buyer_count':is_buyer },'top_countries':[{'name':r [0 ]or 'Unknown','count':r [1 ]}for r in top_countries ],'top_sources':[{'name':r [0 ]or 'Organic','count':r [1 ]}for r in top_sources ],'imports':{'processed':processed ,'success':processed ,'failed':0 }}

async def create_import_log (filename :str ,source :str ,total :int ,ins :int ,upd :int ,skp :int )->int :
    async with AsyncSessionLocal ()as session :
        log =ImportLog (filename =filename ,source =source ,total_rows =total ,inserted =ins ,updated =upd ,skipped =skp )
        session .add (log )
        await session .commit ()
        await session .refresh (log )
        return log .id 
