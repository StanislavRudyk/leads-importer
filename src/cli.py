import asyncio 
import argparse 
import os 
from src .parser import stream_process_file 
from src .db import AsyncSessionLocal ,upsert_leads_batch ,create_import_log 
from src .normalizer import normalize_city ,normalize_country ,normalize_email ,normalize_nationality ,normalize_phone 

async def run_import (file_path :str ,source_name ='default')->dict :
    """Runs the full T3 import pipeline. Returns final metrics as a dict."""
    if not os .path .exists (file_path ):
        return {'status':'error','message':f'File {file_path } not found'}
    filename =os .path .basename (file_path )
    print (f' STARTING T3 PIPELINE IMPORT: {filename }')
    async with AsyncSessionLocal ()as session :
        batch =[]
        batch_size =500 
        total_inserted =0 
        total_updated =0 
        total_skipped =0 
        total_found =0 
        error_msg =None 
        try :
            async for raw_row in stream_process_file (file_path ,source_name ):
                if '_error'in raw_row :
                    total_skipped +=1 
                    total_found +=1 
                    continue 
                total_found +=1 
                email =normalize_email (raw_row .get ('email',''))
                if not email :
                    total_skipped +=1 
                    continue 
                normalized_row ={'email':email ,'phone':normalize_phone (raw_row .get ('phone')),'first_name':raw_row .get ('first_name'),'last_name':raw_row .get ('last_name'),'country_iso2':normalize_country (raw_row .get ('country',raw_row .get ('country_iso2'))),'nationality':normalize_nationality (raw_row .get ('nationality')),'city':normalize_city (raw_row .get ('city'),raw_row .get ('country',raw_row .get ('country_iso2',''))),'language':raw_row .get ('language'),'source':source_name ,'status':'new','is_buyer':str (raw_row .get ('is_buyer','')).lower ()in ('true','1','yes'),'tags':raw_row .get ('tags',[]),'metadata':raw_row .get ('metadata',{})}
                batch .append (normalized_row )
                if len (batch )>=batch_size :
                    ins ,upd ,skp =await upsert_leads_batch (session ,batch ,filename )
                    total_inserted +=ins 
                    total_updated +=upd 
                    total_skipped +=skp 
                    batch =[]
                    print (f'   📊 [Progress] Processed {total_found } rows...')
            if batch :
                ins ,upd ,skp =await upsert_leads_batch (session ,batch ,filename )
                total_inserted +=ins 
                total_updated +=upd 
                total_skipped +=skp 
        except ValueError as v_exc :
            print (f' CRITICAL ERROR: {v_exc }')
            error_msg =str (v_exc )
            import_id =await create_import_log (filename ,source_name ,total_found ,0 ,0 ,0 )
            return {'import_id':import_id ,'rows_total':0 ,'rows_inserted':0 ,'rows_updated':0 ,'rows_skipped':0 ,'status':'error','message':error_msg }
    print (f' T3 PIPELINE FINISHED: {filename }')
    import_id =await create_import_log (filename ,source_name ,total_found ,total_inserted ,total_updated ,total_skipped )
    results ={'import_id':import_id ,'filename':filename ,'rows_total':total_found ,'rows_inserted':total_inserted ,'rows_updated':total_updated ,'rows_skipped':total_skipped ,'status':'success'}
    from .notifier import notifier 
    await notifier .send_import_summary (results )
    return results 
if __name__ =='__main__':
    parser =argparse .ArgumentParser ()
    parser .add_argument ('--file',required =True )
    args =parser .parse_args ()
    asyncio .run (run_import (args .file ))
