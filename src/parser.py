import pathlib as ph 
import yaml 
import os 
import pandas as pd 
import logging 
logger =logging .getLogger ('leads_importer.parser')

def get_mappings_from_yaml ()->dict :
    yaml_path =os .path .join (os .path .dirname (__file__ ),'..','config','column_mappings.yaml')
    if not os .path .exists (yaml_path ):
        return {}
    with open (yaml_path ,'r',encoding ='utf-8')as f :
        config =yaml .safe_load (f )
    mappings ={}
    if config :
        for std ,syns in config .items ():
            if syns :
                for s in syns :
                    mappings [str (s ).strip ().lower ()]=std 
    return mappings 

def get_standard_column_name (raw_name ,mappings ):
    if not raw_name :
        return None 
    clean =str (raw_name ).strip ().lower ()
    return mappings .get (clean )

async def stream_process_file (file_path :str ,source_name :str ):
    path =ph .Path (file_path )
    mappings =get_mappings_from_yaml ()
    ext =path .suffix .lower ()
    implicit_country =None 
    implicit_city =None 
    parts =[str (p ).strip ()for p in path .parts ]
    lower_parts =[p .lower ()for p in parts ]

    try :
        if 'mailchimp'in lower_parts :
            idx =lower_parts .index ('mailchimp')


            if len (lower_parts )>idx +1 :
                country_part =lower_parts [idx +1 ]
                if 'usa'in country_part :implicit_country ='US'
                elif 'canada'in country_part :implicit_country ='CA'
                elif 'europe'in country_part :implicit_country ='GB'
                elif 'middle'in country_part or 'dubai'in country_part :implicit_country ='AE'
                elif 'australia'in country_part or 'oceania'in country_part :implicit_country ='AU'
                elif 'asia'in country_part :implicit_country ='PH'



            if len (parts )>idx +2 :

                potential_city =parts [idx +2 ]


                if potential_city ==path .name :


                    potential_city =parts [idx +1 ]


                if not any (x in potential_city .lower ()for x in ['(1)','(2)','(3)','(4)','(5)','(6)','master','mailchimp','total']):
                    implicit_city =potential_city 
    except Exception as e :
        logger .warning (f"Path parsing failed for {file_path }: {e }")
    df =None 
    if ext in {'.xlsx','.xls'}:
        try :
            df =pd .read_excel (file_path ,dtype =str )
        except Exception as e :
            logger .error (f'Excel read failed: {e }')
            raise ValueError (f'Invalid Excel file: {e }')
    elif ext =='.csv':
        encodings =['utf-8','windows-1251','latin-1']
        for enc in encodings :
            try :
                df =pd .read_csv (file_path ,sep =None ,engine ='python',encoding =enc ,dtype =str )
                logger .info (f'Successfully read CSV with {enc }')
                break 
            except (UnicodeDecodeError ,pd .errors .ParserError ):
                continue 
        if df is None :
            raise ValueError ('CSV failed to load with supported encodings (utf-8, windows-1251, latin-1)')
    else :
        raise ValueError (f'Unsupported file format: {ext }')
    if df is None or df .empty :
        return 
    df =df .fillna ('')
    has_email_col =False 
    for col in df .columns :
        col_str =str (col ).lower ()
        if 'email'in col_str or 'mail'in col_str :
            has_email_col =True 
            break 
    if not has_email_col :
        for _ ,row in df .head (10 ).iterrows ():
            for val in row :
                v_str =str (val ).strip ()
                if '@'in v_str and '.'in v_str :
                    has_email_col =True 
                    break 
            if has_email_col :
                break 
    if not has_email_col :
        raise ValueError ('Critical: No email column detected in file.')
    for _ ,series in df .iterrows ():
        row_dict =series .to_dict ()
        email =None 
        for k ,v in row_dict .items ():
            val_str =str (v ).strip ()
            if val_str and '@'in val_str and ('.'in val_str ):
                email =val_str 
                break 
        if not email :
            yield {'_error':'No email found in row'}
            continue 
        res ={'email':email }
        meta ={}
        for k ,v in row_dict .items ():
            val_clean =str (v ).strip ()
            if val_clean ==email or val_clean =='':
                continue 
            std =get_standard_column_name (k ,mappings )
            if std :
                res [std ]=val_clean 
            else :
                meta [str (k )]=val_clean 
        res ['metadata']=meta 
        res ['source']=path .name 
        if not res .get ('city')and implicit_city :
            res ['city']=implicit_city 
        if not res .get ('country_iso2')and implicit_country :
            res ['country_iso2']=implicit_country 
        yield res 
