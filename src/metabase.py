import jwt 
import time 
from fastapi import APIRouter ,Depends ,HTTPException 
from src .auth import get_current_user 
from src .db import get_db 
from src .config import settings 
router =APIRouter ()
DASHBOARD_TITLES ={1 :'Leads Overview',2 :'Campaigns & Sources',3 :'System & Imports'}

async def get_user_dashboard_ids (user_role :str ,db )->list [int ]:
    result =await db .execute ('SELECT dashboard_ids FROM dashboard_permissions WHERE role = $1',user_role )
    row =result .fetchone ()
    if not row :
        raise HTTPException (status_code =403 ,detail ='No dashboards configured for your role')
    return row ['dashboard_ids']

def generate_metabase_token (dashboard_id :int )->str :
    payload ={'resource':{'dashboard':dashboard_id },'params':{},'exp':round (time .time ())+600 }
    return jwt .encode (payload ,settings .METABASE_EMBEDDING_SECRET_KEY ,algorithm ='HS256')

@router .get ('/api/v1/metabase/dashboards')
async def get_available_dashboards (user =Depends (get_current_user ),db =Depends (get_db )):
    dashboard_ids =await get_user_dashboard_ids (user .role ,db )
    dashboards =[]
    for dashboard_id in dashboard_ids :
        token =generate_metabase_token (dashboard_id )
        url =f'{settings .METABASE_SITE_URL }/embed/dashboard/{token }#bordered=false&titled=false'
        dashboards .append ({'id':dashboard_id ,'title':DASHBOARD_TITLES .get (dashboard_id ,f'Dashboard {dashboard_id }'),'url':url })
    return {'dashboards':dashboards }
