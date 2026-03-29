import asyncio 
from datetime import datetime ,timedelta 
from sqlalchemy import select ,func 
from src .db import AsyncSessionLocal ,Lead ,ImportLog 
from src .notifier import send_telegram_message 

async def generate_weekly_report ():
    one_week_ago =datetime .utcnow ()-timedelta (days =7 )
    async with AsyncSessionLocal ()as session :
        stmt_new =select (func .count (Lead .id )).where (Lead .created_at >=one_week_ago )
        res_new =await session .execute (stmt_new )
        count_new =res_new .scalar ()
        stmt_sources =select (Lead .source ,func .count (Lead .id )).where (Lead .created_at >=one_week_ago ).group_by (Lead .source )
        res_sources =await session .execute (stmt_sources )
        sources_data =res_sources .all ()
        stmt_imports =select (func .count (ImportLog .id )).where (ImportLog .imported_at >=one_week_ago )
        res_imports =await session .execute (stmt_imports )
        count_imports =res_imports .scalar ()
        report_msg =f"Weekly leads report\nPeriod: {one_week_ago .strftime ('%d.%m')} - {datetime .utcnow ().strftime ('%d.%m')}\n\nNew leads: {count_new }\nTotal imports: {count_imports }\n\nBy source:\n"
        for source ,count in sources_data :
            report_msg +=f"- {source or 'Unknown'}: {count }\n"
        await send_telegram_message (report_msg )
        print ('Weekly report sent to Telegram.')
if __name__ =='__main__':
    asyncio .run (generate_weekly_report ())
