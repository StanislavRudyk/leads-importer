import os 
import httpx 
import logging 
from datetime import datetime ,timedelta 
from typing import List ,Dict ,Any 
from jinja2 import Environment ,FileSystemLoader 
import aiosmtplib 
from email .message import EmailMessage 
from .db import AsyncSessionLocal ,DigestRecipient ,get_weekly_stats 
from sqlalchemy import select 
logger =logging .getLogger ('leads_importer.notifier')

class Notifier :

    def __init__ (self ):
        self .bot_token =os .getenv ('TELEGRAM_BOT_TOKEN')
        self .chat_id =os .getenv ('TELEGRAM_CHAT_ID')
        self .smtp_host =os .getenv ('SMTP_SERVER','smtp.gmail.com')
        self .smtp_port =int (os .getenv ('SMTP_PORT','587'))
        self .smtp_user =os .getenv ('SMTP_USER')
        self .smtp_pass =os .getenv ('SMTP_PASSWORD')
        template_dir =os .path .join (os .path .dirname (__file__ ),'..','templates')
        if not os .path .exists (template_dir ):
            os .makedirs (template_dir )
        self .jinja_env =Environment (loader =FileSystemLoader (template_dir ))

    async def send_telegram (self ,message :str ):
        if not self .bot_token or not self .chat_id :
            logger .warning ('Telegram credentials missing, skipping notification.')
            return 
        url =f'https://api.telegram.org/bot{self .bot_token }/sendMessage'
        payload ={'chat_id':self .chat_id ,'text':message ,'parse_mode':'HTML'}
        try :
            async with httpx .AsyncClient ()as client :
                await client .post (url ,json =payload ,timeout =10.0 )
        except Exception as e :
            logger .error (f'Failed to send Telegram message: {e }')

    async def send_import_summary (self ,stats :Dict [str ,Any ]):
        """
        Operational notification (T3 Part 10.1)
        """
        status_emoji =''if stats .get ('status')=='success'else ''
        msg =f"{status_emoji } <b>Import Result</b>\nFile: <code>{stats .get ('filename')}</code>\nTotal: {stats .get ('rows_total')}\nNew: {stats .get ('rows_inserted')}\nUpdated: {stats .get ('rows_updated')}\nSkipped: {stats .get ('rows_skipped')}\n"
        if stats .get ('status')=='error':
            msg +=f"\n Error: {stats .get ('message')}"
        await self .send_telegram (msg )

    async def _send_digest_email (self ,recipient :DigestRecipient ,stats :Dict [str ,Any ]):
        if not self .smtp_user or not self .smtp_pass :
            logger .warning ('SMTP credentials missing, skipping email for %s',recipient .email )
            return 
        try :
            template =self .jinja_env .get_template ('weekly_digest.html')
            html_content =template .render (name =recipient .name ,stats =stats )
            message =EmailMessage ()
            message ['From']=self .smtp_user 
            message ['To']=recipient .email 
            message ['Subject']=f"Weekly Leads Report — {datetime .now ().strftime ('%B %d, %Y')}"
            message .set_content ('Please view this email in an HTML-compatible client.')
            message .add_alternative (html_content ,subtype ='html')
            await aiosmtplib .send (message ,hostname =self .smtp_host ,port =self .smtp_port ,username =self .smtp_user ,password =self .smtp_pass ,use_tls =True if self .smtp_port ==465 else False ,start_tls =True if self .smtp_port ==587 else False )
            return True 
        except Exception as e :
            logger .error (f'Failed to send email to {recipient .email }: {e }')
            raise e 

    async def send_weekly_digest (self ):
        """
        Weekly digest (T3 Part 10.3)
        """
        async with AsyncSessionLocal ()as session :
            stats =await get_weekly_stats (session )
            res =await session .execute (select (DigestRecipient ).where (DigestRecipient .is_active ==True ))
            recipients =res .scalars ().all ()
            if not recipients :
                await self .send_telegram ('📬 Weekly Digest: No active recipients found.')
                return 
            import asyncio 
            tasks =[self ._send_digest_email (r ,stats )for r in recipients ]
            results =await asyncio .gather (*tasks ,return_exceptions =True )
            failed =[r .email for r ,res in zip (recipients ,results )if isinstance (res ,Exception )]
            delivered_count =len (recipients )-len (failed )
            if failed :
                tg_msg =f"📬 <b>Weekly Digest — Delivery Issues</b>\nDelivered: {delivered_count }\nFailed: {', '.join (failed )}"
            else :
                tg_msg =f'📬 <b>Weekly Digest Sent</b>\nRecipients: {len (recipients )}\nDelivered: {delivered_count }\nFailed: 0'
            await self .send_telegram (tg_msg )
notifier =Notifier ()
