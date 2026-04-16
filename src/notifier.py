import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from jinja2 import Environment, FileSystemLoader
import aiosmtplib
from email.message import EmailMessage

from .db import AsyncSessionLocal, DigestRecipient, get_weekly_stats
from sqlalchemy import select
from .config import settings

logger = logging.getLogger('leads_importer.notifier')

class Notifier:
    """Manager for Telegram and Email notifications."""

    def __init__(self) -> None:
        self.bot_token = settings.TELEGRAM_BOT_TOKEN
        self.chat_id = settings.TELEGRAM_CHAT_ID
        self.smtp_host = settings.SMTP_SERVER or 'smtp.gmail.com'
        self.smtp_port = settings.SMTP_PORT or 587
        self.smtp_user = settings.SMTP_USER
        self.smtp_pass = settings.SMTP_PASSWORD

        template_dir = os.path.join(os.path.dirname(__file__), '..', 'templates')
        os.makedirs(template_dir, exist_ok=True)
        self.jinja_env = Environment(loader=FileSystemLoader(template_dir))

    async def send_telegram(self, message: str) -> None:
        """Dispatch a message to the configured Telegram chat."""
        if not self.bot_token or not self.chat_id:
            return

        url = f'https://api.telegram.org/bot{self.bot_token}/sendMessage'
        payload = {'chat_id': self.chat_id, 'text': message, 'parse_mode': 'HTML'}

        try:
            async with httpx.AsyncClient() as client:
                await client.post(url, json=payload, timeout=10.0)
        except Exception as e:
            logger.error(f'Telegram send failed: {e}')

    async def send_import_summary(self, stats: Dict[str, Any]) -> None:
        """Post a summary report of import results to Telegram."""
        status = stats.get('status', 'unknown')
        filename = stats.get('filename', 'unknown')

        emoji = {
            'success': '✅',
            'partial': '⚠️',
            'skipped': 'ℹ️',
            'error': '❌'
        }.get(status, '❓')
        
        title = {
            'success': 'Import Complete',
            'partial': 'Import Partial',
            'skipped': 'Already Synchronized',
            'error': 'Import Failed'
        }.get(status, 'Import Finished')

        msg = [
            f'{emoji} <b>{title}</b>',
            f'File: <code>{filename}</code>',
            f'Total rows: {stats.get("rows_total", 0):,}',
            f'New leads: {stats.get("rows_inserted", 0):,}',
            f'Updated: {stats.get("rows_updated", 0):,}',
            f'Skipped: {stats.get("rows_skipped", 0):,}',
        ]
        if stats.get('duration'):
            msg.append(f'Duration: {stats["duration"]}s')
        if status == 'error' and stats.get('message'):
            msg.append(f'\n⚠️ Error: {stats["message"]}')

        await self.send_telegram('\n'.join(msg))

    async def _send_digest_email(self, recipient_email: str, recipient_name: Optional[str], stats: Dict[str, Any]) -> bool:
        """Send the weekly digest report via SMTP."""
        if not self.smtp_user or not self.smtp_pass:
            return False

        name = recipient_name or recipient_email.split('@')[0].title()
        now = datetime.now(timezone.utc)

        try:
            template = self.jinja_env.get_template('weekly_digest.html')
            html_content = template.render(name=name, stats=stats, date=now)
        except Exception as e:
            logger.warning(f'Email template not found or failed to render: {e}. Using fallback.')
            html_content = self._build_digest_html(name, stats, now)

        message = EmailMessage()
        message['From'], message['To'] = self.smtp_user, recipient_email
        message['Subject'] = f"Weekly Leads Report — {now.strftime('%B %d, %Y')}"
        message.set_content('Weekly Leads Report. Please use HTML client.')
        message.add_alternative(html_content, subtype='html')

        await aiosmtplib.send(
            message, hostname=self.smtp_host, port=self.smtp_port,
            username=self.smtp_user, password=self.smtp_pass,
            use_tls=(self.smtp_port == 465), start_tls=(self.smtp_port == 587),
        )
        return True

    def _build_digest_html(self, name: str, stats: Dict[str, Any], date: datetime) -> str:
        """Fallback HTML generator if the jinja2 template is unavailable."""
        dq = stats.get('data_quality', {})
        countries = ''.join([f'<li>{c.get("name", "?")} — {c.get("count", 0):,}</li>' for c in stats.get('top_countries', [])])
        sources = ''.join([f'<li>{s.get("name", "?")} — {s.get("count", 0):,}</li>' for s in stats.get('top_sources', [])])
        imp = stats.get('imports', {})

        return f"""
        <html><body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <div style="background: #f8f9fa; padding: 20px; border-bottom: 2px solid #007bff; text-align: center;">
                <h2 style="margin:0;">Weekly Leads Report</h2>
                <p style="color:#666;">{date.strftime('%B %d, %Y')}</p>
            </div>
            <p>Hi {name},</p>
            <p>Please find below this week's summary of our leads database.</p>
            
            <h3 style="color: #444; border-bottom: 1px solid #ddd;">📊 DATABASE OVERVIEW</h3>
            <p>Total: <b>{stats.get('total_leads', 0):,}</b><br>
               New: <b>+{stats.get('new_this_week', 0):,}</b><br>
               Updated: <b>+{stats.get('updated_this_week', 0):,}</b></p>
            
            <h3 style="color: #444; border-bottom: 1px solid #ddd;">📈 DATA QUALITY</h3>
            <p>Phone: <b>{dq.get('phone_pct', 0)}%</b> | Name: <b>{dq.get('name_pct', 0)}%</b> | Country: <b>{dq.get('country_pct', 0)}%</b> | Buyers: <b>{dq.get('buyer_pct', 0)}%</b></p>
            
            <h3 style="color: #444; border-bottom: 1px solid #ddd;">🌍 TOP COUNTRIES</h3>
            <ul>{countries or '<li>No data</li>'}</ul>
            
            <h3 style="color: #444; border-bottom: 1px solid #ddd;">📡 TOP SOURCES THIS WEEK</h3>
            <ul>{sources or '<li>No data</li>'}</ul>

            <h3 style="color: #444; border-bottom: 1px solid #ddd;">📦 IMPORTS THIS WEEK</h3>
            <p>Files processed: <b>{imp.get('processed', 0)}</b><br>
               Successful: <span style="color:green;"><b>{imp.get('success', 0)}</b></span> | Failed: <span style="color:red;"><b>{imp.get('failed', 0)}</b></span></p>

            <p style="margin-top:30px; border-top: 1px solid #eee; padding-top:10px; font-size:12px; color:#999;">
                Best regards,<br><b>Antigravity AI System</b>
            </p>
        </body></html>
        """

    async def send_weekly_digest(self) -> None:
        """Orchestrate the delivery of the weekly digest to all active recipients."""
        async with AsyncSessionLocal() as session:
            stats = await get_weekly_stats(session)
            res = await session.execute(select(DigestRecipient).where(DigestRecipient.is_active.is_(True)))
            recipients = res.scalars().all()

        if not recipients:
            await self.send_telegram('📬 Weekly Digest: No active recipients found.')
            return

        tasks = [self._send_digest_email(r.email, r.full_name, stats) for r in recipients]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        failed = [f'{r.email}' for r, res in zip(recipients, results) if isinstance(res, Exception)]
        delivered = len(recipients) - len(failed)
        
        tg_msg = f'📬 Weekly Digest Sent\nRecipients: {len(recipients)}\nDelivered: {delivered}\nFailed: {len(failed)}'
        await self.send_telegram(tg_msg)

notifier = Notifier()
