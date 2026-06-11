import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from app.config import settings

logger = logging.getLogger("portal.email")


def send_email(to: str, subject: str, html: str) -> bool:
    if not settings.SMTP_USER or not settings.SMTP_PASS:
        logger.warning("SMTP not configured — email not sent")
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = f"FerroFlux <{settings.SMTP_USER}>"
        msg["To"] = to
        msg["Subject"] = subject
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP_SSL(settings.SMTP_HOST, settings.SMTP_PORT) as server:
            server.login(settings.SMTP_USER, settings.SMTP_PASS)
            server.sendmail(settings.SMTP_USER, to, msg.as_string())
        logger.info(f"Email sent to {to}")
        return True
    except Exception as e:
        logger.error(f"Email send failed: {e}")
        return False


def send_reset_email(to: str, reset_link: str) -> bool:
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;background:#080B10;color:#C7CDD4;padding:32px;border-radius:8px">
      <h2 style="color:#00AEEF;font-size:22px;margin-bottom:8px">FerroFlux — Password Reset</h2>
      <p style="margin-bottom:24px">Click the button below to reset your password. This link expires in <strong>1 hour</strong>.</p>
      <a href="{reset_link}"
         style="display:inline-block;background:#00AEEF;color:#080B10;font-weight:700;padding:14px 28px;border-radius:6px;text-decoration:none;font-size:15px">
        Reset Password
      </a>
      <p style="margin-top:24px;font-size:12px;color:#8A929D">
        If you didn't request this, ignore this email — your password won't change.
      </p>
    </div>
    """
    return send_email(to, "FerroFlux — Reset your password", html)
