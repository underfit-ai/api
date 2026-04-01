from __future__ import annotations

import smtplib
from email.message import EmailMessage

from underfit_api.config import EmailConfig


def send_email(cfg: EmailConfig, to: str, subject: str, body: str) -> None:
    msg = EmailMessage()
    msg["From"] = cfg.from_address
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port) as server:
        if cfg.starttls:
            server.starttls()
        if cfg.smtp_user:
            server.login(cfg.smtp_user, cfg.smtp_password)
        server.send_message(msg)
