from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

from auction_ahrefs.config import EmailReportConfig


def send_report_email(
    cfg: EmailReportConfig,
    *,
    subject: str,
    body: str,
    attachments: list[Path],
) -> None:
    if not cfg.to_addrs:
        raise ValueError("email.to_addrs is empty")
    if not (cfg.smtp_user or "").strip():
        raise ValueError("email_report.smtp_user is empty")

    pwd_env = cfg.smtp_password_env or "SMTP_PASSWORD"
    password = os.environ.get(pwd_env, "") or ""
    if not password.strip():
        raise RuntimeError(
            f"Missing SMTP password in environment variable {pwd_env!r}"
        )

    from_addr = cfg.from_addr or cfg.smtp_user
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(cfg.to_addrs)
    msg.set_content(body)

    for p in attachments:
        data = Path(p).read_bytes()
        if p.suffix.lower() == ".csv":
            msg.add_attachment(
                data, maintype="text", subtype="csv", filename=p.name
            )
        else:
            msg.add_attachment(
                data,
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=p.name,
            )

    if cfg.use_ssl:
        with smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port) as smtp:
            smtp.login(cfg.smtp_user, password)
            smtp.send_message(msg)
    else:
        with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port) as smtp:
            smtp.ehlo()
            if cfg.use_tls:
                smtp.starttls()
                smtp.ehlo()
            smtp.login(cfg.smtp_user, password)
            smtp.send_message(msg)
