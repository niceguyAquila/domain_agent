from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    create_engine,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from auction_ahrefs.models import AhrefsBundle, AuctionListing


class Base(DeclarativeBase):
    pass


class RunRow(Base):
    __tablename__ = "runs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    stats_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class ListingRow(Base):
    __tablename__ = "listings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    domain: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(512), nullable=False)
    auction_end_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    price_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    bids: Mapped[int | None] = mapped_column(Integer, nullable=True)
    auction_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    detail_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class AhrefsCacheRow(Base):
    __tablename__ = "ahrefs_cache"
    domain: Mapped[str] = mapped_column(String(255), primary_key=True)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    report_date: Mapped[str] = mapped_column(String(16), nullable=False)
    domain_rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    ahrefs_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    org_keywords: Mapped[int | None] = mapped_column(Integer, nullable=True)
    org_traffic: Mapped[int | None] = mapped_column(Integer, nullable=True)
    org_cost_usd_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)
    raw_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)


def make_engine():
    url = os.environ.get("DATABASE_URL")
    if url:
        if url.startswith("postgres://"):
            url = "postgresql+psycopg2://" + url[len("postgres://") :]
        elif url.startswith("postgresql://") and "+psycopg2" not in url:
            url = "postgresql+psycopg2://" + url[len("postgresql://") :]
        return create_engine(url, pool_pre_ping=True)

    sqlite_path = os.environ.get("SQLITE_PATH", "./data/pipeline.db")
    Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
    return create_engine(
        f"sqlite:///{sqlite_path}",
        connect_args={"check_same_thread": False},
    )


def init_db(engine) -> None:
    Base.metadata.create_all(engine)


def session_factory(engine):
    return sessionmaker(engine, expire_on_commit=False)


def insert_run(
    session: Session,
    listings: Sequence[AuctionListing],
    note: str | None = None,
    stats: dict | None = None,
) -> int:
    now = datetime.now(timezone.utc)
    run = RunRow(created_at=now, note=note, stats_json=stats)
    session.add(run)
    session.flush()
    rid = run.id
    for L in listings:
        session.add(
            ListingRow(
                run_id=rid,
                source=L.source,
                domain=L.domain.lower(),
                fingerprint=L.listing_fingerprint(),
                auction_end_time=L.auction_end_time,
                price_usd=L.price_usd,
                bids=L.bids,
                auction_type=L.auction_type,
                detail_url=L.detail_url,
                raw_json=L.raw,
            )
        )
    session.commit()
    return rid


def get_cached_ahrefs(
    session: Session, domain: str, ttl_days: int, now: datetime | None = None
) -> AhrefsCacheRow | None:
    now = now or datetime.now(timezone.utc)
    row = session.get(AhrefsCacheRow, domain.lower())
    if row is None:
        return None
    age = now - row.fetched_at
    if age.days >= ttl_days:
        return None
    return row


def upsert_ahrefs_cache(session: Session, bundle: AhrefsBundle) -> None:
    now = datetime.now(timezone.utc)
    raw = {
        "domain_rating": bundle.raw_domain_rating,
        "metrics": bundle.raw_metrics,
    }
    row = session.get(AhrefsCacheRow, bundle.domain.lower())
    if row is None:
        row = AhrefsCacheRow(domain=bundle.domain.lower())
        session.add(row)
    row.fetched_at = now
    row.report_date = bundle.report_date
    row.domain_rating = bundle.domain_rating
    row.ahrefs_rank = bundle.ahrefs_rank
    row.org_keywords = bundle.org_keywords
    row.org_traffic = bundle.org_traffic
    row.org_cost_usd_cents = bundle.org_cost_usd_cents
    row.raw_json = raw
    session.commit()


def fetch_latest_run_domains_with_ahrefs(
    session: Session, run_id: int
) -> list[dict[str, Any]]:
    stmt = select(ListingRow).where(ListingRow.run_id == run_id)
    rows = list(session.scalars(stmt))
    out: list[dict[str, Any]] = []
    for lr in rows:
        cache = session.get(AhrefsCacheRow, lr.domain)
        item = {
            "domain": lr.domain,
            "source": lr.source,
            "bids": lr.bids,
            "price_usd": lr.price_usd,
            "detail_url": lr.detail_url,
            "domain_rating": cache.domain_rating if cache else None,
            "ahrefs_rank": cache.ahrefs_rank if cache else None,
            "org_keywords": cache.org_keywords if cache else None,
            "org_traffic": cache.org_traffic if cache else None,
        }
        out.append(item)
    out.sort(
        key=lambda x: (
            -(x["domain_rating"] or -1.0),
            -(x["org_traffic"] or 0),
            x["domain"],
        )
    )
    return out


def previous_run_id(session: Session, before_run_id: int) -> int | None:
    stmt = (
        select(RunRow.id)
        .where(RunRow.id < before_run_id)
        .order_by(RunRow.id.desc())
        .limit(1)
    )
    return session.scalar(stmt)


def domains_for_run(session: Session, run_id: int) -> set[str]:
    stmt = select(ListingRow.domain).where(ListingRow.run_id == run_id)
    return {d for d in session.scalars(stmt)}
