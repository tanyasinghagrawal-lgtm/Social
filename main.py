import os
import re
import random
import asyncio
import base64
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import httpx

from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, Float, Boolean, 
    DateTime, ForeignKey, select, update, delete, func, or_, and_, desc
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import ARRAY

import json
import redis.asyncio as aioredis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("telegram_social_backend")

POST_CACHE_TTL = 30 * 24 * 60 * 60  # 30 Days in seconds (2,592,000s)

async def cache_single_post(post_dict: dict):
    """Saves post details and vector embedding in Redis with 30-day TTL."""
    try:
        post_id = post_dict["post_id"]
        created_ts = datetime.fromisoformat(post_dict["created_at"]).timestamp()
        
        pipe = redis_client.pipeline()
        pipe.set(f"post:{post_id}:data", json.dumps(post_dict), ex=POST_CACHE_TTL)
        pipe.zadd("posts:recent:zset", {str(post_id): created_ts})
        await pipe.execute()
    except Exception as e:
        logger.error(f"Error caching single post {post_dict.get('post_id')}: {e}")

async def remove_post_from_cache(post_id: int):
    """Deletes post from Redis cache and sorted set."""
    try:
        pipe = redis_client.pipeline()
        pipe.delete(f"post:{post_id}:data")
        pipe.zrem("posts:recent:zset", str(post_id))
        await pipe.execute()
    except Exception as e:
        logger.error(f"Error removing post {post_id} from cache: {e}")

async def update_post_cache_stats(post_id: int, field: str, increment: int):
    """Live-updates counters in Redis so feeds always show fresh likes/views/comments."""
    cache_key = f"post:{post_id}:data"
    try:
        raw_data = await redis_client.get(cache_key)
        if raw_data:
            post_data = json.loads(raw_data)
            post_data[field] = max(0, post_data.get(field, 0) + increment)
            
            ttl = await redis_client.ttl(cache_key)
            if ttl and ttl > 0:
                await redis_client.set(cache_key, json.dumps(post_data), ex=ttl)
    except Exception as e:
        logger.error(f"Error updating cache stats for post {post_id}: {e}")

async def increment_db_view(post_id: int):
    """Background task to increment view count in DB when post is served from Cache."""
    try:
        async with AsyncSessionLocal() as session:
            post = await session.get(Post, post_id)
            if post:
                post.view_count += 1
                await session.commit()
    except Exception as e:
        logger.error(f"Error incrementing DB view for post {post_id}: {e}")

async def sync_30_days_posts_to_redis():
    """Ultra-fast batch startup sync: Loads missing 30-day posts into Redis using pipelines."""
    try:
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        cutoff_ts = thirty_days_ago.timestamp()
        
        # Cleanup expired items from Redis Sorted Set
        await redis_client.zremrangebyscore("posts:recent:zset", "-inf", cutoff_ts)
        
        async with AsyncSessionLocal() as session:
            stmt = (
                select(Post, User.username)
                .outerjoin(User, Post.author_telegram_id == User.telegram_id)
                .where(Post.created_at >= thirty_days_ago)
                .order_by(Post.id.desc())
            )
            res = await session.execute(stmt)
            rows = res.all()
            
            if not rows:
                return

            pipe = redis_client.pipeline()
            batch_count = 0
            
            for p, u_name in rows:
                post_data = {
                    "post_id": p.id,
                    "author_id": p.author_telegram_id,
                    "author_username": u_name or "anonymous",
                    "snippet": p.content[:70],
                    "content": p.content,
                    "embedding": p.embedding or [],
                    "like_count": p.like_count,
                    "comment_count": p.comment_count,
                    "view_count": p.view_count,
                    "created_at": p.created_at.isoformat()
                }
                
                # Check remaining TTL or write if missing
                pipe.set(f"post:{p.id}:data", json.dumps(post_data), ex=POST_CACHE_TTL, nx=True)
                pipe.zadd("posts:recent:zset", {str(p.id): p.created_at.timestamp()})
                batch_count += 1
                
                # Execute in batches of 200 to prevent Redis lock & CPU spike
                if batch_count % 200 == 0:
                    await pipe.execute()
                    pipe = redis_client.pipeline()

            await pipe.execute()
            logger.info(f"Successfully verified & cached {len(rows)} posts from the last 30 days.")
    except Exception as e:
        logger.error(f"Failed to sync 30-day posts to Redis: {e}")
# Database & Cache URIs provided by user

DATABASE_URL = "postgresql+asyncpg://avnadmin:AVNS_jTfrFSn4cMYbutIKDKN@pg-88cc622-youtrendsfunny-a945.a.aivencloud.com:22179/defaultdb?ssl=require"
REDIS_URL = "rediss://default:AVNS_AsrIADJbKztk2gy1vQx@valkey-102cf15d-mritunjayanything-ba30.e.aivencloud.com:13011"
BOT_TOKEN = "8949183239:AAHinQntOYZVQ6Bz6WRgszqFEjq3M3AfTbc"
FRONTEND_URL = "https://htmlditorsssj.pages.dev"
ADMIN_ID = 6796088344

_ENCODED_GEMINI_KEY = "QVEuQWI4Uk42TFJWVmZSNTAxV1dodXEwZUZESzh2NVlqOVZUa1hyMnlLa1ozeHJlT0RtQWc="

GEMINI_KEYS = [
    base64.b64decode(_ENCODED_GEMINI_KEY).decode("utf-8"),
    base64.b64decode(_ENCODED_GEMINI_KEY).decode("utf-8"),
    base64.b64decode(_ENCODED_GEMINI_KEY).decode("utf-8"),
    base64.b64decode(_ENCODED_GEMINI_KEY).decode("utf-8"),
]
engine = create_async_engine(
    DATABASE_URL, 
    echo=False, 
    pool_pre_ping=True,
    pool_size=20,
    max_overflow=10
)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    telegram_id = Column(BigInteger, primary_key=True, index=True)
    username = Column(String(50), unique=True, index=True, nullable=True)
    profile_score_int = Column(Integer, default=0, index=True) 
    mention_score = Column(Integer, default=0, index=True)
    badges = Column(Integer, default=0)
    follower_count = Column(Integer, default=0)
    following_count = Column(Integer, default=0)
    last_score_milestone = Column(Integer, default=0) 
    
    # NEW: Avatar SVG Configuration (Default set to Bella + Mint bg)
    avatar_seed = Column(String(50), default="Bella")
    avatar_bg = Column(String(10), default="c1f2d6")
    avatar_flip = Column(Boolean, default=False)
    avatar_scale = Column(Integer, default=100)
    
    last_active = Column(DateTime, default=datetime.utcnow, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
class Follow(Base):
    __tablename__ = "follows"
    follower_id = Column(BigInteger, ForeignKey("users.telegram_id"), primary_key=True)
    followed_id = Column(BigInteger, ForeignKey("users.telegram_id"), primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
class Post(Base):
    __tablename__ = "posts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    author_telegram_id = Column(BigInteger, ForeignKey("users.telegram_id"), index=True)
    content = Column(Text, nullable=False)
    embedding = Column(ARRAY(Float), nullable=True)
    like_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)
    view_count = Column(Integer, default=0)
    impression_count = Column(Integer, default=0)
    milestone_views_notified = Column(Integer, default=0) # Bitmask for 10, 100, 1000 views
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

class Comment(Base):
    __tablename__ = "comments"
    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(Integer, ForeignKey("posts.id", ondelete="CASCADE"), index=True)
    author_telegram_id = Column(BigInteger, ForeignKey("users.telegram_id"), index=True)
    parent_comment_id = Column(Integer, ForeignKey("comments.id", ondelete="CASCADE"), nullable=True)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Like(Base):
    __tablename__ = "likes"
    user_telegram_id = Column(BigInteger, ForeignKey("users.telegram_id"), primary_key=True)
    post_id = Column(Integer, ForeignKey("posts.id", ondelete="CASCADE"), primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class Block(Base):
    __tablename__ = "blocks"
    blocker_id = Column(BigInteger, ForeignKey("users.telegram_id"), primary_key=True)
    blocked_id = Column(BigInteger, ForeignKey("users.telegram_id"), primary_key=True)

class Report(Base):
    __tablename__ = "reports"
    id = Column(Integer, primary_key=True, autoincrement=True)
    target_type = Column(String(20), nullable=False) # 'post' or 'comment'
    target_id = Column(Integer, nullable=False)
    reported_by = Column(BigInteger, ForeignKey("users.telegram_id"), nullable=False)
    reason = Column(Text, nullable=False)
    expectation = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class DirectMessage(Base):
    __tablename__ = "direct_messages"
    id = Column(Integer, primary_key=True, autoincrement=True)
    sender_id = Column(BigInteger, ForeignKey("users.telegram_id"), index=True)
    receiver_id = Column(BigInteger, ForeignKey("users.telegram_id"), index=True)
    content = Column(Text, nullable=False)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Notification(Base):
    __tablename__ = "notifications"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.telegram_id"), index=True)
    sender_id = Column(BigInteger, ForeignKey("users.telegram_id"))
    notification_type = Column(String(30)) # 'like', 'comment', 'reply', 'mention'
    post_id = Column(Integer, nullable=True)
    content = Column(Text, nullable=True)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class AvailableUsername(Base):
    __tablename__ = "available_usernames"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, index=True)

class DailyAnalytics(Base):
    __tablename__ = "daily_analytics"
    date_key = Column(String(10), primary_key=True) # YYYY-MM-DD
    views = Column(Integer, default=0)
    posts = Column(Integer, default=0)
    comments = Column(Integer, default=0)
    livefeed_views = Column(Integer, default=0)
    homefeed_views = Column(Integer, default=0)
    dms = Column(Integer, default=0)

class NewPostRequest(BaseModel):
    telegram_id: int
    content: str = Field(..., max_length=5000)

class LikeRequest(BaseModel):
    telegram_id: int
    post_id: int

class ImpressionRequest(BaseModel):
    viewed_by: int
    post_id: int

class ReportRequest(BaseModel):
    reported_by: int
    target_type: str
    target_id: int
    reason: str
    expectation: str

class SendMessageRequest(BaseModel):
    sender_id: int
    receiver_id: int
    content: str

class BlockRequest(BaseModel):
    blocker_id: int
    blocked_id: int

class FollowRequest(BaseModel):
    follower_id: int
    followed_id: int
class UsernameRequest(BaseModel):
    telegram_id: int
    username: str
    gender: str = 'o'  # Default to 'o' for backward compatibility

class GenderUpdateRequest(BaseModel):
    telegram_id: int
    gender: str

class AvatarUpdateRequest(BaseModel):
    telegram_id: int
    avatar_seed: str
    avatar_bg: str
    avatar_flip: bool
    avatar_scale: int


class CommentRequest(BaseModel):
    telegram_id: int
    post_id: int
    content: str = Field(..., max_length=200)
    parent_comment_id: Optional[int] = None
class ItemDeleteRequest(BaseModel):
    telegram_id: int
    item_type: str # 'post' or 'comment'
    item_id: int

class ReadNotificationsRequest(BaseModel):
    telegram_id: int

async def send_telegram_bot_message(
    chat_id: int, 
    text: str, 
    button_text: Optional[str] = None, 
    button_url: Optional[str] = None
):
    """Sends a direct message via Telegram Bot API with an optional WebApp inline keyboard."""
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        logger.info(f"[Bot Mock] Message to {chat_id}: {text}")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }

    if button_text and button_url:
        payload["reply_markup"] = {
            "inline_keyboard": [[
                {
                    "text": button_text,
                    "web_app": {"url": button_url}
                }
            ]]
        }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.post(url, json=payload)
            if res.status_code != 200:
                logger.error(f"Telegram Bot API Error ({res.status_code}): {res.text}")
    except Exception as e:
        logger.error(f"Failed to send Telegram Bot message: {e}")

async def get_gemini_embedding(text: str) -> List[float]:
    """Uses 'gemini-embedding-2' with 128 dimensions, random key rotation & Admin alert."""
    url_template = "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-2:embedContent?key={api_key}"
    payload = {
        "model": "models/gemini-embedding-2",
        "content": {"parts": [{"text": text[:2000]}]},
        "outputDimensionality": 128
    }

    available_keys = GEMINI_KEYS.copy()
    random.shuffle(available_keys)
    errors = []

    async with httpx.AsyncClient(timeout=10.0) as client:
        for idx, api_key in enumerate(available_keys):
            if not api_key:
                continue
                
            url = url_template.format(api_key=api_key)
            try:
                # Direct simple request like main.py (No extra Auth headers needed for these keys)
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    if "embedding" in data and "values" in data["embedding"]:
                        return data["embedding"]["values"]
                    elif "embedding" in data:
                        return data["embedding"]
                else:
                    err_msg = f"Key #{idx + 1} HTTP {resp.status_code}: {resp.text[:120]}"
                    logger.warning(f"Embedding failed: {err_msg}")
                    errors.append(err_msg)
            except Exception as e:
                err_msg = f"Key #{idx + 1} Network Error: {str(e)}"
                logger.warning(err_msg)
                errors.append(err_msg)

    # Agar saari keys fail hoti hain tabhi Admin ko alert jayega
    if errors:
        error_lines = "\n".join([f"• <code>{err}</code>" for err in errors])
        admin_alert_text = (
            f"⚠️ <b>Gemini Embedding Failure Alert (128-dim)!</b>\n\n"
            f"📝 <b>Post:</b> <i>\"{text[:60]}...\"</i>\n\n"
            f"❌ <b>Errors Across Retries:</b>\n{error_lines}\n\n"
            f"⏰ <b>Time:</b> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        try:
            await send_telegram_bot_message(ADMIN_ID, admin_alert_text)
        except Exception as notify_err:
            logger.error(f"Telegram notification error: {notify_err}")

    return [0.0] * 128
async def increment_analytics(metric: str, amount: int = 1):
    """Stores analytics counters in Redis, synced to Postgres every 20-60s."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    cache_key = f"analytics:{today}:{metric}"
    try:
        await redis_client.incrby(cache_key, amount)
    except Exception as e:
        logger.error(f"Failed to increment redis analytics: {e}")

def cosine_similarity(v1: List[float], v2: List[float]) -> float:
    if not v1 or not v2 or len(v1) != len(v2):
        return 0.0
    dot_product = sum(a * b for a, b in zip(v1, v2))
    norm_v1 = sum(a * a for a in v1) ** 0.5
    norm_v2 = sum(b * b for b in v2) ** 0.5
    if norm_v1 == 0 or norm_v2 == 0:
        return 0.0
    return dot_product / (norm_v1 * norm_v2)

async def is_user_top_10_percent(session: AsyncSession, telegram_id: int) -> bool:
    """Checks if a given user is in the top 10% of users based on profile score."""
    total_users_res = await session.execute(select(func.count(User.telegram_id)))
    total_users = total_users_res.scalar() or 0
    if total_users <= 1:
        return True

    rank_res = await session.execute(
        select(func.count(User.telegram_id))
        .where(User.profile_score_int > select(User.profile_score_int).where(User.telegram_id == telegram_id).scalar_subquery())
    )
    user_rank = rank_res.scalar() or 0
    return (user_rank / total_users) <= 0.10

async def check_user_score_milestones(session: AsyncSession, user: User, background_tasks: BackgroundTasks):
    """Triggers Telegram Bot notification when user hits score milestones 10, 50, or 100."""
    actual_score = user.profile_score_int / 100.0
    milestones = [100, 50, 10]
    
    for m in milestones:
        if actual_score >= m and user.last_score_milestone < m:
            user.last_score_milestone = m
            msg = f"🎉 <b>Congratulations!</b>\nYour profile score has reached <b>{m}+ points</b> on WebApp Social!"
            target_url = f"{FRONTEND_URL}/?profile={user.telegram_id}"
            background_tasks.add_task(
                send_telegram_bot_message,
                user.telegram_id,
                msg,
                "Open Profile",
                target_url
            )
            break

async def check_post_view_milestones(session: AsyncSession, post: Post, background_tasks: BackgroundTasks):
    """Triggers Telegram Bot notification when a post crosses 10, 100, or 1000 views."""
    views = post.view_count
    milestones = [(1000, 4), (100, 2), (10, 1)] # Milestone value & bitmask flag
    
    for milestone_val, flag in milestones:
        if views >= milestone_val and not (post.milestone_views_notified & flag):
            post.milestone_views_notified |= flag
            msg = f"🔥 <b>Trending Post!</b>\nYour post <i>\"{post.content[:30]}...\"</i> has crossed <b>{milestone_val}+ views</b>!"
            target_url = f"{FRONTEND_URL}/?post={post.id}"
            background_tasks.add_task(
                send_telegram_bot_message,
                post.author_telegram_id,
                msg,
                "View Post",
                target_url
            )
            break



async def get_user_basics(user_ids: List[int], session: AsyncSession) -> Dict[int, dict]:
    """Fetch username and avatar from Redis cache or DB efficiently without blocking."""
    if not user_ids:
        return {}
    
    unique_ids = list(set(user_ids))
    pipe = redis_client.pipeline()
    for uid in unique_ids:
        pipe.get(f"user:{uid}:basic")
    
    cached_results = await pipe.execute()
    
    final_data = {}
    missing_ids = []
    
    for uid, cached_str in zip(unique_ids, cached_results):
        if cached_str:
            final_data[uid] = json.loads(cached_str)
        else:
            missing_ids.append(uid)
            
    if missing_ids:
        res = await session.execute(
            select(User.telegram_id, User.username, User.avatar_seed, User.avatar_bg, User.avatar_flip, User.avatar_scale)
            .where(User.telegram_id.in_(missing_ids))
        )
        rows = res.all()
        
        pipe = redis_client.pipeline()
        for row in rows:
            user_dict = {
                "username": row.username or f"User_{row.telegram_id}",
                "avatar_seed": row.avatar_seed or "Bella",
                "avatar_bg": row.avatar_bg or "c1f2d6",
                "avatar_flip": row.avatar_flip or False,
                "avatar_scale": row.avatar_scale or 100
            }
            final_data[row.telegram_id] = user_dict
            pipe.set(f"user:{row.telegram_id}:basic", json.dumps(user_dict), ex=86400) # Cache for 24 hours
        await pipe.execute()
        
    return final_data

app = FastAPI(title="Telegram WebApp Social Media Backend", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def periodic_db_sync():
    """Syncs Redis statistics to Postgres every 20s and notifies inactive users daily."""
    while True:
        try:
            await asyncio.sleep(20)
            async with AsyncSessionLocal() as session:
                today = datetime.utcnow().strftime("%Y-%m-%d")
                metrics = ["views", "posts", "comments", "livefeed_views", "homefeed_views", "dms"]
                
                stmt = select(DailyAnalytics).where(DailyAnalytics.date_key == today)
                res = await session.execute(stmt)
                analytics_record = res.scalars().first()
                
                if not analytics_record:
                    analytics_record = DailyAnalytics(date_key=today)
                    session.add(analytics_record)
                    await session.commit()
                
                for m in metrics:
                    redis_k = f"analytics:{today}:{m}"
                    val = await redis_client.getset(redis_k, "0")
                    if val and int(val) > 0:
                        current = getattr(analytics_record, m, 0)
                        setattr(analytics_record, m, current + int(val))
                
                await session.commit()

                # Inactive User Re-engagement Check (Every 1 Hour)
                check_key = "system:last_inactive_check"
                last_checked = await redis_client.get(check_key)
                if not last_checked:
                    await redis_client.set(check_key, "1", ex=3600)
                    three_days_ago = datetime.utcnow() - timedelta(days=3)
                    inactive_users_stmt = select(User).where(User.last_active < three_days_ago).limit(10)
                    inactive_res = await session.execute(inactive_users_stmt)
                    inactive_users = inactive_res.scalars().all()
                    
                    for iu in inactive_users:
                        msg = "👋 <b>We miss you on WebApp Social!</b>\nCheck out the latest trending posts today."
                        await send_telegram_bot_message(iu.telegram_id, msg, "Explore Feed", FRONTEND_URL)
                        iu.last_active = datetime.utcnow() # Reset active flag
                    await session.commit()

        except Exception as e:
            logger.error(f"Error in background worker: {e}")

async def seed_initial_usernames():
    """Seeds sample available usernames into database if empty."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(func.count(AvailableUsername.id)))
        count = result.scalar()
        if count == 0:
            sample_names = [
                f"user_{random.randint(1000, 9999)}",
                f"cyber_{random.randint(100, 999)}",
                f"pixel_{random.randint(100, 999)}",
                f"shadow_{random.randint(100, 999)}",
                f"nova_{random.randint(100, 999)}",
                f"alpha_{random.randint(100, 999)}",
            ]
            for name in sample_names:
                session.add(AvailableUsername(username=name))
            await session.commit()

@app.on_event("startup")
async def startup_event():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await seed_initial_usernames()
    asyncio.create_task(periodic_db_sync())
    asyncio.create_task(sync_30_days_posts_to_redis())  # Fast sync for 1-month post & vector cache

async def process_post_embedding(post_id: int, content: str):
    """Background task to compute Gemini vector embedding & update Redis Cache."""
    emb = await get_gemini_embedding(content)
    async with AsyncSessionLocal() as session:
        stmt = update(Post).where(Post.id == post_id).values(embedding=emb)
        await session.execute(stmt)
        await session.commit()
        
        # Fetch fresh post details to update RAM/Redis cache
        post_res = await session.execute(
            select(Post, User.username)
            .outerjoin(User, Post.author_telegram_id == User.telegram_id)
            .where(Post.id == post_id)
        )
        row = post_res.first()
        if row:
            p, u_name = row
            post_data = {
                "post_id": p.id,
                "author_id": p.author_telegram_id,
                "author_username": u_name or "anonymous",
                "snippet": p.content[:70],
                "content": p.content,
                "embedding": emb,
                "like_count": p.like_count,
                "comment_count": p.comment_count,
                "view_count": p.view_count,
                "created_at": p.created_at.isoformat()
            }
            await cache_single_post(post_data)
async def update_user_activity(session: AsyncSession, telegram_id: int):
    """Updates user last active timestamp."""
    user = await session.get(User, telegram_id)
    if user:
        user.last_active = datetime.utcnow()
    else:
        user = User(telegram_id=telegram_id, last_active=datetime.utcnow())
        session.add(user)
    await session.commit()

@app.get("/update")
async def check_live_updates(telegram_id: int, since: float = Query(0.0)):
    """Ultra-fast in-memory cache check for new feed posts, notifications, and DMs."""
    now_ts = datetime.utcnow().timestamp()
    
    pipe = redis_client.pipeline()
    pipe.get("global:last_post_ts")
    pipe.get(f"user:{telegram_id}:last_notif_ts")
    pipe.get(f"user:{telegram_id}:last_dm_ts")
    results = await pipe.execute()
    
    last_post = float(results[0]) if results[0] else 0.0
    last_notif = float(results[1]) if results[1] else 0.0
    last_dm = float(results[2]) if results[2] else 0.0
    
    # Check if any event happened after client's last check timestamp
    has_feed = last_post > since and (now_ts - last_post) <= 60
    has_notif = last_notif > since
    has_dm = last_dm > since
    
    return {
        "status": 200,
        "has_feed": bool(has_feed),
        "has_notif": bool(has_notif),
        "has_dm": bool(has_dm),
        "server_time": now_ts
    }

@app.get("/checknewposts")
async def check_new_posts(last_post_id: int):
    """Checks for new posts since the last loaded post and returns up to 3 latest users."""
    async with AsyncSessionLocal() as session:
        stmt = (
            select(Post, User)
            .join(User, Post.author_telegram_id == User.telegram_id)
            .where(Post.id > last_post_id)
            .order_by(Post.id.desc())
        )
        res = await session.execute(stmt)
        rows = res.all()
        
        if not rows:
            return {"has_new": False, "users": [], "count": 0}
        
        users_list = []
        seen_users = set()
        
        # Get up to 3 unique user dicts for the notification pill rings
        for p, u in rows:
            if u.telegram_id not in seen_users:
                seen_users.add(u.telegram_id)
                users_list.append({
                    "telegram_id": u.telegram_id,
                    "username": u.username or f"User_{u.telegram_id}",
                    "avatar_seed": u.avatar_seed or "Bella",
                    "avatar_bg": u.avatar_bg or "c1f2d6",
                    "avatar_flip": u.avatar_flip or False,
                    "avatar_scale": u.avatar_scale or 100
                })
                
                if len(users_list) == 3:
                    break
                    
        return {"has_new": True, "count": len(rows), "users": users_list}
@app.post("/newpost")
async def new_post(payload: NewPostRequest, background_tasks: BackgroundTasks):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    daily_post_key = f"user:{payload.telegram_id}:posts:{today}"
    
    current_count = await redis_client.get(daily_post_key)
    current_count = int(current_count) if current_count else 0
    if current_count >= 10:
        raise HTTPException(status_code=429, detail="Daily 10 post limit reached.")
    
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.telegram_id)

        new_p = Post(
            author_telegram_id=payload.telegram_id,
            content=payload.content
        )
        session.add(new_p)
        await session.commit()
        await session.refresh(new_p)
        
        now_ts = str(datetime.utcnow().timestamp())
        pipe = redis_client.pipeline()
        pipe.incr(daily_post_key)
        pipe.expire(daily_post_key, 86400)
        pipe.set("global:last_post_ts", now_ts, ex=120)
        await pipe.execute()        
        background_tasks.add_task(process_post_embedding, new_p.id, payload.content)
        background_tasks.add_task(increment_analytics, "posts", 1)
        
        return {
            "status": 200,
            "message": "Post published successfully",
            "post_id": new_p.id,
            "daily_left": 10 - (current_count + 1)
        }

@app.post("/like")
async def like_post(payload: LikeRequest, background_tasks: BackgroundTasks):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.telegram_id)
        
        existing = await session.get(Like, (payload.telegram_id, payload.post_id))
        if existing:
            return {"status": 400, "message": "Already liked this post"}
        
        post = await session.get(Post, payload.post_id)
        if not post:
            raise HTTPException(status_code=404, detail="Post not found")
        
        block_stmt = select(Block).where(
            or_(
                and_(Block.blocker_id == payload.telegram_id, Block.blocked_id == post.author_telegram_id),
                and_(Block.blocker_id == post.author_telegram_id, Block.blocked_id == payload.telegram_id)
            )
        )
        block_res = await session.execute(block_stmt)
        if block_res.scalars().first():
            raise HTTPException(status_code=403, detail="Cannot interact with this post")

        session.add(Like(user_telegram_id=payload.telegram_id, post_id=payload.post_id))
        post.like_count += 1
        
        # Scores: Liker gets +2.0 (+200 int), Author gets +0.05 (+5 int)
        liker = await session.get(User, payload.telegram_id)
        if liker:
            liker.profile_score_int += 200
            await check_user_score_milestones(session, liker, background_tasks)
            
        author = await session.get(User, post.author_telegram_id)
        if author:
            author.profile_score_int += 5
            
            notif = Notification(
                user_id=post.author_telegram_id,
                sender_id=payload.telegram_id,
                notification_type="like",
                post_id=post.id
            )
            session.add(notif)
            # Update author's live notification cache
            await redis_client.set(
                f"user:{post.author_telegram_id}:last_notif_ts", 
                str(datetime.utcnow().timestamp()), 
                ex=120
            )
            # Check if liker is in Top 10% Users for custom Telegram bot alert
            if liker and liker.username:
                if await is_user_top_10_percent(session, liker.telegram_id):
                    bot_msg = f"⭐ <b>Top User Interaction!</b>\n@{liker.username} (Top 10% Creator) has liked your post!"
                    target_url = f"{FRONTEND_URL}/?post={post.id}"
                    background_tasks.add_task(
                        send_telegram_bot_message,
                        author.telegram_id,
                        bot_msg,
                        "View Like",
                        target_url
                    )

        await session.commit()
        # Redis cache update
        background_tasks.add_task(update_post_cache_stats, payload.post_id, "like_count", 1)
        return {"status": 200, "message": "Post liked successfully"}
@app.post("/unlike")
async def unlike_post(payload: LikeRequest, background_tasks: BackgroundTasks):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.telegram_id)
        
        existing = await session.get(Like, (payload.telegram_id, payload.post_id))
        if not existing:
            return {"status": 400, "message": "Post was not liked"}

        post = await session.get(Post, payload.post_id)
        await session.delete(existing)
        
        if post and post.like_count > 0:
            post.like_count -= 1
        
        # Deduct Scores: Liker -3.0 (-300 int), Author -0.05 (-5 int)
        liker = await session.get(User, payload.telegram_id)
        if liker:
            liker.profile_score_int = max(0, liker.profile_score_int - 300)
            
        if post:
            author = await session.get(User, post.author_telegram_id)
            if author:
                author.profile_score_int = max(0, author.profile_score_int - 5)

        await session.commit()
        # Redis cache update
        background_tasks.add_task(update_post_cache_stats, payload.post_id, "like_count", -1)
        return {"status": 200, "message": "Post unliked successfully"}
@app.post("/impression")
async def record_impression(payload: ImpressionRequest, background_tasks: BackgroundTasks):
    imp_key = f"impression:{payload.post_id}:{payload.viewed_by}"
    exists = await redis_client.get(imp_key)
    
    if exists:
        return {"status": 200, "message": "Impression already recorded recently"}
    
    await redis_client.set(imp_key, "1", ex=600)
    
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.viewed_by)
        
        post = await session.get(Post, payload.post_id)
        if post:
            post.impression_count += 1
            post.view_count += 1
            
            viewer = await session.get(User, payload.viewed_by)
            if viewer:
                viewer.profile_score_int += 100
                await check_user_score_milestones(session, viewer, background_tasks)
                
            author = await session.get(User, post.author_telegram_id)
            if author:
                author.profile_score_int += 1
                
            await check_post_view_milestones(session, post, background_tasks)
            await session.commit()
            # Redis cache update
            background_tasks.add_task(update_post_cache_stats, payload.post_id, "view_count", 1)

    background_tasks.add_task(increment_analytics, "views", 1)
    return {"status": 200, "message": "Impression recorded"}
@app.get("/loadpost")
async def load_post(post_id: int, telegram_id: int, background_tasks: BackgroundTasks):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, telegram_id)
        
        # ==========================================
        # STEP 1: LOAD MAIN POST DATA (CACHE FIRST)
        # ==========================================
        cached_post_raw = await redis_client.get(f"post:{post_id}:data")
        post_data = None
        
        if cached_post_raw:
            # CACHE HIT: Ultra fast load from Redis
            post_data = json.loads(cached_post_raw)
            post_data["view_count"] += 1 # Optimistic local increment
            
            # Update cache & DB in background so user doesn't wait
            background_tasks.add_task(update_post_cache_stats, post_id, "view_count", 1)
            background_tasks.add_task(increment_db_view, post_id)
        else:
            # CACHE MISS: Load from Database
            post_res = await session.execute(
                select(Post, User.username)
                .outerjoin(User, Post.author_telegram_id == User.telegram_id)
                .where(Post.id == post_id)
            )
            row = post_res.first()
            if not row:
                raise HTTPException(status_code=404, detail="Post not found")
            post_db, author_username = row
            
            post_db.view_count += 1
            await session.commit()
            
            post_data = {
                "post_id": post_db.id,
                "author_id": post_db.author_telegram_id,
                "author_username": author_username or "anonymous",
                "snippet": post_db.content[:70],
                "content": post_db.content,
                "embedding": post_db.embedding or [],
                "like_count": post_db.like_count,
                "comment_count": post_db.comment_count,
                "view_count": post_db.view_count,
                "impression_count": post_db.impression_count,
                "created_at": post_db.created_at.isoformat()
            }
            # Save back to Cache for the next user
            background_tasks.add_task(cache_single_post, post_data)

        # ==========================================
        # STEP 2: LOAD USER-SPECIFIC DATA & COMMENTS (FAST DB QUERY)
        # ==========================================
        liked = await session.get(Like, (telegram_id, post_id))
        
        stmt = (
            select(Comment, User.username)
            .outerjoin(User, Comment.author_telegram_id == User.telegram_id)
            .where(Comment.post_id == post_id)
            .order_by(Comment.created_at.desc())
            .limit(12)
        )
        res = await session.execute(stmt)
        top_comments = res.all()

        user_ids = [post_data["author_id"]] + [c[0].author_telegram_id for c in top_comments]
        users_map = await get_user_basics(user_ids, session)

        return {
            "post_id": post_data["post_id"],
            "author_id": post_data["author_id"],
            "author_username": post_data["author_username"],
            "content": post_data["content"],
            "like_count": post_data["like_count"],
            "comment_count": post_data["comment_count"],
            "view_count": post_data["view_count"],
            "impression_count": post_data.get("impression_count", 0),
            "is_liked": bool(liked),
            "top_12_comments": [
                {
                    "comment_id": c.id,
                    "author_id": c.author_telegram_id,
                    "author_username": c_username or "anonymous",
                    "parent_comment_id": c.parent_comment_id,
                    "content": c.content,
                    "created_at": c.created_at.isoformat()
                } for c, c_username in top_comments
            ],
            "users_map": users_map
        }
@app.get("/livefeed")
async def live_feed(cursor_post_id: Optional[int] = Query(None), background_tasks: BackgroundTasks = None):
    feed_items = []
    last_id = None
    
    # Try Redis fast retrieval
    try:
        max_score = "+inf"
        if cursor_post_id:
            cursor_data = await redis_client.get(f"post:{cursor_post_id}:data")
            if cursor_data:
                max_score = f"({datetime.fromisoformat(json.loads(cursor_data)['created_at']).timestamp()}"

        post_ids = await redis_client.zrevrangebyscore("posts:recent:zset", max_score, "-inf", start=0, num=20)
        if post_ids:
            pipe = redis_client.pipeline()
            for pid in post_ids:
                pipe.get(f"post:{pid}:data")
            cached_posts = await pipe.execute()
            
            for raw in cached_posts:
                if raw:
                    p = json.loads(raw)
                    feed_items.append({
                        "post_id": p["post_id"],
                        "author_id": p["author_id"],
                        "author_username": p["author_username"],
                        "snippet": p["snippet"],
                        "content": p["content"],
                        "like_count": p["like_count"],
                        "comment_count": p["comment_count"],
                        "view_count": p["view_count"],
                        "created_at": p["created_at"]
                    })
            if feed_items:
                last_id = feed_items[-1]["post_id"]
    except Exception as e:
        logger.error(f"Livefeed Redis cache error: {e}")

    # Fallback to Database if cache is empty or missed
    if not feed_items:
        async with AsyncSessionLocal() as session:
            query = (
                select(Post, User.username)
                .outerjoin(User, Post.author_telegram_id == User.telegram_id)
            )
            if cursor_post_id:
                query = query.where(Post.id < cursor_post_id)
                
            query = query.order_by(Post.id.desc()).limit(20)
            result = await session.execute(query)
            rows = result.all()
            
            for p, u_name in rows:
                p_item = {
                    "post_id": p.id,
                    "author_id": p.author_telegram_id,
                    "author_username": u_name or "anonymous",
                    "snippet": p.content[:70],
                    "content": p.content,
                    "embedding": p.embedding or [],
                    "like_count": p.like_count,
                    "comment_count": p.comment_count,
                    "view_count": p.view_count,
                    "created_at": p.created_at.isoformat()
                }
                feed_items.append(p_item)
                if background_tasks:
                    background_tasks.add_task(cache_single_post, p_item)

            last_id = rows[-1][0].id if rows else None

    async with AsyncSessionLocal() as session:
        user_ids = [p["author_id"] for p in feed_items]
        users_map = await get_user_basics(user_ids, session)

    if background_tasks:
        background_tasks.add_task(increment_analytics, "livefeed_views", 1)

    return {
        "posts": feed_items,
        "users_map": users_map,
        "last_post_id": last_id
    }
@app.get("/homefeed")
async def home_feed(telegram_id: int, cursor_post_id: Optional[int] = Query(None), background_tasks: BackgroundTasks = None):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, telegram_id)
        
        liked_posts = await session.execute(
            select(Post.embedding)
            .join(Like, Like.post_id == Post.id)
            .where(Like.user_telegram_id == telegram_id)
            .limit(10)
        )
        embeddings = [e for e in liked_posts.scalars().all() if e and len(e) == 128]
        
        user_vector = None
        if embeddings:
            user_vector = [sum(e[i] for e in embeddings) / len(embeddings) for i in range(128)]

    # --- 1. Redis In-Memory Candidate Retrieval ---
    candidate_posts = []
    missing_ids = []
    
    try:
        # Get candidate post IDs from Redis sorted set
        max_score = "+inf"
        if cursor_post_id:
            cursor_data = await redis_client.get(f"post:{cursor_post_id}:data")
            if cursor_data:
                max_score = f"({datetime.fromisoformat(json.loads(cursor_data)['created_at']).timestamp()}"

        post_ids = await redis_client.zrevrangebyscore("posts:recent:zset", max_score, "-inf", start=0, num=60)
        
        if post_ids:
            pipe = redis_client.pipeline()
            for pid in post_ids:
                pipe.get(f"post:{pid}:data")
            cached_raw = await pipe.execute()
            
            for pid, raw in zip(post_ids, cached_raw):
                if raw:
                    candidate_posts.append(json.loads(raw))
                else:
                    missing_ids.append(int(pid))
    except Exception as e:
        logger.error(f"Redis homefeed retrieval error: {e}")

    # --- 2. Fallback & Self-Healing if Cache Misses or Low Count ---
    if len(candidate_posts) < 15 or missing_ids:
        async with AsyncSessionLocal() as session:
            query = (
                select(Post, User.username)
                .outerjoin(User, Post.author_telegram_id == User.telegram_id)
            )
            if cursor_post_id:
                query = query.where(Post.id < cursor_post_id)
            query = query.order_by(Post.id.desc()).limit(50)
            
            db_candidates = (await session.execute(query)).all()
            
            for p, u_name in db_candidates:
                if not any(cp["post_id"] == p.id for cp in candidate_posts):
                    p_data = {
                        "post_id": p.id,
                        "author_id": p.author_telegram_id,
                        "author_username": u_name or "anonymous",
                        "snippet": p.content[:70],
                        "content": p.content,
                        "embedding": p.embedding or [],
                        "like_count": p.like_count,
                        "comment_count": p.comment_count,
                        "view_count": p.view_count,
                        "created_at": p.created_at.isoformat()
                    }
                    candidate_posts.append(p_data)
                    if background_tasks:
                        background_tasks.add_task(cache_single_post, p_data)

    # --- 3. Vector Similarity & Scoring in Memory ---
    # --- 3. Vector Similarity & Scoring in Memory (With 20% Follow Boost) ---
    followed_ids = set()
    async with AsyncSessionLocal() as session:
        # Fetching users that the current user follows
        followed_res = await session.execute(select(Follow.followed_id).where(Follow.follower_id == telegram_id))
        followed_ids = set(followed_res.scalars().all())

    scored_posts = []
    for cp in candidate_posts:
        sim = 0.0
        vec = cp.get("embedding")
        if user_vector and vec and len(vec) == 128:
            sim = cosine_similarity(user_vector, vec)
            
        popularity = (cp.get("like_count", 0) * 2) + (cp.get("comment_count", 0) * 3)
        final_score = (sim * 10.0) + (popularity * 0.1) + random.uniform(0.0, 1.0)
        
        # 💡 SMART ALGORITHM: 20% influence/boost if user follows the author
        if cp.get("author_id") in followed_ids:
            final_score *= 1.20 
            
        scored_posts.append((final_score, cp))

    scored_posts.sort(key=lambda x: x[0], reverse=True)
    selected_posts = [sp[1] for sp in scored_posts[:16]]
    if background_tasks:
        background_tasks.add_task(increment_analytics, "homefeed_views", 1)

    lowest_candidate_id = min([cp["post_id"] for cp in candidate_posts]) if candidate_posts else None
    
    async with AsyncSessionLocal() as session:
        user_ids = [p["author_id"] for p in selected_posts]
        users_map = await get_user_basics(user_ids, session)

    return {
        "posts": [
            {
                "post_id": p["post_id"],
                "author_id": p["author_id"],
                "author_username": p["author_username"],
                "snippet": p["snippet"],
                "content": p["content"],
                "like_count": p["like_count"],
                "comment_count": p["comment_count"],
                "view_count": p["view_count"],
                "created_at": p["created_at"]
            } for p in selected_posts
        ],
        "users_map": users_map,
        "last_post_id": lowest_candidate_id
    }
@app.get("/profile")
async def get_profile(target_id: int, loaded_by: int):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, loaded_by)
        
        # Redis Profile Stats Cache check
        cached_stats = await redis_client.get(f"user:{target_id}:stats")
        followers, following = 0, 0
        
        user = await session.get(User, target_id)
        if not user:
            raise HTTPException(status_code=404, detail="User profile not found")
            
        if cached_stats:
            stats = json.loads(cached_stats)
            followers = stats.get("followers", user.follower_count)
            following = stats.get("following", user.following_count)
        else:
            followers = user.follower_count
            following = user.following_count
            await redis_client.set(f"user:{target_id}:stats", json.dumps({"followers": followers, "following": following}), ex=86400)
        
        is_self = (target_id == loaded_by)
        is_following = False
        
        if not is_self:
            follow_check = await session.get(Follow, (loaded_by, target_id))
            if follow_check:
                is_following = True

        # NEW: Get Total Posts Count accurately
        total_posts_res = await session.execute(select(func.count(Post.id)).where(Post.author_telegram_id == target_id))
        total_posts = total_posts_res.scalar() or 0

        posts = (await session.execute(
            select(Post).where(Post.author_telegram_id == target_id).order_by(Post.id.desc()).limit(20)
        )).scalars().all()

        return {
            "telegram_id": user.telegram_id,
            "username": user.username or f"User_{user.telegram_id}",
            "profile_score": user.profile_score_int / 100.0,
            "badges": user.badges,
            "follower_count": followers,
            "following_count": following,
            "is_following": is_following,
            "is_self": is_self,
            "avatar_seed": user.avatar_seed or "Bella",
            "avatar_bg": user.avatar_bg or "c1f2d6",
            "avatar_flip": user.avatar_flip or False,
            "avatar_scale": user.avatar_scale or 100,
            "total_posts": total_posts, # <--- NEW FIELD ADDED
            "posts": [                
                {
                    "post_id": p.id,
                    "content": p.content,
                    "like_count": p.like_count,
                    "comment_count": p.comment_count,
                    "view_count": p.view_count,
                    "created_at": p.created_at.isoformat()
                } for p in posts
            ]
        }

# NEW ENDPOINT: Fetch paginated profile posts
@app.get("/userposts")
async def get_user_posts(target_id: int, cursor_post_id: Optional[int] = Query(None)):
    async with AsyncSessionLocal() as session:
        query = select(Post).where(Post.author_telegram_id == target_id)
        if cursor_post_id:
            query = query.where(Post.id < cursor_post_id)
        query = query.order_by(Post.id.desc()).limit(20)
        
        posts = (await session.execute(query)).scalars().all()
        last_id = posts[-1].id if posts else None
        
        return {
            "posts": [{
                "post_id": p.id,
                "content": p.content,
                "like_count": p.like_count,
                "comment_count": p.comment_count,
                "view_count": p.view_count,
                "created_at": p.created_at.isoformat()
            } for p in posts],
            "last_post_id": last_id
        }

# NEW ENDPOINT: Fetch paginated comments
@app.get("/postcomments")
async def get_post_comments(post_id: int, cursor_comment_id: Optional[int] = Query(None)):
    async with AsyncSessionLocal() as session:
        query = (
            select(Comment, User.username)
            .outerjoin(User, Comment.author_telegram_id == User.telegram_id)
            .where(Comment.post_id == post_id)
        )
        if cursor_comment_id:
            query = query.where(Comment.id < cursor_comment_id)
        query = query.order_by(Comment.id.desc()).limit(20)
        
        res = await session.execute(query)
        comments = res.all()
        last_id = comments[-1][0].id if comments else None
        
        user_ids = [c[0].author_telegram_id for c in comments]
        users_map = await get_user_basics(user_ids, session)
        
        return {
            "comments": [{
                "comment_id": c.id,
                "author_id": c.author_telegram_id,
                "author_username": c_username or "anonymous",
                "parent_comment_id": c.parent_comment_id,
                "content": c.content,
                "created_at": c.created_at.isoformat()
            } for c, c_username in comments],
            "users_map": users_map,
            "last_comment_id": last_id
        }
@app.post("/follow")
async def follow_user(payload: FollowRequest, background_tasks: BackgroundTasks):
    if payload.follower_id == payload.followed_id:
        raise HTTPException(status_code=400, detail="Cannot follow yourself")
        
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.follower_id)
        
        existing = await session.get(Follow, (payload.follower_id, payload.followed_id))
        if existing:
            return {"status": 400, "message": "Already following this user"}
            
        # Add Follow Relation
        session.add(Follow(follower_id=payload.follower_id, followed_id=payload.followed_id))
        
        # Update DB Counts
        follower_user = await session.get(User, payload.follower_id)
        followed_user = await session.get(User, payload.followed_id)
        
        if follower_user: follower_user.following_count += 1
        if followed_user: followed_user.follower_count += 1
            
        # Notification System
        session.add(Notification(
            user_id=payload.followed_id,
            sender_id=payload.follower_id,
            notification_type="follow",
            content="Started following you"
        ))
        
        await session.commit()

        # Update Redis Cache Live
        if followed_user:
            await redis_client.set(f"user:{payload.followed_id}:stats", json.dumps({
                "followers": followed_user.follower_count,
                "following": followed_user.following_count
            }), ex=86400)
            await redis_client.set(f"user:{payload.followed_id}:last_notif_ts", str(datetime.utcnow().timestamp()), ex=120)

        if follower_user:
            await redis_client.set(f"user:{payload.follower_id}:stats", json.dumps({
                "followers": follower_user.follower_count,
                "following": follower_user.following_count
            }), ex=86400)

        return {"status": 200, "message": "Successfully followed user"}

@app.post("/unfollow")
async def unfollow_user(payload: FollowRequest):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.follower_id)
        
        existing = await session.get(Follow, (payload.follower_id, payload.followed_id))
        if not existing:
            return {"status": 400, "message": "Not following this user"}
            
        await session.delete(existing)
        
        follower_user = await session.get(User, payload.follower_id)
        followed_user = await session.get(User, payload.followed_id)
        
        if follower_user and follower_user.following_count > 0: 
            follower_user.following_count -= 1
        if followed_user and followed_user.follower_count > 0: 
            followed_user.follower_count -= 1
            
        await session.commit()

        # Update Redis Cache Live
        if followed_user:
            await redis_client.set(f"user:{payload.followed_id}:stats", json.dumps({"followers": followed_user.follower_count, "following": followed_user.following_count}), ex=86400)
        if follower_user:
            await redis_client.set(f"user:{payload.follower_id}:stats", json.dumps({"followers": follower_user.follower_count, "following": follower_user.following_count}), ex=86400)

        return {"status": 200, "message": "Successfully unfollowed user"}

@app.get("/followers")
async def get_followers(user_id: int, cursor_ts: Optional[float] = Query(None)):
    async with AsyncSessionLocal() as session:
        query = (
            select(User, Follow.created_at)
            .join(Follow, Follow.follower_id == User.telegram_id)
            .where(Follow.followed_id == user_id)
        )
        if cursor_ts:
            query = query.where(Follow.created_at < datetime.fromtimestamp(cursor_ts))
            
        query = query.order_by(Follow.created_at.desc()).limit(20)
        rows = (await session.execute(query)).all()
        
        last_ts = rows[-1][1].timestamp() if rows else None
        users = [r[0] for r in rows]
        
        user_ids = [u.telegram_id for u in users]
        users_map = await get_user_basics(user_ids, session)
        
        return {
            "users": [{"telegram_id": u.telegram_id, "username": u.username, "profile_score": u.profile_score_int / 100.0} for u in users],
            "users_map": users_map,
            "last_cursor_ts": last_ts
        }

@app.get("/following")
async def get_following(user_id: int, cursor_ts: Optional[float] = Query(None)):
    async with AsyncSessionLocal() as session:
        query = (
            select(User, Follow.created_at)
            .join(Follow, Follow.followed_id == User.telegram_id)
            .where(Follow.follower_id == user_id)
        )
        if cursor_ts:
            query = query.where(Follow.created_at < datetime.fromtimestamp(cursor_ts))
            
        query = query.order_by(Follow.created_at.desc()).limit(20)
        rows = (await session.execute(query)).all()
        
        last_ts = rows[-1][1].timestamp() if rows else None
        users = [r[0] for r in rows]
        
        user_ids = [u.telegram_id for u in users]
        users_map = await get_user_basics(user_ids, session)
        
        return {
            "users": [{"telegram_id": u.telegram_id, "username": u.username, "profile_score": u.profile_score_int / 100.0} for u in users],
            "users_map": users_map,
            "last_cursor_ts": last_ts
        }

@app.post("/report")
async def report_content(payload: ReportRequest):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.reported_by)
        
        rep = Report(
            target_type=payload.target_type,
            target_id=payload.target_id,
            reported_by=payload.reported_by,
            reason=payload.reason,
            expectation=payload.expectation
        )
        session.add(rep)
        await session.commit()
        return {"status": 200, "message": "Report submitted successfully to moderators"}

@app.post("/delete")
async def delete_item(payload: ItemDeleteRequest, background_tasks: BackgroundTasks):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.telegram_id)
        
        if payload.item_type == "post":
            post = await session.get(Post, payload.item_id)
            if not post or post.author_telegram_id != payload.telegram_id:
                raise HTTPException(status_code=403, detail="Unauthorized to delete post")
            await session.delete(post)
            background_tasks.add_task(remove_post_from_cache, payload.item_id)
        elif payload.item_type == "comment":
            comment = await session.get(Comment, payload.item_id)
            if not comment or comment.author_telegram_id != payload.telegram_id:
                raise HTTPException(status_code=403, detail="Unauthorized to delete comment")
            await session.delete(comment)
        
        await session.commit()
        return {"status": 200, "message": f"{payload.item_type.capitalize()} deleted successfully"}
@app.post("/deleteprofile")
async def delete_profile(telegram_id: int):
    async with AsyncSessionLocal() as session:
        user = await session.get(User, telegram_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Foreign Key child records clear karein
        await session.execute(delete(Post).where(Post.author_telegram_id == telegram_id))
        await session.execute(delete(Comment).where(Comment.author_telegram_id == telegram_id))
        await session.execute(delete(Like).where(Like.user_telegram_id == telegram_id))
        await session.execute(delete(Notification).where(or_(Notification.user_id == telegram_id, Notification.sender_id == telegram_id)))
        await session.execute(delete(DirectMessage).where(or_(DirectMessage.sender_id == telegram_id, DirectMessage.receiver_id == telegram_id)))
        await session.execute(delete(Block).where(or_(Block.blocker_id == telegram_id, Block.blocked_id == telegram_id)))
        await session.execute(delete(Report).where(Report.reported_by == telegram_id))
        
        await session.delete(user)
        await session.commit()
        return {"status": 200, "message": "User profile and all associated data deleted"}
@app.post("/sendmessage")
async def send_dm(payload: SendMessageRequest, background_tasks: BackgroundTasks):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.sender_id)
        
        block = await session.execute(
            select(Block).where(
                or_(
                    and_(Block.blocker_id == payload.sender_id, Block.blocked_id == payload.receiver_id),
                    and_(Block.blocker_id == payload.receiver_id, Block.blocked_id == payload.sender_id)
                )
            )
        )
        if block.scalars().first():
            raise HTTPException(status_code=403, detail="Messaging blocked between users")

        msg = DirectMessage(
            sender_id=payload.sender_id,
            receiver_id=payload.receiver_id,
            content=payload.content
        )
        session.add(msg)
        await session.commit()

        # Update receiver's live DM cache
        await redis_client.set(
            f"user:{payload.receiver_id}:last_dm_ts", 
            str(datetime.utcnow().timestamp()), 
            ex=120
        )
        # Notify recipient via Telegram Bot
        sender = await session.get(User, payload.sender_id)
        sender_name = sender.username if (sender and sender.username) else f"User_{payload.sender_id}"
        bot_text = f"📩 <b>New Direct Message</b> from @{sender_name}:\n<i>\"{payload.content[:50]}\"</i>"
        target_url = f"{FRONTEND_URL}/?dm={payload.sender_id}"
        background_tasks.add_task(
            send_telegram_bot_message,
            payload.receiver_id,
            bot_text,
            "Reply in App",
            target_url
        )
        
        background_tasks.add_task(increment_analytics, "dms", 1)
        return {"status": 200, "message": "Message sent"}


@app.get("/getmessages")
async def get_messages(user1: int, user2: int, cursor_msg_id: Optional[int] = Query(None)):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, user1)
        
        query = select(DirectMessage).where(
            or_(
                and_(DirectMessage.sender_id == user1, DirectMessage.receiver_id == user2),
                and_(DirectMessage.sender_id == user2, DirectMessage.receiver_id == user1)
            )
        )
        
        if cursor_msg_id:
            query = query.where(DirectMessage.id < cursor_msg_id)
            
        # Naye messages pehle layenge (desc), limit 20
        query = query.order_by(DirectMessage.id.desc()).limit(20)
        msgs = (await session.execute(query)).scalars().all()
        
        # Ab list ko ulta karenge taaki purane messages upar aur naye niche aayein
        msgs.reverse()
        
        # Sabse upar wale (oldest in this batch) ka ID cursor hoga
        last_id = msgs[0].id if msgs else None
        
        return {
            "messages": [
                {
                    "id": m.id,
                    "sender_id": m.sender_id,
                    "receiver_id": m.receiver_id,
                    "content": m.content,
                    "created_at": m.created_at.isoformat()
                } for m in msgs
            ],
            "last_msg_id": last_id
        }

@app.get("/conversations")
async def get_conversations(telegram_id: int, cursor_msg_id: Optional[int] = Query(None)):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, telegram_id)
        
        query = select(DirectMessage).where(or_(DirectMessage.sender_id == telegram_id, DirectMessage.receiver_id == telegram_id))
        if cursor_msg_id:
            query = query.where(DirectMessage.id < cursor_msg_id)
            
        query = query.order_by(DirectMessage.id.desc()).limit(150)
        res = await session.execute(query)
        msgs = res.scalars().all()
        
        partners = {}
        last_id = None
        for m in msgs:
            partner_id = m.receiver_id if m.sender_id == telegram_id else m.sender_id
            if partner_id not in partners:
                partners[partner_id] = m
            last_id = m.id
            if len(partners) == 20: # Limit to latest 20 users per batch
                break
                
        if not partners:
            return {"conversations": []}
            
        users_res = await session.execute(
            select(User.telegram_id, User.username).where(User.telegram_id.in_(list(partners.keys())))
        )
        user_map = {row[0]: row[1] for row in users_res.all()}
        
        recent_chats = []
        for partner_id, m in partners.items():
            recent_chats.append({
                "partner_id": partner_id,
                "partner_username": user_map.get(partner_id) or "anonymous",
                "last_message": m.content,
                "created_at": m.created_at.isoformat()
            })
            
        users_map = await get_user_basics(list(partners.keys()), session)
        return {"conversations": recent_chats, "users_map": users_map, "last_msg_id": last_id}

@app.post("/block")
async def block_user(payload: BlockRequest):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.blocker_id)
        
        blk = Block(blocker_id=payload.blocker_id, blocked_id=payload.blocked_id)
        session.add(blk)
        await session.commit()
        return {"status": 200, "message": "User blocked successfully"}

@app.post("/unblock")
async def unblock_user(payload: BlockRequest):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.blocker_id)
        
        stmt = delete(Block).where(
            and_(Block.blocker_id == payload.blocker_id, Block.blocked_id == payload.blocked_id)
        )
        await session.execute(stmt)
        await session.commit()
        return {"status": 200, "message": "User unblocked successfully"}

@app.post("/new")
async def create_username(payload: UsernameRequest, background_tasks: BackgroundTasks):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.telegram_id)
        
        check = await session.execute(select(User).where(User.username == payload.username))
        if check.scalars().first():
            suggestions = (await session.execute(
                select(AvailableUsername.username).limit(3)
            )).scalars().all()
            
            return {
                "status": 409,
                "message": "Username occupied",
                "suggestions": suggestions or [f"{payload.username}_{random.randint(10, 99)}" for _ in range(3)]
            }
            
        user = await session.get(User, payload.telegram_id)
        is_brand_new = False
        if not user:
            user = User(
                telegram_id=payload.telegram_id, 
                username=payload.username,
                avatar_seed=f"Bella-{payload.gender}",
                avatar_bg="c1f2d6",
                avatar_flip=False,
                avatar_scale=100
            )
            session.add(user)
            is_brand_new = True
        else:
            if not user.username or user.username.startswith("User_"):
                is_brand_new = True
            user.username = payload.username
            
            # Preserve existing avatar or set default, then append new gender
            base_seed = user.avatar_seed or "Bella"
            if '-' in base_seed and base_seed.split('-')[-1] in ['m', 'f', 'o']:
                base_seed = base_seed.rsplit('-', 1)[0]
            user.avatar_seed = f"{base_seed}-{payload.gender}"
            
        await session.commit()
        # Cache basic info for ultra-fast avatar fetching later
        user_dict = {
            "username": user.username,
            "avatar_seed": user.avatar_seed or "Bella",
            "avatar_bg": user.avatar_bg or "c1f2d6",
            "avatar_flip": user.avatar_flip or False,
            "avatar_scale": user.avatar_scale or 100
        }
        await redis_client.set(f"user:{payload.telegram_id}:basic", json.dumps(user_dict), ex=86400)

        # Send alert to Admin on new signup
        # Send alert to Admin on new signup
        admin_alert_text = (
            f"🎉 <b>New User Registered!</b>\n\n"
            f"👤 <b>Username:</b> @{payload.username}\n"
            f"🆔 <b>Telegram ID:</b> <code>{payload.telegram_id}</code>\n"
            f"⏰ <b>Time:</b> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        background_tasks.add_task(send_telegram_bot_message, ADMIN_ID, admin_alert_text)

        return {"status": 200, "message": "Username registered successfully"}

@app.post("/updateavatar")
async def update_avatar(payload: AvatarUpdateRequest):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.telegram_id)
        user = await session.get(User, payload.telegram_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Black colors strictly fallback to Mint to avoid dark patterns
        bg_color = payload.avatar_bg
        if bg_color.lower() in ["000000", "black", "1e293b"]:
            bg_color = "c1f2d6"
            
        user.avatar_seed = payload.avatar_seed
        user.avatar_bg = bg_color
        user.avatar_flip = payload.avatar_flip
        user.avatar_scale = payload.avatar_scale
        await session.commit()
        
        # Update live cache globally
        user_dict = {
            "username": user.username or f"User_{user.telegram_id}",
            "avatar_seed": user.avatar_seed,
            "avatar_bg": user.avatar_bg,
            "avatar_flip": user.avatar_flip,
            "avatar_scale": user.avatar_scale
        }
        await redis_client.set(f"user:{user.telegram_id}:basic", json.dumps(user_dict), ex=86400)
        
        return {"status": 200, "message": "Avatar updated successfully everywhere"}

@app.post("/updategender")
async def update_gender(payload: GenderUpdateRequest):
    async with AsyncSessionLocal() as session:
        user = await session.get(User, payload.telegram_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Remove old gender suffix if exists, then append new
        base_seed = user.avatar_seed or "Bella"
        if '-' in base_seed and base_seed.split('-')[-1] in ['m', 'f', 'o']:
            base_seed = base_seed.rsplit('-', 1)[0]
            
        user.avatar_seed = f"{base_seed}-{payload.gender}"
        await session.commit()
        
        # Update cache globally
        user_dict = {
            "username": user.username or f"User_{user.telegram_id}",
            "avatar_seed": user.avatar_seed,
            "avatar_bg": user.avatar_bg,
            "avatar_flip": user.avatar_flip,
            "avatar_scale": user.avatar_scale
        }
        await redis_client.set(f"user:{user.telegram_id}:basic", json.dumps(user_dict), ex=86400)
        
        return {"status": 200, "message": "Gender updated successfully"}

@app.get("/availableuser")
async def get_available_usernames():
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(AvailableUsername.username).limit(10))
        names = res.scalars().all()
        selected = random.sample(names, min(len(names), 3)) if names else []
        return {"available_usernames": selected}

@app.post("/comment")
async def post_comment(payload: CommentRequest, background_tasks: BackgroundTasks):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, payload.telegram_id)
        
        post = await session.get(Post, payload.post_id)
        if not post:
            raise HTTPException(status_code=404, detail="Post not found")
        
        new_comment = Comment(
            post_id=payload.post_id,
            author_telegram_id=payload.telegram_id,
            parent_comment_id=payload.parent_comment_id,
            content=payload.content
        )
        session.add(new_comment)
        post.comment_count += 1
        
        # Scores: Commenter gets +3.0 (+300 int), Author gets +0.06 (+6 int)
        commenter = await session.get(User, payload.telegram_id)
        if commenter:
            commenter.profile_score_int += 300
            await check_user_score_milestones(session, commenter, background_tasks)
            
        author = await session.get(User, post.author_telegram_id)
        if author:
            author.profile_score_int += 6

        # Check if commenter is Top 10% User
        if commenter and commenter.username and author:
            if await is_user_top_10_percent(session, commenter.telegram_id):
                bot_msg = f"💬 <b>Top User Commented!</b>\n@{commenter.username} (Top 10% Creator) commented on your post!"
                target_url = f"{FRONTEND_URL}/?post={post.id}"
                background_tasks.add_task(
                    send_telegram_bot_message,
                    author.telegram_id,
                    bot_msg,
                    "View Comment",
                    target_url
                )

        now_ts = str(datetime.utcnow().timestamp())
        if payload.parent_comment_id:
            parent_c = await session.get(Comment, payload.parent_comment_id)
            if parent_c:
                parent_user = await session.get(User, parent_c.author_telegram_id)
                if parent_user and parent_user.username:
                    new_comment.content = f"@{parent_user.username} {payload.content}"
                
                session.add(Notification(
                    user_id=parent_c.author_telegram_id,
                    sender_id=payload.telegram_id,
                    notification_type="reply",
                    post_id=payload.post_id,
                    content=payload.content
                ))
                await redis_client.set(f"user:{parent_c.author_telegram_id}:last_notif_ts", now_ts, ex=120)
        else:
            # Post author gets notification if someone commented
            if post.author_telegram_id != payload.telegram_id:
                await redis_client.set(f"user:{post.author_telegram_id}:last_notif_ts", now_ts, ex=120)

            mentions = re.findall(r"@(\w+)", payload.content)
            for m in set(mentions):
                m_user = (await session.execute(select(User).where(User.username == m))).scalars().first()
                if m_user:
                    m_user.mention_score += 1
                    session.add(Notification(
                        user_id=m_user.telegram_id,
                        sender_id=payload.telegram_id,
                        notification_type="mention",
                        post_id=payload.post_id,
                        content=payload.content
                    ))
                    await redis_client.set(f"user:{m_user.telegram_id}:last_notif_ts", now_ts, ex=120)
        await session.commit()
        # Redis cache update
        background_tasks.add_task(update_post_cache_stats, payload.post_id, "comment_count", 1)
        background_tasks.add_task(increment_analytics, "comments", 1)
        return {"status": 200, "message": "Comment posted"}
@app.get("/notification")
async def get_notifications(telegram_id: int, cursor_notif_id: Optional[int] = Query(None)):
    async with AsyncSessionLocal() as session:
        await update_user_activity(session, telegram_id)
        
        query = (
            select(Notification, User.username)
            .outerjoin(User, Notification.sender_id == User.telegram_id)
            .where(Notification.user_id == telegram_id)
        )
        if cursor_notif_id:
            query = query.where(Notification.id < cursor_notif_id)
            
        query = query.order_by(Notification.id.desc()).limit(20)
        res = await session.execute(query)
        notifs = res.all()
        
        last_id = notifs[-1][0].id if notifs else None
        sender_ids = [n.sender_id for n, s_username in notifs if n.sender_id]
        users_map = await get_user_basics(sender_ids, session)
        
        return {
            "notifications": [
                {
                    "notification_id": n.id,
                    "sender_id": n.sender_id,
                    "sender_username": s_username or "anonymous",
                    "type": n.notification_type,
                    "post_id": n.post_id,
                    "content": n.content,
                    "is_read": n.is_read,
                    "created_at": n.created_at.isoformat()
                } for n, s_username in notifs
            ],
            "users_map": users_map,
            "last_notif_id": last_id
        }

@app.post("/readnotifications")
async def read_notifications(payload: ReadNotificationsRequest):
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(Notification)
            .where(Notification.user_id == payload.telegram_id)
            .values(is_read=True)
        )
        await session.commit()
        return {"status": 200, "message": "Notifications marked as read"}

@app.get("/usersearch")
async def user_search(query: str):
    clean_query = query.lstrip("@").lower()
    if not clean_query:
        return {"results": []}

    async with AsyncSessionLocal() as session:
        p_res = await session.execute(
            select(User)
            .where(User.username.ilike(f"{clean_query}%"))
            .order_by(User.profile_score_int.desc())
            .limit(50)
        )
        profile_top = p_res.scalars().all()

        m_res = await session.execute(
            select(User)
            .where(User.username.ilike(f"{clean_query}%"))
            .order_by(User.mention_score.desc())
            .limit(50)
        )
        mention_top = m_res.scalars().all()

        rank_map = {}
        for idx, u in enumerate(profile_top):
            rank_map[u.telegram_id] = rank_map.get(u.telegram_id, 0) + (idx + 1)
            
        for idx, u in enumerate(mention_top):
            rank_map[u.telegram_id] = rank_map.get(u.telegram_id, 0) + (idx + 1)

        all_users = {u.telegram_id: u for u in profile_top + mention_top}
        sorted_users = sorted(all_users.values(), key=lambda u: rank_map.get(u.telegram_id, 999))
        
        top_10 = sorted_users[:10]
        user_ids = [u.telegram_id for u in top_10]
        users_map = await get_user_basics(user_ids, session)

        return {
            "results": [
                {
                    "telegram_id": u.telegram_id,
                    "username": u.username,
                    "profile_score": u.profile_score_int / 100.0,
                    "mention_score": u.mention_score
                } for u in top_10
            ],
            "users_map": users_map
        }
@app.get("/admin/analytics")
async def get_admin_analytics(date_key: Optional[str] = Query(None)):
    target_date = date_key or datetime.utcnow().strftime("%Y-%m-%d")
    async with AsyncSessionLocal() as session:
        res = await session.execute(
            select(DailyAnalytics).where(DailyAnalytics.date_key == target_date)
        )
        record = res.scalars().first()
        if not record:
            return {
                "date": target_date,
                "views": 0, "posts": 0, "comments": 0,
                "livefeed_views": 0, "homefeed_views": 0, "dms": 0
            }
            
        return {
            "date": record.date_key,
            "views": record.views,
            "posts": record.posts,
            "comments": record.comments,
            "livefeed_views": record.livefeed_views,
            "homefeed_views": record.homefeed_views,
            "dms": record.dms
        }

@app.post("/webhook")
async def telegram_webhook(update_data: Dict[str, Any], background_tasks: BackgroundTasks):
    """Handles all incoming Telegram updates/messages and replies with WebApp button."""
    message = update_data.get("message") or update_data.get("edited_message")
    if not message:
        return {"ok": True}

    chat = message.get("chat", {})
    chat_id = chat.get("id")
    from_user = message.get("from", {})
    first_name = from_user.get("first_name", "Friend")

    if chat_id:
        welcome_reply = (
            f"👋 <b>Welcome to the Future, {first_name}!</b>\n\n"
            f"✨ We are the <b>New-Age Anonymous Social Media</b> on Telegram.\n\n"
            f"🎭 <b>Express freely & tell anything anonymously</b> within safe community limits and regulations.\n"
            f"🔒 Your real identity stays private — customize your own handle & build your creator score!\n\n"
            f"👇 <i>Tap the button below to launch the WebApp and start exploring or create your profile now:</i>"
        )
        background_tasks.add_task(
            send_telegram_bot_message,
            chat_id,
            welcome_reply,
            "🚀 Launch WebApp",
            FRONTEND_URL
        )

    return {"ok": True}

@app.get("/setwebhook")
async def set_telegram_webhook():
    """Helper route to automatically set the Telegram Webhook URL."""
    webhook_url = "https://social-0axb.onrender.com/webhook"
    api_url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook?url={webhook_url}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.get(api_url)
            return res.json()
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/")
async def root_status():
    return {
        "status": "online", 
        "message": "Telegram WebApp Social Media Backend operational",
        "version": "2.0.0"
    }
