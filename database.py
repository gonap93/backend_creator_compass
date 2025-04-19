from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, Text, Enum, ARRAY
import os
from dotenv import load_dotenv
from datetime import datetime
from system_messages import Tone
import uuid

load_dotenv()  # only works locally, won’t affect Railway

# Get database URL from environment variable or use default
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://gonap93:gonap93@localhost:5432/chat_history")

# Create async engine
engine = create_async_engine(DATABASE_URL, echo=True)

# Create async session factory
AsyncSessionLocal = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

# Create base class for models
Base = declarative_base()

class ChatHistory(Base):
    __tablename__ = "chat_history"

    id = Column(Integer, primary_key=True, index=True)
    message = Column(Text, nullable=False)
    response = Column(Text, nullable=False)
    tone = Column(Enum(Tone), nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)

class TikTokVideo(Base):
    __tablename__ = "tiktok_videos"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    username = Column(String, nullable=False)
    caption = Column(Text, nullable=False)
    hashtags = Column(ARRAY(String), nullable=True)
    likes = Column(Integer, nullable=False)
    comments = Column(Integer, nullable=False)
    shares = Column(Integer, nullable=False)
    views = Column(Integer, nullable=False)
    publish_date = Column(DateTime, nullable=False)
    music = Column(String, nullable=True)
    thumbnail_url = Column(String, nullable=True)
    video_url = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class TikTokProfile(Base):
    __tablename__ = "tiktok_profile"

    username = Column(String, primary_key=True, index=True)
    verified = Column(Integer, nullable=True)  # Boolean represented as 0/1
    private_account = Column(Integer, nullable=True)  # Boolean represented as 0/1
    region = Column(String, nullable=True)
    following = Column(Integer, nullable=True)
    friends = Column(Integer, nullable=True)
    fans = Column(Integer, nullable=True)
    heart = Column(Integer, nullable=True)  # Total likes received
    video = Column(Integer, nullable=True)  # Total video count
    avatar_url = Column(String, nullable=True)  # URL to the user's avatar
    signature = Column(String, nullable=True)  # User's bio/signature
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# Dependency to get DB session
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

# Create tables
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all) 