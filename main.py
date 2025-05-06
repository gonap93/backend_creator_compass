from typing import Union, List, Optional, Dict, Any
import os
import logging
import json
from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
from groq import Groq
from system_messages import Tone, SYSTEM_MESSAGES
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db, init_db, ChatHistory, TikTokVideo, TikTokProfile, InstagramProfile, InstagramPost
from sqlalchemy import select, func
from dotenv import load_dotenv
from services.tiktok import TikTokService
from services.groq import GroqService
from services.instagram import InstagramService

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variable
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise ValueError("API_KEY environment variable is not set")

# Create API key header
api_key_header = APIKeyHeader(name="X-API-Key")

async def get_api_key(api_key: str = Depends(api_key_header)):
    logger.info(f"API key validation - Received key: {api_key[:4]}... (first 4 chars only)")
    logger.info(f"Expected API key first 4 chars: {API_KEY[:4]}...")
    
    if api_key != API_KEY:
        logger.warning(f"API Key validation failed - Key lengths: received={len(api_key)}, expected={len(API_KEY)}")
        logger.warning("API Key validation failed - Headers might be missing or incorrect")
        raise HTTPException(
            status_code=403,
            detail="Invalid API Key"
        )
    logger.info("API Key validation successful")
    return api_key

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Your Next.js frontend URL
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Middleware to log request details
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Request path: {request.url.path}")
    logger.info(f"Request method: {request.method}")
    
    # Log headers
    headers = dict(request.headers)
    # Redact sensitive information
    if "authorization" in headers:
        headers["authorization"] = "REDACTED"
    if "x-api-key" in headers:
        headers["x-api-key"] = f"{headers['x-api-key'][:4]}... (first 4 chars only)"
    
    logger.info(f"Request headers: {headers}")
    
    response = await call_next(request)
    logger.info(f"Response status code: {response.status_code}")
    
    return response

# Initialize Groq client
client = Groq(api_key=os.getenv('GROQ_API_KEY'))

class ChatRequest(BaseModel):
    message: str
    tone: Tone = Tone.INFORMATIVE  # Default to informative if not specified

class ChatResponse(BaseModel):
    response: str
    history_id: int

class Item(BaseModel):
    name: str
    price: float
    is_offer: Union[bool, None] = None

class TikTokScrapeRequest(BaseModel):
    username: str

class TikTokScrapeResponse(BaseModel):
    username: str
    videos_saved: int
    latest_video_date: Union[str, None]
    profile_saved: bool = False
    error: Optional[str] = None
    success: bool = True

class TikTokProfileScrapeResponse(BaseModel):
    username: str
    profile_data: Optional[Dict[str, Any]] = None
    profile_updated: bool = False
    error: Optional[str] = None
    success: bool = True

class TikTokRecommendationsRequest(BaseModel):
    username: str

class ContentIdea(BaseModel):
    title: str
    description: str
    hashtags: List[str]

class TikTokRecommendationsResponse(BaseModel):
    username: str
    ideas: List[ContentIdea]

class TikTokProfileResponse(BaseModel):
    username: str
    verified: bool = False
    private_account: bool = False
    region: Union[str, None] = None
    following: int = 0
    friends: int = 0
    fans: int = 0
    heart: int = 0
    video: int = 0
    avatar_url: Union[str, None] = None
    signature: Union[str, None] = None

class TikTokVideoResponse(BaseModel):
    id: str
    username: str
    caption: str
    hashtags: Optional[List[str]] = None
    likes: int
    comments: int
    shares: int
    views: int
    publish_date: str
    music: Optional[str] = None
    thumbnail_url: Optional[str] = None
    video_url: str

class InstagramScrapeRequest(BaseModel):
    username: str

class InstagramScrapeResponse(BaseModel):
    username: str
    posts_saved: int
    profile_saved: bool = False
    error: Optional[str] = None
    success: bool = True

class InstagramProfileResponse(BaseModel):
    username: str
    full_name: Optional[str] = None
    biography: Optional[str] = None
    followers_count: int = 0
    following_count: int = 0
    posts_count: int = 0
    is_private: bool = False
    is_verified: bool = False
    avatar_url: Optional[str] = None

class InstagramPostResponse(BaseModel):
    id: str
    username: str
    caption: Optional[str] = None
    likes: int
    comments: int
    media_type: str
    image_url: str
    permalink: str
    timestamp: str
    children: Optional[List[Dict[str, Any]]] = None

@app.on_event("startup")
async def startup_event():
    await init_db()
    logger.info("Database initialized")

@app.get("/")
def read_root():
    logger.info("Root endpoint accessed")
    return {"Hello": "World"}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    logger.info(f"Reading item {item_id} with query param: {q}")
    return {"item_id": item_id, "q": q}

@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    logger.info(f"Updating item {item_id} with data: {item.dict()}")
    return {"item_name": item.name, "item_id": item_id}

@app.post("/chat", response_model=ChatResponse)
async def chat_completion(
    request: ChatRequest, 
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    try:
        logger.info(f"Processing chat request with tone: {request.tone}")
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": SYSTEM_MESSAGES[request.tone]
                },
                {
                    "role": "user",
                    "content": request.message
                }
            ],
            model="llama-3.3-70b-versatile"
        )
        response = chat_completion.choices[0].message.content
        
        # Store in database
        chat_history = ChatHistory(
            message=request.message,
            response=response,
            tone=request.tone
        )
        db.add(chat_history)
        await db.commit()
        await db.refresh(chat_history)
        
        logger.info("Chat completion successful and stored in database")
        return ChatResponse(
            response=response,
            history_id=chat_history.id
        )
    except Exception as e:
        logger.error(f"Error in chat completion: {str(e)}", exc_info=True)
        return {"error": str(e)}

@app.get("/chat/history")
async def get_chat_history(
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    try:
        result = await db.execute(
            select(ChatHistory).order_by(ChatHistory.timestamp.desc())
        )
        history = result.scalars().all()
        return [
            {
                "id": chat.id,
                "message": chat.message,
                "response": chat.response,
                "tone": chat.tone,
                "timestamp": chat.timestamp.isoformat()
            }
            for chat in history
        ]
    except Exception as e:
        logger.error(f"Error fetching chat history: {str(e)}", exc_info=True)
        return {"error": str(e)}

@app.post("/tiktok/scrape-posts", response_model=TikTokScrapeResponse)
async def scrape_tiktok_videos(
    request: TikTokScrapeRequest,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """
    Scrape TikTok videos and profile for a specific username.
    
    Args:
        request: Request containing the username to scrape
        db: Database session
        
    Returns:
        Summary of the scraping operation
    """
    logger.info(f"Received request to scrape TikTok videos for username: {request.username}")
    
    try:
        tiktok_service = TikTokService(db)
        result = await tiktok_service.scrape_user_videos(request.username)
        
        return TikTokScrapeResponse(
            username=request.username,
            videos_saved=result.get("videos_saved", 0),
            latest_video_date=result.get("latest_video_date"),
            profile_saved=result.get("profile_saved", False),
            error=result.get("error"),
            success=True
        )
    except Exception as e:
        logger.error(f"Error scraping TikTok videos: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tiktok/scrape-profile", response_model=TikTokProfileScrapeResponse)
async def scrape_tiktok_profile(
    request: TikTokScrapeRequest,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """
    Scrape only the TikTok profile for a specific username without fetching videos.
    Also updates the profile in the database if there are changes.
    
    Args:
        request: Request containing the username to scrape
        db: Database session
        
    Returns:
        TikTok profile data and update status
    """
    logger.info(f"Received request to scrape TikTok profile for username: {request.username}")
    
    try:
        tiktok_service = TikTokService(db)
        result = await tiktok_service.scrape_user_profile(request.username)
        
        return TikTokProfileScrapeResponse(
            username=request.username,
            profile_data=result.get("profile_data"),
            profile_updated=result.get("profile_saved", False),
            error=result.get("error"),
            success=True
        )
    except Exception as e:
        logger.error(f"Error scraping TikTok profile: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tiktok/recommendations", response_model=TikTokRecommendationsResponse)
async def get_tiktok_recommendations(
    request: TikTokRecommendationsRequest,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """
    Get content recommendations for a TikTok user based on their top performing videos.
    
    Args:
        request: Request containing the TikTok username
        db: Database session
        
    Returns:
        Content recommendations generated by Groq
    """
    try:
        # Query the database for the user's top 5 videos by total engagement
        query = select(TikTokVideo).where(
            TikTokVideo.username == request.username
        ).order_by(
            (TikTokVideo.likes + TikTokVideo.comments + TikTokVideo.shares + TikTokVideo.views).desc()
        ).limit(30)
        
        result = await db.execute(query)
        videos = result.scalars().all()
        
        if not videos:
            raise HTTPException(
                status_code=404,
                detail=f"No videos found for user: {request.username}"
            )
        
        # Format videos data for Groq
        videos_data = [
            {
                "caption": video.caption,
                "likes": video.likes,
                "comments": video.comments,
                "shares": video.shares,
                "views": video.views,
                "hashtags": video.hashtags,
                "music": video.music
            }
            for video in videos
        ]
        
        # Generate recommendations using Groq
        groq_service = GroqService()
        recommendations = groq_service.generate_content_recommendations(
            videos_data,
            request.username
        )
        
        # Parse the JSON response
        recommendations_data = json.loads(recommendations)
        
        return {
            "username": request.username,
            "ideas": recommendations_data["ideas"]
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error generating recommendations: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/tiktok/profile/{username}", response_model=TikTokProfileResponse)
async def get_tiktok_profile(
    username: str,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """
    Get TikTok profile information for a specific username.
    
    Args:
        username: TikTok username
        db: Database session
        
    Returns:
        TikTok profile data
    """
    logger.info(f"Getting TikTok profile for username: {username}")
    try:
        query = select(TikTokProfile).where(TikTokProfile.username == username)
        result = await db.execute(query)
        profile = result.scalars().first()
        
        if not profile:
            raise HTTPException(
                status_code=404,
                detail=f"Profile not found for user: {username}"
            )
        
        return TikTokProfileResponse(
            username=profile.username,
            verified=bool(profile.verified),
            private_account=bool(profile.private_account),
            region=profile.region,
            following=profile.following,
            friends=profile.friends,
            fans=profile.fans,
            heart=profile.heart,
            video=profile.video,
            avatar_url=profile.avatar_url,
            signature=profile.signature
        )
    except HTTPException as e:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Error fetching TikTok profile: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/tiktok/videos/{username}", response_model=List[TikTokVideoResponse])
async def get_tiktok_videos(
    username: str,
    limit: int = 50,
    offset: int = 0,
    sort_by: str = "publish_date",
    order: str = "desc",
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """
    Get videos for a specific TikTok user.
    
    Args:
        username: TikTok username
        limit: Maximum number of videos to return (default: 50)
        offset: Number of videos to skip (for pagination)
        sort_by: Field to sort by (publish_date, likes, views, etc.)
        order: Sort order (asc or desc)
        db: Database session
        
    Returns:
        List of TikTok videos
    """
    logger.info(f"Getting videos for username: {username}, limit: {limit}, offset: {offset}, sort_by: {sort_by}, order: {order}")
    try:
        # Validate sort_by field
        valid_sort_fields = ["publish_date", "likes", "comments", "shares", "views"]
        if sort_by not in valid_sort_fields:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid sort_by field. Must be one of: {', '.join(valid_sort_fields)}"
            )
            
        # Validate order
        if order not in ["asc", "desc"]:
            raise HTTPException(
                status_code=400,
                detail="Invalid order. Must be 'asc' or 'desc'"
            )
            
        # Build the query
        query = select(TikTokVideo).where(TikTokVideo.username == username)
        
        # Apply sorting
        if sort_by == "publish_date":
            query = query.order_by(TikTokVideo.publish_date.desc() if order == "desc" else TikTokVideo.publish_date.asc())
        elif sort_by == "likes":
            query = query.order_by(TikTokVideo.likes.desc() if order == "desc" else TikTokVideo.likes.asc())
        elif sort_by == "comments":
            query = query.order_by(TikTokVideo.comments.desc() if order == "desc" else TikTokVideo.comments.asc())
        elif sort_by == "shares":
            query = query.order_by(TikTokVideo.shares.desc() if order == "desc" else TikTokVideo.shares.asc())
        elif sort_by == "views":
            query = query.order_by(TikTokVideo.views.desc() if order == "desc" else TikTokVideo.views.asc())
            
        # Apply pagination
        query = query.offset(offset).limit(limit)
        
        # Execute query
        result = await db.execute(query)
        videos = result.scalars().all()
        
        if not videos:
            raise HTTPException(
                status_code=404,
                detail=f"No videos found for user: {username}"
            )
            
        # Convert to response model
        return [
            TikTokVideoResponse(
                id=video.id,
                username=video.username,
                caption=video.caption,
                hashtags=video.hashtags,
                likes=video.likes,
                comments=video.comments,
                shares=video.shares,
                views=video.views,
                publish_date=video.publish_date.isoformat(),
                music=video.music,
                thumbnail_url=video.thumbnail_url,
                video_url=video.video_url
            )
            for video in videos
        ]
    except HTTPException as e:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Error fetching TikTok videos: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/instagram/scrape-posts", response_model=InstagramScrapeResponse)
async def scrape_instagram(
    request: InstagramScrapeRequest,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """
    Scrape Instagram profile and posts for a specific username.
    
    Args:
        request: Request containing the username to scrape
        db: Database session
        
    Returns:
        Summary of the scraping operation
    """
    logger.info(f"Received request to scrape Instagram for username: {request.username}")
    
    try:
        instagram_service = InstagramService(db)
        result = await instagram_service.scrape_profile(request.username)
        
        return InstagramScrapeResponse(
            username=request.username,
            posts_saved=result.get("posts_saved", 0),
            profile_saved=result.get("profile_saved", False),
            error=result.get("error"),
            success=True
        )
    except Exception as e:
        logger.error(f"Error scraping Instagram: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/instagram/profile/{username}", response_model=InstagramProfileResponse)
async def get_instagram_profile(
    username: str,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """
    Get Instagram profile information for a specific username.
    
    Args:
        username: Instagram username
        db: Database session
        
    Returns:
        Instagram profile data
    """
    logger.info(f"Getting Instagram profile for username: {username}")
    try:
        query = select(InstagramProfile).where(InstagramProfile.username == username)
        result = await db.execute(query)
        profile = result.scalars().first()
        
        if not profile:
            raise HTTPException(
                status_code=404,
                detail=f"Profile not found for user: {username}"
            )
        
        return InstagramProfileResponse(
            username=profile.username,
            full_name=profile.full_name,
            biography=profile.biography,
            followers_count=profile.followers_count,
            following_count=profile.following_count,
            posts_count=profile.posts_count,
            is_private=bool(profile.is_private),
            is_verified=bool(profile.is_verified),
            avatar_url=profile.avatar_url
        )
    except HTTPException as e:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Error fetching Instagram profile: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/instagram/posts/{username}", response_model=List[InstagramPostResponse])
async def get_instagram_posts(
    username: str,
    limit: int = 50,
    offset: int = 0,
    sort_by: str = "timestamp",
    order: str = "desc",
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """
    Get posts for a specific Instagram user.
    
    Args:
        username: Instagram username
        limit: Maximum number of posts to return (default: 50)
        offset: Number of posts to skip (for pagination)
        sort_by: Field to sort by (timestamp, like_count, comment_count)
        order: Sort order (asc or desc)
        db: Database session
        
    Returns:
        List of Instagram posts
    """
    logger.info(f"Getting posts for username: {username}, limit: {limit}, offset: {offset}, sort_by: {sort_by}, order: {order}")
    try:
        # Validate sort_by field
        valid_sort_fields = ["timestamp", "likes", "comments"]
        if sort_by not in valid_sort_fields:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid sort_by field. Must be one of: {', '.join(valid_sort_fields)}"
            )
            
        # Validate order
        if order not in ["asc", "desc"]:
            raise HTTPException(
                status_code=400,
                detail="Invalid order. Must be 'asc' or 'desc'"
            )
            
        # Build the query with a join to InstagramProfile
        query = select(InstagramPost).join(
            InstagramProfile,
            InstagramPost.profile_id == InstagramProfile.id
        ).where(InstagramProfile.username == username)
        
        # Apply sorting
        if sort_by == "timestamp":
            query = query.order_by(InstagramPost.timestamp.desc() if order == "desc" else InstagramPost.timestamp.asc())
        elif sort_by == "likes":
            query = query.order_by(InstagramPost.likes.desc() if order == "desc" else InstagramPost.likes.asc())
        elif sort_by == "comments":
            query = query.order_by(InstagramPost.comments.desc() if order == "desc" else InstagramPost.comments.asc())
            
        # Apply pagination
        query = query.offset(offset).limit(limit)
        
        # Execute query
        result = await db.execute(query)
        posts = result.scalars().all()
        
        if not posts:
            raise HTTPException(
                status_code=404,
                detail=f"No posts found for user: {username}"
            )
            
        # Convert to response model
        return [
            InstagramPostResponse(
                id=post.id,
                username=username,  # Use the username from the request
                caption=post.caption,
                likes=post.likes,
                comments=post.comments,
                media_type="image",  # Default to image since we don't store media type
                image_url=post.image_url,
                permalink=post.post_url,
                timestamp=post.timestamp.isoformat(),
                children=None  # We don't store children in the database
            )
            for post in posts
        ]
    except HTTPException as e:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Error fetching Instagram posts: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")