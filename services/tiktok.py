import os
import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from apify_client import ApifyClient

from database import TikTokVideo, TikTokProfile
from services.apify import ApifyService

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TikTokService:
    """Service for handling TikTok video scraping and storage."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.apify = ApifyService()

    async def run_scraper(self, username: str) -> Dict[str, Any]:
        """
        Run the TikTok scraper for a given username.
        
        Args:
            username: TikTok username to scrape
            
        Returns:
            Dict containing the run details including dataset_id
        """
        try:
            # Format the username to ensure it's a valid profile identifier
            # Remove @ symbol if present and ensure it's a valid format
            formatted_username = username.strip('@')
            
            # Configure the input according to the API documentation
            run_input = {
                "profiles": [formatted_username],  # Just provide the username directly
                "resultsPerPage": 50,
                "shouldDownloadCovers": True,
                "shouldDownloadVideos": False,
                "shouldDownloadSlideshowImages": False,
                "maxProfilesPerQuery": 1
            }

            logger.info(f"Starting TikTok scraper for username: {formatted_username}")
            logger.info(f"Run input configuration: {run_input}")

            # Run the Actor and wait for it to finish
            run = self.apify.run_actor("clockworks/tiktok-scraper", run_input)
            logger.info(f"Run started with ID: {run['id']}")
            
            return run
        except Exception as e:
            logger.error(f"Error running scraper: {str(e)}")
            raise Exception(f"Apify API error: {str(e)}")

    async def wait_for_run_to_finish(self, run_id: str) -> None:
        """
        Wait for a specific run to finish.
        
        Args:
            run_id: The ID of the run to wait for
        """
        while True:
            run = self.apify.run(run_id).get()
            status = run.get("status")
            logger.info(f"Run status: {status}")
            
            if status in ["SUCCEEDED", "FAILED", "ABORTED"]:
                if status != "SUCCEEDED":
                    error_message = run.get("error", "Unknown error")
                    logger.error(f"Run failed with status: {status}, error: {error_message}")
                    raise Exception(f"Run failed with status: {status}, error: {error_message}")
                logger.info("Run completed successfully")
                break
            await asyncio.sleep(2)  # Check every 2 seconds

    async def get_dataset_items(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Get items from a dataset.
        
        Args:
            dataset_id: ID of the dataset to fetch
            
        Returns:
            List of items from the dataset
        """
        logger.info(f"Fetching dataset items from dataset ID: {dataset_id}")
        try:
            # Get the dataset items
            dataset = self.apify.dataset(dataset_id)
            items = list(dataset.list_items().items)
            
            logger.info(f"Retrieved {len(items)} items from dataset")
            
            # Log the structure of the first item if available
            if len(items) > 0:
                logger.info(f"Sample item structure: {items[0]}")
                logger.info(f"Available fields in first item: {list(items[0].keys())}")
                
                # Check if we have the expected structure
                if "authorUsername" not in items[0] and "username" not in items[0]:
                    logger.warning("The response structure doesn't match our expectations. Trying to adapt.")
                    
                    # If the items are nested differently, try to extract them
                    if "items" in items[0]:
                        logger.info("Found nested 'items' field, extracting videos from there")
                        all_videos = []
                        for item in items:
                            if "items" in item and isinstance(item["items"], list):
                                all_videos.extend(item["items"])
                        items = all_videos
                        logger.info(f"Extracted {len(items)} videos from nested structure")
            
            return items
        except Exception as e:
            logger.error(f"Error fetching dataset items: {str(e)}")
            raise

    def parse_video_data(self, video_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw video data into the format needed for our database.
        
        Args:
            video_data: Raw video data from Apify
            
        Returns:
            Parsed video data matching our model
        """
        try:
            # Log the incoming video data structure
            logger.info(f"Parsing video data: {video_data}")
            
            # Extract data from the new structure
            username = video_data.get("authorMeta", {}).get("name")
            text = video_data.get("text")
            create_time = video_data.get("createTimeISO")
            video_url = video_data.get("webVideoUrl")
            
            # Extract hashtags from the new structure
            hashtags = [tag.get("name") for tag in video_data.get("hashtags", []) if tag.get("name")]
            
            # Ensure required fields are present
            if not username:
                logger.error(f"Missing username field. Available fields: {list(video_data.keys())}")
                raise ValueError("Missing required field: username")
            if not text:
                logger.warning(f"Missing text field, using empty string. Available fields: {list(video_data.keys())}")
                text = ""
            if not create_time:
                logger.error(f"Missing createTimeISO field. Available fields: {list(video_data.keys())}")
                raise ValueError("Missing required field: createTimeISO")
            if not video_url:
                logger.error(f"Missing webVideoUrl field. Available fields: {list(video_data.keys())}")
                raise ValueError("Missing required field: webVideoUrl")
            
            # Parse the data with validation
            parsed_data = {
                "username": username,
                "caption": text,
                "hashtags": hashtags,
                "likes": int(video_data.get("diggCount", 0)),
                "comments": int(video_data.get("commentCount", 0)),
                "shares": int(video_data.get("shareCount", 0)),
                "views": int(video_data.get("playCount", 0)),
                "publish_date": datetime.fromisoformat(create_time.replace("Z", "+00:00")).replace(tzinfo=None),
                "music": video_data.get("musicMeta", {}).get("musicAuthor", ""),
                "thumbnail_url": video_data.get("videoMeta", {}).get("originalCoverUrl", ""),
                "video_url": video_url,
            }
            
            # Validate numeric fields
            if parsed_data["likes"] < 0 or parsed_data["comments"] < 0 or parsed_data["shares"] < 0:
                raise ValueError("Invalid negative value for likes, comments, or shares")
            
            logger.info(f"Successfully parsed video data: {parsed_data}")
            return parsed_data
        except Exception as e:
            logger.error(f"Error parsing video data: {str(e)}")
            raise

    async def save_videos(self, videos_data: List[Dict[str, Any]]) -> int:
        """
        Save multiple videos to the database.
        
        Args:
            videos_data: List of video data to save
            
        Returns:
            Number of videos saved
        """
        try:
            videos = []
            successful_parses = 0
            
            # First parse all videos
            for i, video_data in enumerate(videos_data):
                try:
                    parsed_data = self.parse_video_data(video_data)
                    video = TikTokVideo(**parsed_data)
                    videos.append(video)
                    successful_parses += 1
                except Exception as e:
                    logger.error(f"Error parsing video {i}: {str(e)}")
                    # Continue with the next video instead of failing the entire batch

            if not videos:
                logger.warning("No valid videos to save after parsing")
                return 0
            
            # Get all video URLs we're about to save
            video_urls = [video.video_url for video in videos]
            
            # Query existing videos with these URLs
            query = select(TikTokVideo.video_url).where(TikTokVideo.video_url.in_(video_urls))
            result = await self.db.execute(query)
            existing_urls = {row[0] for row in result}
            
            # Filter out videos that already exist
            new_videos = [video for video in videos if video.video_url not in existing_urls]
            
            if not new_videos:
                logger.info("No new videos to save - all videos already exist in database")
                return 0
                
            logger.info(f"Attempting to save {len(new_videos)} new videos to database")
            self.db.add_all(new_videos)
            await self.db.commit()
            logger.info(f"Successfully saved {len(new_videos)} new videos to database")
            return len(new_videos)
        except Exception as e:
            logger.error(f"Error saving videos to database: {str(e)}")
            await self.db.rollback()
            raise

    async def get_latest_video_date(self, username: str) -> Optional[datetime]:
        """
        Get the date of the most recent video for a user.
        
        Args:
            username: TikTok username
            
        Returns:
            Datetime of the most recent video, or None if no videos exist
        """
        try:
            query = select(TikTokVideo.publish_date).where(
                TikTokVideo.username == username
            ).order_by(TikTokVideo.publish_date.desc()).limit(1)
            
            result = await self.db.execute(query)
            latest_date = result.scalar_one_or_none()
            
            return latest_date
        except Exception as e:
            logger.error(f"Error getting latest video date: {str(e)}")
            raise

    def parse_profile_data(self, video_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse profile data from video data.
        
        Args:
            video_data: Video data containing profile information
            
        Returns:
            Parsed profile data or None if data is incomplete
        """
        try:
            author_meta = video_data.get("authorMeta", {})
            if not author_meta:
                return None
                
            return {
                "username": author_meta.get("name"),
                "verified": 1 if author_meta.get("verified") else 0,
                "private_account": 1 if author_meta.get("private") else 0,
                "region": author_meta.get("region"),
                "following": author_meta.get("following"),
                "friends": author_meta.get("friends"),
                "fans": author_meta.get("fans"),
                "heart": author_meta.get("heart"),
                "video": author_meta.get("video"),
                "avatar_url": author_meta.get("avatar"),
                "signature": author_meta.get("signature")
            }
        except Exception as e:
            logger.error(f"Error parsing profile data: {str(e)}")
            return None

    async def save_profile(self, profile_data: Dict[str, Any]) -> bool:
        """
        Save or update a TikTok profile in the database.
        
        Args:
            profile_data: Profile data to save
            
        Returns:
            True if profile was saved/updated, False otherwise
        """
        try:
            if not profile_data:
                return False
                
            # Check if profile exists
            query = select(TikTokProfile).where(TikTokProfile.username == profile_data["username"])
            result = await self.db.execute(query)
            existing_profile = result.scalar_one_or_none()
            
            if existing_profile:
                # Update existing profile
                for key, value in profile_data.items():
                    setattr(existing_profile, key, value)
            else:
                # Create new profile
                profile = TikTokProfile(**profile_data)
                self.db.add(profile)
            
            await self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Error saving profile: {str(e)}")
            await self.db.rollback()
            return False

    async def extract_and_save_profile(self, videos: List[Dict[str, Any]]) -> bool:
        """
        Extract and save profile information from video data.
        
        Args:
            videos: List of video data
            
        Returns:
            True if profile was saved/updated, False otherwise
        """
        try:
            if not videos:
                return False
                
            # Get profile data from the first video
            profile_data = self.parse_profile_data(videos[0])
            if not profile_data:
                return False
                
            return await self.save_profile(profile_data)
        except Exception as e:
            logger.error(f"Error extracting and saving profile: {str(e)}")
            return False

    async def scrape_user_videos(self, username: str) -> Dict[str, Any]:
        """
        Scrape videos for a TikTok user.
        
        Args:
            username: TikTok username to scrape
            
        Returns:
            Dict containing scraping results
        """
        try:
            # Format username
            formatted_username = username.strip('@')
            
            # Configure run input
            run_input = {
                "profiles": [formatted_username],
                "resultsPerPage": 50,
                "shouldDownloadCovers": True,
                "shouldDownloadVideos": False,
                "shouldDownloadSlideshowImages": False,
                "maxProfilesPerQuery": 1
            }

            # Run the actor and get results
            run = await self.apify.run_actor("clockworks/tiktok-scraper", run_input)
            await self.apify.wait_for_run_to_finish(run["id"])
            items = await self.apify.get_dataset_items(run["defaultDatasetId"])
            
            # Check if we have the expected structure
            if items and "authorUsername" not in items[0] and "username" not in items[0]:
                logger.warning("The response structure doesn't match our expectations. Trying to adapt.")
                if "items" in items[0]:
                    logger.info("Found nested 'items' field, extracting videos from there")
                    all_videos = []
                    for item in items:
                        if "items" in item and isinstance(item["items"], list):
                            all_videos.extend(item["items"])
                    items = all_videos
                    logger.info(f"Extracted {len(items)} videos from nested structure")
            
            # Save videos to database
            videos_saved = await self.save_videos(items)
            
            # Get the most recent video date
            latest_video_date = await self.get_latest_video_date(formatted_username)
            
            # Extract and save profile data
            profile_saved = await self.extract_and_save_profile(items)
            
            return {
                "videos_saved": videos_saved,
                "latest_video_date": latest_video_date.isoformat() if latest_video_date else None,
                "profile_saved": profile_saved,
                "run_id": run["id"]
            }
        except Exception as e:
            logger.error(f"Error scraping user videos: {str(e)}")
            raise

    async def scrape_user_profile(self, username: str) -> Dict[str, Any]:
        """
        Scrape profile information for a TikTok user.
        
        Args:
            username: TikTok username to scrape
            
        Returns:
            Dict containing profile data
        """
        try:
            # First scrape videos to get profile data
            result = await self.scrape_user_videos(username)
            videos = result["videos"]
            
            if not videos:
                raise Exception("No videos found for user")
                
            # Extract and save profile data
            profile_saved = await self.extract_and_save_profile(videos)
            
            return {
                "profile_saved": profile_saved,
                "videos_count": len(videos),
                "run_id": result["run_id"]
            }
        except Exception as e:
            logger.error(f"Error scraping user profile: {str(e)}")
            raise 