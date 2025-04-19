import os
import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from apify_client import ApifyClient

from database import TikTokVideo, TikTokProfile

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TikTokService:
    """Service for handling TikTok video scraping and storage."""
    
    def __init__(self):
        self.apify_token = os.getenv("APIFY_TOKEN")
        if not self.apify_token:
            raise ValueError("APIFY_TOKEN environment variable is not set")
            
        self.client = ApifyClient(self.apify_token)

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
            run = self.client.actor("clockworks/tiktok-scraper").call(run_input=run_input)
            logger.info(f"Run started with ID: {run.get('id')}")
            
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
            run = self.client.run(run_id).get()
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
            dataset = self.client.dataset(dataset_id)
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
                "thumbnail_url": video_data.get("videoMeta", {}).get("coverUrl", ""),
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

    async def save_videos(self, db: AsyncSession, videos_data: List[Dict[str, Any]]) -> int:
        """
        Save multiple videos to the database.
        
        Args:
            db: Database session
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
            result = await db.execute(query)
            existing_urls = {row[0] for row in result}
            
            # Filter out videos that already exist
            new_videos = [video for video in videos if video.video_url not in existing_urls]
            
            if not new_videos:
                logger.info("No new videos to save - all videos already exist in database")
                return 0
                
            logger.info(f"Attempting to save {len(new_videos)} new videos to database")
            db.add_all(new_videos)
            await db.commit()
            logger.info(f"Successfully saved {len(new_videos)} new videos to database")
            return len(new_videos)
        except Exception as e:
            logger.error(f"Error saving videos to database: {str(e)}")
            await db.rollback()
            raise

    async def get_latest_video_date(self, db: AsyncSession, username: str) -> Optional[datetime]:
        """
        Get the date of the most recent video for a user.
        
        Args:
            db: Database session
            username: TikTok username
            
        Returns:
            Datetime of the most recent video or None if no videos exist
        """
        query = select(TikTokVideo.publish_date)\
            .where(TikTokVideo.username == username)\
            .order_by(TikTokVideo.publish_date.desc())\
            .limit(1)
        
        result = await db.execute(query)
        row = result.first()
        return row[0] if row else None

    def parse_profile_data(self, video_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse TikTok profile data from the video response.
        
        Args:
            video_data: Raw video data from Apify
            
        Returns:
            Parsed profile data or None if authorMeta is missing
        """
        try:
            # Check if authorMeta exists
            author_meta = video_data.get("authorMeta")
            if not author_meta:
                logger.warning("No authorMeta found in video data")
                return None
                
            logger.info(f"Parsing profile data from authorMeta: {author_meta}")
            
            # Extract profile data
            parsed_profile = {
                "username": author_meta.get("name"),
                "verified": int(author_meta.get("verified", 0)),
                "private_account": int(author_meta.get("privateAccount", 0)),
                "region": author_meta.get("region", ""),
                "following": int(author_meta.get("following", 0)),
                "friends": int(author_meta.get("friends", 0)),
                "fans": int(author_meta.get("fans", 0)),
                "heart": int(author_meta.get("heart", 0)),
                "video": int(author_meta.get("video", 0)),
                "avatar_url": author_meta.get("avatar", ""),  # Add avatar URL from authorMeta
                "signature": author_meta.get("signature", "")  # Add signature from authorMeta
            }
            
            # Ensure username exists
            if not parsed_profile["username"]:
                logger.error("Missing username in profile data")
                return None
                
            logger.info(f"Successfully parsed profile data: {parsed_profile}")
            return parsed_profile
        except Exception as e:
            logger.error(f"Error parsing profile data: {str(e)}")
            return None
            
    async def save_profile(self, db: AsyncSession, profile_data: Dict[str, Any]) -> bool:
        """
        Save profile data to the database.
        
        Args:
            db: Database session
            profile_data: Parsed profile data
            
        Returns:
            True if profile was saved, False otherwise
        """
        try:
            # Check if profile already exists
            username = profile_data["username"]
            query = select(TikTokProfile).where(TikTokProfile.username == username)
            result = await db.execute(query)
            existing_profile = result.scalars().first()
            
            if existing_profile:
                logger.info(f"Updating existing profile for user: {username}")
                # Update existing profile
                for key, value in profile_data.items():
                    if hasattr(existing_profile, key):
                        setattr(existing_profile, key, value)
                profile = existing_profile
            else:
                logger.info(f"Creating new profile for user: {username}")
                # Create new profile
                profile = TikTokProfile(**profile_data)
                db.add(profile)
                
            await db.commit()
            logger.info(f"Successfully saved profile for: {username}")
            return True
        except Exception as e:
            logger.error(f"Error saving profile: {str(e)}")
            await db.rollback()
            return False
    
    async def extract_and_save_profile(self, db: AsyncSession, videos: List[Dict[str, Any]]) -> bool:
        """
        Extract profile data from videos and save it.
        
        Args:
            db: Database session
            videos: List of video data
            
        Returns:
            True if profile was extracted and saved, False otherwise
        """
        if not videos:
            logger.warning("No videos to extract profile from")
            return False
            
        # We only need the first video to get the profile data
        profile_data = self.parse_profile_data(videos[0])
        if not profile_data:
            logger.warning("Could not parse profile data from videos")
            return False
            
        return await self.save_profile(db, profile_data)

    async def scrape_user_videos(self, db: AsyncSession, username: str) -> Dict[str, Any]:
        """
        Main function to scrape and save videos and profile data for a user.
        
        Args:
            db: Database session
            username: TikTok username to scrape
            
        Returns:
            Summary of the operation
        """
        try:
            logger.info(f"Starting video scraping for user: {username}")
            
            # Run the scraper
            run_data = await self.run_scraper(username)
            run_id = run_data["id"]
            logger.info(f"Scraper run started with ID: {run_id}")
            
            # Wait for completion
            await self.wait_for_run_to_finish(run_id)
            
            # Get the results
            dataset_id = run_data["defaultDatasetId"]
            logger.info(f"Fetching results from dataset: {dataset_id}")
            videos = await self.get_dataset_items(dataset_id)
            
            if not videos:
                logger.warning(f"No videos found for user: {username}")
                return {
                    "username": username,
                    "videos_saved": 0,
                    "latest_video_date": None,
                    "profile_saved": False,
                    "error": "No videos found for this user"
                }
            
            # Extract and save profile data
            profile_saved = await self.extract_and_save_profile(db, videos)
            logger.info(f"Profile data saved: {profile_saved}")
            
            # Save videos to database
            saved_count = await self.save_videos(db, videos)
            
            # Get the most recent video date
            latest_video_date = await self.get_latest_video_date(db, username)
            
            logger.info(f"Scraping completed successfully for user: {username}")
            return {
                "username": username,
                "videos_saved": saved_count,
                "latest_video_date": latest_video_date.isoformat() if latest_video_date else None,
                "profile_saved": profile_saved,
                "success": True
            }
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            return {
                "username": username,
                "videos_saved": 0,
                "latest_video_date": None,
                "profile_saved": False,
                "error": str(e),
                "success": False
            }

    async def scrape_user_profile(self, db: AsyncSession, username: str) -> Dict[str, Any]:
        """
        Scrape only the profile information for a TikTok user without fetching videos.
        Uses the Apify TikTok Profile Scraper.
        
        Args:
            db: Database session
            username: TikTok username to scrape
            
        Returns:
            Profile data and operation summary
        """
        try:
            logger.info(f"Starting profile scraping for user: {username}")
            
            # Format the username to ensure it's a valid profile identifier
            formatted_username = username.strip('@')
            
            # Configure the input for the TikTok Profile Scraper
            run_input = {
                "profiles": [formatted_username],
                "shouldDownloadCovers": False,
                "shouldDownloadVideos": False,
                "shouldDownloadSlideshowImages": False,
                "shouldDownloadSubtitles": False,
                "resultsPerPage": 1  # We only need one result to get the profile
            }
            
            # Run the TikTok Profile Scraper actor
            logger.info(f"Starting TikTok Profile Scraper for username: {formatted_username}")
            run = self.client.actor("clockworks/tiktok-profile-scraper").call(run_input=run_input)
            run_id = run.get("id")
            logger.info(f"Profile scraper run started with ID: {run_id}")
            
            # Wait for completion
            await self.wait_for_run_to_finish(run_id)
            
            # Get the results
            dataset_id = run.get("defaultDatasetId")
            logger.info(f"Fetching profile results from dataset: {dataset_id}")
            
            # Get the dataset items
            dataset = self.client.dataset(dataset_id)
            items = list(dataset.list_items().items)
            
            if not items:
                logger.warning(f"No profile data found for user: {username}")
                return {
                    "username": username,
                    "profile_data": None,
                    "profile_updated": False,
                    "error": "No profile data found for this user",
                    "success": False
                }
            
            # Extract profile data from the first item
            profile_data = self.parse_profile_data(items[0])
            if not profile_data:
                logger.warning(f"Could not parse profile data for user: {username}")
                return {
                    "username": username,
                    "profile_data": None,
                    "profile_updated": False,
                    "error": "Could not parse profile data",
                    "success": False
                }
            
            # Update the database if needed
            profile_updated = await self.save_profile(db, profile_data)
            logger.info(f"Profile data updated in database: {profile_updated}")
            
            # Return the profile data directly
            return {
                "username": username,
                "profile_data": profile_data,
                "profile_updated": profile_updated,
                "success": True
            }
        except Exception as e:
            logger.error(f"Error during profile scraping: {str(e)}")
            return {
                "username": username,
                "profile_data": None,
                "profile_updated": False,
                "error": str(e),
                "success": False
            } 