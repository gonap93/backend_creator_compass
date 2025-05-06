import logging
from typing import Dict, Any, List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from datetime import datetime, timezone

from database import InstagramProfile, InstagramPost
from services.apify import ApifyService

logger = logging.getLogger(__name__)

class InstagramService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.apify = ApifyService()

    async def scrape_profile(self, username: str) -> Dict[str, Any]:
        """
        Scrape an Instagram profile and its recent posts.
        
        Args:
            username: Instagram username to scrape
            
        Returns:
            Dict containing the scraped data
        """
        try:
            # Format username
            formatted_username = username.strip('@')
            
            # Configure run input
            run_input = {
                "addParentData": False,
                "directUrls": [f"https://www.instagram.com/{formatted_username}/"],
                "enhanceUserSearchWithFacebookPage": False,
                "isUserReelFeedURL": False,
                "isUserTaggedFeedURL": False,
                "resultsLimit": 200,
                "resultsType": "details",
                "searchLimit": 1,
                "searchType": "hashtag"
            }

            # Run the actor and get results
            run = await self.apify.run_actor("apify/instagram-scraper", run_input)
            await self.apify.wait_for_run_to_finish(run["id"])
            items = await self.apify.get_dataset_items(run["defaultDatasetId"])
            
            if not items:
                raise Exception("No results returned from Apify")
            
            # Parse and save profile data
            profile_data = self.parse_profile_data(items[0])
            if profile_data:
                profile_saved = await self.save_profile(profile_data)
            else:
                profile_saved = False
                logger.warning("Could not parse profile data from response")
            
            # Parse and save posts
            posts_data = self.parse_posts_data(items[0])
            posts_saved = await self.save_posts(posts_data) if posts_data else 0
            
            return {
                "profile_saved": profile_saved,
                "posts_saved": posts_saved,
                "run_id": run["id"]
            }

        except Exception as e:
            logger.error(f"Error scraping Instagram profile: {str(e)}")
            raise

    def parse_profile_data(self, profile_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse Instagram profile data from Apify response.
        
        Args:
            profile_data: Raw profile data from Apify
            
        Returns:
            Parsed profile data or None if data is incomplete
        """
        try:
            if not profile_data:
                return None
                
            return {
                "id": profile_data.get("id"),
                "username": profile_data.get("username"),
                "full_name": profile_data.get("fullName"),
                "biography": profile_data.get("biography"),
                "followers_count": profile_data.get("followersCount", 0),
                "following_count": profile_data.get("followsCount", 0),
                "posts_count": profile_data.get("postsCount", 0),
                "avatar_url": profile_data.get("profilePicUrl"),
                "is_verified": 1 if profile_data.get("verified") else 0,
                "is_private": 1 if profile_data.get("private") else 0
            }
        except Exception as e:
            logger.error(f"Error parsing profile data: {str(e)}")
            return None

    def parse_posts_data(self, profile_data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """
        Parse Instagram posts data from Apify response.
        
        Args:
            profile_data: Raw profile data containing posts
            
        Returns:
            List of parsed posts or None if no posts found
        """
        try:
            posts = profile_data.get("latestPosts", [])
            if not posts:
                return None
                
            parsed_posts = []
            for post in posts:
                # Handle carousel posts (multiple media items)
                children = []
                if post.get("childPosts"):
                    for child in post["childPosts"]:
                        children.append({
                            "type": child.get("type"),
                            "display_url": child.get("displayUrl"),
                            "video_url": child.get("videoUrl")
                        })
                
                # Convert timestamp string to timezone-naive UTC datetime
                timestamp_str = post.get("timestamp")
                if timestamp_str:
                    # First convert to timezone-aware datetime
                    timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    # Then convert to timezone-naive UTC datetime
                    timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)
                else:
                    timestamp = None
                
                parsed_post = {
                    "id": post.get("id"),
                    "profile_id": profile_data.get("id"),
                    "shortcode": post.get("shortCode"),
                    "caption": post.get("caption"),
                    "likes": post.get("likesCount", 0),
                    "comments": post.get("commentsCount", 0),
                    "image_url": post.get("displayUrl"),
                    "post_url": post.get("url"),
                    "timestamp": timestamp
                }
                parsed_posts.append(parsed_post)
            
            return parsed_posts
        except Exception as e:
            logger.error(f"Error parsing posts data: {str(e)}")
            return None

    async def save_profile(self, profile_data: Dict[str, Any]) -> bool:
        """
        Save or update an Instagram profile in the database.
        
        Args:
            profile_data: Profile data to save
            
        Returns:
            True if profile was saved/updated, False otherwise
        """
        try:
            if not profile_data:
                return False
                
            # Check if profile exists
            query = select(InstagramProfile).where(InstagramProfile.username == profile_data["username"])
            result = await self.db.execute(query)
            existing_profile = result.scalar_one_or_none()
            
            if existing_profile:
                # Update existing profile
                for key, value in profile_data.items():
                    setattr(existing_profile, key, value)
            else:
                # Create new profile
                profile = InstagramProfile(**profile_data)
                self.db.add(profile)
            
            await self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Error saving profile: {str(e)}")
            await self.db.rollback()
            return False

    async def save_posts(self, posts_data: List[Dict[str, Any]]) -> int:
        """
        Save Instagram posts to the database.
        
        Args:
            posts_data: List of post data to save
            
        Returns:
            Number of posts saved
        """
        try:
            if not posts_data:
                return 0
                
            posts = []
            for post_data in posts_data:
                # Remove any fields that don't exist in the InstagramPost model
                valid_fields = {
                    "id", "profile_id", "shortcode", "caption", "likes", "comments",
                    "image_url", "post_url", "timestamp"
                }
                filtered_data = {k: v for k, v in post_data.items() if k in valid_fields}
                post = InstagramPost(**filtered_data)
                posts.append(post)
            
            # Get all post IDs we're about to save
            post_ids = [post.id for post in posts]
            
            # Query existing posts with these IDs
            query = select(InstagramPost.id).where(InstagramPost.id.in_(post_ids))
            result = await self.db.execute(query)
            existing_ids = {row[0] for row in result}
            
            # Filter out posts that already exist
            new_posts = [post for post in posts if post.id not in existing_ids]
            
            if not new_posts:
                logger.info("No new posts to save - all posts already exist in database")
                return 0
                
            logger.info(f"Attempting to save {len(new_posts)} new posts to database")
            self.db.add_all(new_posts)
            await self.db.commit()
            logger.info(f"Successfully saved {len(new_posts)} new posts to database")
            return len(new_posts)
        except Exception as e:
            logger.error(f"Error saving posts: {str(e)}")
            await self.db.rollback()
            raise 