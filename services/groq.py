import os
from typing import List, Dict, Any
from groq import Groq
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GroqService:
    """Service for handling Groq API interactions."""
    
    def __init__(self):
        self.api_key = os.getenv("GROQ_API_KEY")
        if not self.api_key:
            raise ValueError("GROQ_API_KEY environment variable is not set")
            
        self.client = Groq(api_key=self.api_key)

    def generate_content_recommendations(self, videos_data: List[Dict[str, Any]], username: str) -> Dict[str, Any]:
        """
        Generate content recommendations based on a user's top performing videos.
        
        Args:
            videos_data: List of video data containing engagement metrics
            username: TikTok username
            
        Returns:
            Dict containing content recommendations
        """
        try:
            # Format the videos data into a natural language prompt
            videos_text = []
            for i, video in enumerate(videos_data, 1):
                hashtags = " ".join([f"#{tag}" for tag in video.get("hashtags", []) if tag])
                music = f" — music by {video.get('music')}" if video.get("music") else ""
                videos_text.append(
                    f"{i}. \"{video['caption']}\" ({video['likes']} likes, {video['comments']} comments, {video['shares']} shares, {video['views']} views){music}"
                )
            
            prompt = f"""You are a TikTok content strategist.
The following are the most successful videos from the creator @{username}:

{chr(10).join(videos_text)}

Based on these videos, suggest 3 new content ideas for the creator. Each idea should include:
- A short title
- A description (what the video could show)
- Optional hashtags

Format your response as a JSON object with the following structure:
{{
    "ideas": [
        {{
            "title": "string",
            "description": "string",
            "hashtags": ["string"]
        }}
    ]
}}"""

            # Call Groq API
            completion = self.client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "You are a TikTok content strategist who provides creative and engaging content ideas."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.8,
                response_format={"type": "json_object"}
            )
            
            # Parse and return the response
            return completion.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error generating content recommendations: {str(e)}")
            raise 