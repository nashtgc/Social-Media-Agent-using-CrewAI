import os
import logging
from typing import List, Dict, Optional, Any, Union
from datetime import datetime
import re
import json
from crewai.tools import BaseTool
from litellm import completion
import hashlib
from ..database.db_manager import DatabaseManager
from pydantic import BaseModel, Field, validator

logger = logging.getLogger(__name__)

# Constants for engagement prediction
DEFAULT_ENGAGEMENT_SCORE = 0.75
HIGH_ENGAGEMENT_SCORE = 0.9
LOW_ENGAGEMENT_SCORE = 0.4
PREDICTION_CONFIDENCE = 0.8


class ContentGeneratorSchema(BaseModel):
    digest: Dict[str, Any] = Field(description="Content digest containing the content to generate from")
    platform: str = Field(description="Target platform (must be 'linkedin' or 'twitter')")

    @validator('platform')
    def validate_platform(cls, v):
        v = str(v).strip('"\'').lower()
        if v not in ['linkedin', 'twitter']:
            raise ValueError("Platform must be either 'linkedin' or 'twitter'")
        return v

    @validator('digest')
    def validate_digest(cls, v):
        if not isinstance(v, dict):
            raise ValueError("digest must be a dictionary")
        
        content_data = v.get('content', {})
        if not isinstance(content_data, dict):
            raise ValueError("digest['content'] must be a dictionary")
        
        if 'combined_digest' not in content_data:
            raise ValueError("digest['content'] must contain 'combined_digest' key")
        
        return v


class ContentGenerator(BaseTool):
    name: str = "Generate content"
    description: str = """Generate platform-specific content for LinkedIn or Twitter.
    Required input format:
    {
        "digest": {
            "content": {
                "combined_digest": "your content summary here"
            }
        },
        "platform": "linkedin" or "twitter"
    }
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._db = DatabaseManager()
        self._prompts = {
            'linkedin': """Create a professional LinkedIn post based on this content:

{summary}

Requirements:
1. Length: 1500-2500 characters
2. Structure:
   - Compelling opening hook
   - Detailed background (2-3 paragraphs)
   - In-depth analysis
   - Key takeaways (3-4 bullet points)
   - Call-to-action question
3. Style: Professional yet conversational
4. NO markdown formatting
5. Add 3-5 relevant hashtags at the end""",

            'twitter': """Create a Twitter thread based on this content:

{summary}

Requirements:
1. Each tweet under 250 characters
2. Create 3-5 tweets that flow together
3. First tweet must hook the reader
4. Last tweet includes call-to-action
5. Add 2-3 hashtags to last tweet
6. Plain text only, no markdown
7. Separate tweets with [TWEET] marker"""
        }

    def _generate_content_hash(self, content: str) -> str:
        """Generate hash for content deduplication"""
        return hashlib.md5(content.encode()).hexdigest()

    def _generate_for_platform(self, digest: Dict, platform: str) -> Dict:
        """Generate content for a specific platform"""
        try:
            prompt = self._prompts[platform].format(summary=digest['content']['combined_digest'])
            
            response = completion(
                model="deepseek/deepseek-chat",
                messages=[{
                    "role": "user",
                    "content": prompt
                }],
                max_tokens=2000,
                temperature=0.7
            )
            
            content = response.choices[0].message.content
            
            if platform == 'twitter':
                tweets = [t.strip() for t in content.split('[TWEET]') if t.strip()]
                tweets = [t for t in tweets if len(t) <= 250]
                formatted_content = {
                    'tweets': tweets,
                    'is_thread': len(tweets) > 1,
                    'platform': 'twitter'
                }
                content_for_db = json.dumps(tweets)
            else:
                formatted_content = {
                    'text': content,
                    'platform': 'linkedin'
                }
                content_for_db = content

            try:
                source_data = {
                    'url': digest.get('url', ''),
                    'title': digest.get('title', ''),
                    'source_type': 'generated',
                    'category': platform,
                    'content_hash': self._generate_content_hash(content_for_db)
                }
                
                source = self._db.add_content_source(source_data)
                if not source:
                    raise ValueError("Failed to create content source")
                
                post_data = {
                    'platform': platform,
                    'content': content_for_db,
                    'source_id': source.id,
                    'status': 'generated'
                }
                
                post = self._db.create_post(post_data)
                if not post:
                    raise ValueError("Failed to create post history")
                
                logger.info(f"Content stored with source ID: {source.id} and post ID: {post.id}")
                
                return {
                    'success': True,
                    'content': formatted_content,
                    'source_id': source.id,
                    'post_id': post.id,
                    'platform': platform
                }
            except Exception as db_error:
                logger.error(f"Database error for {platform}: {str(db_error)}")
                return {
                    'success': True,
                    'content': formatted_content,
                    'source_id': None,
                    'post_id': None,
                    'platform': platform,
                    'db_error': str(db_error)
                }
                
        except Exception as e:
            logger.error(f"Error generating content for {platform}: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'platform': platform
            }

    def _run(self, digest: Dict, platform: Union[str, List[str]] = 'twitter') -> Dict:
        """Generate content for one or multiple platforms"""
        try:
            if not isinstance(digest, dict):
                raise ValueError("digest must be a dictionary")
            
            content_data = digest.get('content', {})
            if not isinstance(content_data, dict):
                raise ValueError("digest['content'] must be a dictionary")
            
            if 'combined_digest' not in content_data:
                raise ValueError("digest['content'] must contain 'combined_digest' key")

            platforms = platform if isinstance(platform, list) else [platform]
            platforms = [p.lower() for p in platforms]
            
            valid_platforms = ['linkedin', 'twitter']
            invalid_platforms = [p for p in platforms if p not in valid_platforms]
            if invalid_platforms:
                raise ValueError(f"Invalid platforms: {invalid_platforms}")

            results = {}
            for p in platforms:
                try:
                    result = self._generate_for_platform(digest, p)
                    results[p] = result
                except Exception as platform_error:
                    logger.error(f"Error generating for {p}: {str(platform_error)}")
                    results[p] = {
                        'success': False,
                        'error': str(platform_error),
                        'platform': p
                    }
                    continue

            return {
                'success': True,
                'results': results
            }
                
        except Exception as e:
            logger.error(f"Error in content generation: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }


class HashtagAnalyzer(BaseTool):
    name: str = "Analyze hashtags"
    description: str = "Analyze and suggest hashtags"

    def _run(self, content: str, platform: str = 'twitter',
            max_hashtags: int = 5) -> Dict:
        try:
            prompt = f"""
            Analyze this content and suggest relevant hashtags for {platform}:
            {content}
            
            Requirements:
            - Maximum {max_hashtags} hashtags
            - Relevant to content topic
            - Popular on {platform}
            - Mix of broad and specific tags
            """
            
            response = completion(
                model="deepseek/deepseek-chat",
                messages=[{
                    "role": "user",
                    "content": prompt
                }],
            )
            
            hashtags = re.findall(r'#\w+', response.choices[0].message.content)
            
            return {
                'success': True,
                'hashtags': hashtags[:max_hashtags],
                'metadata': {
                    'platform': platform,
                    'analyzed_at': datetime.now().isoformat()
                }
            }
        except Exception as e:
            logger.error(f"Error analyzing hashtags: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }


class EngagementPredictor(BaseTool):
    name: str = "Predict engagement"
    description: str = "Predict potential engagement for content"

    def _run(self, content: str, platform: str = 'twitter',
            historical_data: Optional[Dict] = None) -> Dict:
        """Predict engagement potential for content"""
        try:
            if not content:
                raise ValueError("Content cannot be empty")

            if isinstance(content, dict):
                if 'content' in content:
                    content = str(content['content'])
                elif 'text' in content:
                    content = content['text']
                else:
                    content = str(content)

            prompt = f"""
            Predict engagement potential for this {platform} content:
            {content}
            
            Consider:
            1. Content quality and relevance
            2. Timing and trends
            3. Target audience
            4. Platform-specific factors
            """
            
            if historical_data and isinstance(historical_data, dict):
                prompt += f"\nHistorical performance data:\n{json.dumps(historical_data)}"
            
            response = completion(
                model="deepseek/deepseek-chat",
                messages=[{
                    "role": "user",
                    "content": prompt
                }],
                max_tokens=500,
                temperature=0.7
            )
            
            analysis = response.choices[0].message.content
            
            engagement_score = DEFAULT_ENGAGEMENT_SCORE
            if 'high engagement' in analysis.lower():
                engagement_score = HIGH_ENGAGEMENT_SCORE
            elif 'low engagement' in analysis.lower():
                engagement_score = LOW_ENGAGEMENT_SCORE
            
            return {
                'success': True,
                'prediction': {
                    'engagement_score': engagement_score,
                    'analysis': analysis,
                    'factors': [factor.strip() for factor in analysis.split('\n') if factor.strip()],
                    'confidence': PREDICTION_CONFIDENCE
                },
                'metadata': {
                    'platform': platform,
                    'predicted_at': datetime.now().isoformat()
                }
            }
        except Exception as e:
            logger.error(f"Error predicting engagement: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
