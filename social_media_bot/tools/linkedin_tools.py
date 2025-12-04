import os
import json
import logging
from datetime import datetime
from typing import Dict, Optional, Any, Type, ClassVar
from pydantic import BaseModel, Field, PrivateAttr, validator
from crewai.tools import BaseTool
from ..database.db_manager import DatabaseManager
import requests
import hashlib

logger = logging.getLogger(__name__)


class ContentTooLong(requests.RequestException):
    """LinkedIn post limit reached"""
    pass


class LinkedInPosterSchema(BaseModel):
    content: Dict[str, Any] = Field(description="Content to post. Must be a dictionary with 'text' key")

    @validator('content', pre=True)
    def validate_content(cls, v):
        if isinstance(v, str):
            try:
                v = json.loads(v)
                if isinstance(v.get('content'), str):
                    try:
                        v['content'] = json.loads(v['content'])
                    except json.JSONDecodeError:
                        pass
            except json.JSONDecodeError:
                v = {"text": v}
        
        if not isinstance(v, dict):
            raise ValueError("Content must be a dictionary or valid JSON string")
        
        if 'text' not in v:
            if 'content' in v and isinstance(v['content'], dict) and 'text' in v['content']:
                v = {'text': v['content']['text']}
            elif any(key in v for key in ['content', 'message']):
                post_content = v.get('content') or v.get('message')
                if isinstance(post_content, dict) and 'text' in post_content:
                    v = {'text': post_content['text']}
                else:
                    v = {'text': str(post_content)}
            else:
                raise ValueError("Content dictionary must contain 'text' key")
        
        return v


class LinkedInPoster(BaseTool):
    """Tool for posting content to LinkedIn"""
    name: str = "Post to LinkedIn"
    description: str = "Post content to LinkedIn. Content should be a dictionary with 'text' key."
    args_schema: Type[BaseModel] = LinkedInPosterSchema
    db_session: Any = Field(description="Database session for storing post history")
    
    POST_CHAR_LIMIT: ClassVar[int] = 3000
    BASE_URL: ClassVar[str] = "https://www.linkedin.com"
    POST_ENDPOINT: ClassVar[str] = BASE_URL + "/voyager/api/contentcreation/normShares"
    
    _cookies: Dict[str, str] = PrivateAttr()
    _headers: Dict[str, str] = PrivateAttr()
    
    def __init__(self, db_session: Any, **data):
        data["db_session"] = db_session
        super().__init__(**data)
        
        self._cookies = {
            "JSESSIONID": os.getenv("LINKEDIN_JSESSIONID", "").strip(),
            "li_at": os.getenv("LINKEDIN_LI_AT", "").strip()
        }
        
        if '\"' in self._cookies["JSESSIONID"]:
            self._cookies["JSESSIONID"] = self._cookies["JSESSIONID"].replace('\"', '')
        
        self._headers = {
            "accept": "application/vnd.linkedin.normalized+json+2.1",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json; charset=UTF-8",
            "csrf-token": self._cookies["JSESSIONID"],
            "origin": self.BASE_URL,
            "cookie": '; '.join([f'{key}="{value}"' if key == "JSESSIONID" else f'{key}={value}' 
                               for key, value in self._cookies.items()]),
            "Referer": self.BASE_URL + "/feed/",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
    
    def _check_session(self, resp_headers=None):
        """Check and update session cookies if needed"""
        try:
            if not resp_headers:
                response = requests.get(self.BASE_URL, headers=self._headers)
                response.raise_for_status()
                resp_headers = response.headers
            
            if "Set-Cookie" in resp_headers and "li_at=" in resp_headers['Set-Cookie']:
                cookie_parts = resp_headers['Set-Cookie'].split(';')
                has_updates = False
                
                for cookie_key in ["JSESSIONID", "li_at"]:
                    if f"{cookie_key}=" in resp_headers['Set-Cookie']:
                        found_cookie = next((part for part in cookie_parts if f"{cookie_key}=" in part), None)
                        
                        if found_cookie:
                            new_value = found_cookie.split(f"{cookie_key}=")[1].split(';')[0].strip().replace('\"', '')
                            
                            if new_value and self._cookies[cookie_key] != new_value:
                                self._cookies[cookie_key] = new_value
                                has_updates = True
                
                if has_updates:
                    self._headers["cookie"] = '; '.join([f'{key}="{value}"' if key == "JSESSIONID" else f'{key}={value}' 
                                                       for key, value in self._cookies.items()])
                    self._headers["csrf-token"] = self._cookies["JSESSIONID"]
                    
        except requests.exceptions.RequestException as e:
            logger.error(f"Error checking LinkedIn session: {str(e)}")
    
    def _run(self, content: Dict[str, Any]) -> Dict[str, Any]:
        """Post content to LinkedIn"""
        post = None
        try:
            if content.get('platform', '').lower() != 'linkedin':
                return {
                    'success': False,
                    'error': "Skipped - not a LinkedIn post",
                    'platform': 'linkedin',
                    'skipped': True
                }
            
            if isinstance(content.get('content'), dict) and 'text' in content['content']:
                text = content['content']['text']
            else:
                text = content.get('text', '').strip()
            
            if not text:
                raise ValueError("Empty post content")
            
            if len(text) > self.POST_CHAR_LIMIT:
                raise ContentTooLong()
            
            post_data = {
                'platform': 'linkedin',
                'content': json.dumps({'text': text}),
                'status': 'pending',
                'created_at': datetime.utcnow(),
                'source_id': content.get('source_id')
            }
            
            hash_content = f"{text}-{post_data['created_at'].isoformat()}"
            post_data['content_hash'] = hashlib.md5(hash_content.encode()).hexdigest()
            
            try:
                post = self.db_session.create_post(post_data)
                if not post or not post.id:
                    raise ValueError("Failed to store post in database")
                
                logger.info(f"Created database entry with ID: {post.id}")
                
                payload = {
                    "visibleToConnectionsOnly": False,
                    "externalAudienceProviders": [],
                    "commentaryV2": {
                        "text": text,
                        "attributes": []
                    },
                    "origin": "FEED",
                    "allowedCommentersScope": "ALL",
                    "postState": "PUBLISHED"
                }
                
                response = requests.post(self.POST_ENDPOINT, headers=self._headers, json=payload)
                response.raise_for_status()
                
                self._check_session(response.headers)
                
                self.db_session.update_post_status(post.id, 'posted')
                
                return {
                    'success': True,
                    'platform': 'linkedin',
                    'post_id': post.id,
                    'posted_at': datetime.utcnow().isoformat()
                }
                
            except requests.exceptions.RequestException as e:
                error_msg = f"LinkedIn API error: {str(e)}"
                if post and post.id:
                    self.db_session.update_post_status(post.id, 'failed', error_msg)
                raise
                
        except ContentTooLong:
            error_msg = "LinkedIn post character limit reached"
            logger.error(error_msg)
            if post and post.id:
                self.db_session.update_post_status(post.id, 'failed', error_msg)
            return {
                'success': False,
                'error': error_msg,
                'platform': 'linkedin',
                'post_id': post.id if post else None
            }
            
        except Exception as e:
            error_msg = f"Error posting to LinkedIn: {str(e)}"
            logger.error(error_msg)
            if post and post.id:
                self.db_session.update_post_status(post.id, 'failed', error_msg)
            return {
                'success': False,
                'error': error_msg,
                'platform': 'linkedin',
                'post_id': post.id if post else None
            }


class LinkedInAnalytics(BaseTool):
    """Tool for analyzing LinkedIn metrics"""
    name: str = "LinkedIn Analytics"
    description: str = "Analyze LinkedIn post performance"
    db_session: Any = Field(description="Database session for storing and retrieving metrics")
    
    def __init__(self, db_session: Any, **data):
        data["db_session"] = db_session
        super().__init__(**data)
    
    def _run(self, post_id: int) -> Dict[str, Any]:
        """Get LinkedIn post analytics"""
        try:
            metrics = {
                'likes': 0,
                'comments': 0,
                'shares': 0,
                'views': 0,
                'engagement_rate': 0.0
            }
            
            return {
                'success': True,
                'platform': 'linkedin',
                'post_id': post_id,
                'metrics': metrics,
                'analyzed_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting LinkedIn metrics: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'platform': 'linkedin',
                'post_id': post_id
            }
