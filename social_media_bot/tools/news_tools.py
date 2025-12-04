from typing import List, Dict, Optional, Any, Type
from datetime import datetime
from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from newsapi import NewsApiClient
import feedparser
import os
import logging
import requests
from bs4 import BeautifulSoup
from litellm import completion

logger = logging.getLogger(__name__)


class NewsGathererSchema(BaseModel):
    query: str = Field(description="Search query for news articles")
    sources: Optional[List[str]] = Field(default=None, description="List of news sources to search from")
    language: str = Field(default="en", description="Language of articles")
    sort_by: str = Field(default="relevancy", description="Sort order for articles")
    page_size: int = Field(default=10, description="Number of articles to return")


class NewsGatherer(BaseTool):
    name: str = "Gather news articles"
    description: str = "Gather news articles based on query"
    args_schema: Type[BaseModel] = NewsGathererSchema
    
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._newsapi = NewsApiClient(api_key=os.getenv("NEWS_API_KEY"))
        self._default_sources = [
            'techcrunch',
            'wired',
            'the-verge',
            'ars-technica',
            'engadget',
            'recode',
            'hacker-news'
        ]

    def _run(self, query: str, sources: Optional[List[str]] = None,
            language: str = 'en', sort_by: str = 'relevancy', 
            page_size: int = 10) -> Dict:
        try:
            sources = sources or self._default_sources
            sources_str = ','.join(sources)
            
            try:
                response = self._newsapi.get_everything(
                    q=query,
                    sources=sources_str,
                    language=language,
                    sort_by=sort_by,
                    page_size=page_size
                )
            except Exception:
                response = self._newsapi.get_top_headlines(
                    q=query,
                    language=language,
                    page_size=page_size
                )
            
            return {
                'success': True,
                'articles': response['articles'],
                'metadata': {
                    'total_results': response['totalResults'],
                    'query': query,
                    'sources': sources,
                    'gathered_at': datetime.now().isoformat()
                }
            }
        except Exception as e:
            logger.error(f"Error gathering news: {str(e)}")
            return {'success': False, 'error': str(e)}


class RSSFeedReader(BaseTool):
    name: str = "Read RSS feeds"
    description: str = "Read and parse RSS feeds"

    def _run(self, feed_urls: List[str], max_entries: Optional[int] = None) -> Dict:
        try:
            feed_data = []
            for url in feed_urls:
                feed = feedparser.parse(url)
                entries = feed.entries[:max_entries] if max_entries else feed.entries
                formatted_entries = [{
                    'title': entry.get('title'),
                    'link': entry.get('link'),
                    'published': entry.get('published'),
                    'summary': entry.get('summary'),
                    'source': feed.feed.get('title')
                } for entry in entries]
                feed_data.extend(formatted_entries)
            
            return {
                'success': True,
                'entries': feed_data,
                'metadata': {
                    'feed_count': len(feed_urls),
                    'total_entries': len(feed_data),
                    'max_entries': max_entries,
                    'read_at': datetime.now().isoformat()
                }
            }
        except Exception as e:
            logger.error(f"Error reading RSS feeds: {str(e)}")
            return {'success': False, 'error': str(e)}


class TrendAnalyzer(BaseTool):
    name: str = "Analyze content trends"
    description: str = "Analyze trending topics and content patterns"

    def _run(self, topic: str, timeframe: str = 'day') -> Dict:
        try:
            return {
                'success': True,
                'trends': [
                    {'topic': topic, 'score': 0.8, 'momentum': 'rising'},
                    {'related_topics': ['AI', 'Technology', 'Innovation']},
                    {'sentiment': 'positive'}
                ],
                'timeframe': timeframe,
                'analyzed_at': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error analyzing trends: {str(e)}")
            return {'success': False, 'error': str(e)}


class ArticleExtractor(BaseTool):
    name: str = "Extract article content"
    description: str = "Extract and process article content using LLM"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def _run(self, url: str) -> Dict:
        try:
            response = requests.get(url, headers=self._headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                element.decompose()
            
            lines = [line.strip() for line in soup.get_text().splitlines() if line.strip()]
            text = '\n'.join(lines)
            
            prompt = f"""You are an expert content analyzer. Analyze this webpage from {url}.

Raw webpage text:
{text[:5000]}

Please provide:
1. Main Article Content Summary
2. Key Points and Insights
3. Technical Concepts
4. Notable Quotes
5. Key Takeaways"""
            
            response = completion(
                model="deepseek/deepseek-chat",
                messages=[{
                    "role": "user",
                    "content": prompt
                }],
            )

            return {
                'success': True,
                'content': {
                    'raw_text': text[:2000],
                    'processed_content': response.choices[0].message.content,
                    'url': url,
                    'extracted_at': datetime.now().isoformat()
                }
            }
        except Exception as e:
            logger.error(f"Error extracting article: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    def combine_summaries(self, articles: List[Dict]) -> Dict:
        """Combine multiple article summaries into a comprehensive digest"""
        try:
            summaries = []
            for article in articles:
                content = article.get('content', {})
                if content.get('success', False):
                    summaries.append(f"""
Article: {content.get('url', 'Unknown URL')}
Content: {content.get('processed_content', 'No content available')}
---""")

            if not summaries:
                return {
                    'success': False,
                    'error': 'No valid article summaries to combine'
                }

            combine_prompt = f"""You are an expert content curator. Create a comprehensive digest from these article summaries:

Article Summaries:
{''.join(summaries)}

Create a digest that:
1. Identifies major themes and trends
2. Highlights key developments
3. Analyzes industry implications
4. Connects related information
5. Provides a strategic overview"""

            response = completion(
                model="deepseek/deepseek-chat",
                messages=[{
                    "role": "user",
                    "content": combine_prompt
                }],
            )

            return {
                'success': True,
                'content': {
                    'combined_digest': response.choices[0].message.content,
                    'source_count': len(summaries),
                    'created_at': datetime.now().isoformat()
                }
            }
        except Exception as e:
            logger.error(f"Error combining summaries: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
