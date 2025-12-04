# Social Media Agent using CrewAI

An AI-powered system for autonomous social media content curation and posting using CrewAI.

## Overview

This project implements an autonomous social media content curator that:
- Gathers trending news from technology and AI sources
- Analyzes content for relevance and engagement potential
- Generates platform-specific content for Twitter and LinkedIn
- Manages posting with safety checks and compliance verification
- Tracks performance metrics and analytics

## Architecture

The system uses a multi-agent architecture with specialized AI agents:

1. **Content Curator Agent**: Gathers and filters relevant news from RSS feeds and NewsAPI
2. **Safety Manager Agent**: Ensures content safety, compliance, and prevents duplicates
3. **Database Manager Agent**: Handles data storage, retrieval, and analytics
4. **Posting Manager Agent**: Generates and posts platform-specific content

## Features

- **Automated News Collection**: Fetches content from multiple RSS feeds and NewsAPI
- **Content Analysis**: Analyzes trends and extracts key insights using LLM
- **Platform-Specific Content Generation**: Creates optimized posts for Twitter and LinkedIn
- **Safety Checks**: Content moderation, duplicate detection, and compliance verification
- **Rate Limiting**: Manages posting frequency per platform
- **Performance Tracking**: Stores and analyzes engagement metrics
- **Database Integration**: SQLAlchemy-based storage with SQLite

## Tech Stack

- **Core Framework**: CrewAI
- **Language**: Python 3.9+
- **LLM**: DeepSeek (via LiteLLM)
- **Database**: SQLAlchemy with SQLite
- **APIs**: 
  - NewsAPI for content collection
  - Twitter/X API (via twikit)
  - LinkedIn API

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/nashtgc/Social-Media-Agent-using-CrewAI.git
   cd Social-Media-Agent-using-CrewAI
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -e .
   ```

4. Set up environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your API keys and credentials
   ```

## Configuration

### Required Environment Variables

```bash
# News API
NEWS_API_KEY=your_news_api_key

# DeepSeek API (for LLM)
DEEPSEEK_API_KEY=your_deepseek_api_key

# Twitter Credentials
TWITTER_USERNAME=your_twitter_username
TWITTER_EMAIL=your_twitter_email
TWITTER_PASSWORD=your_twitter_password

# LinkedIn Credentials
LINKEDIN_JSESSIONID=your_linkedin_jsessionid
LINKEDIN_LI_AT=your_linkedin_li_at
```

### RSS Feeds Configuration

Edit `social_media_bot/config/feeds.py` to customize RSS feed sources:
- Technology news feeds
- AI/ML news feeds
- Startup news feeds

## Usage

Run the social media bot:

```bash
python social_media_bot/main.py
```

Or use the console script:

```bash
social-media-bot
```

## Project Structure

```
social_media_bot/
├── __init__.py
├── main.py              # Entry point
├── agents.py            # Agent definitions
├── tasks.py             # Task definitions
├── config/
│   ├── feeds.py         # RSS feed configuration
│   └── llm_config.py    # LLM configuration
├── database/
│   ├── models.py        # SQLAlchemy models
│   ├── db_manager.py    # Database operations
│   └── init_db.py       # Database initialization
└── tools/
    ├── news_tools.py    # News gathering tools
    ├── content_tools.py # Content generation tools
    ├── safety_tools.py  # Safety check tools
    ├── twitter_tools.py # Twitter posting tools
    ├── linkedin_tools.py# LinkedIn posting tools
    └── database_tools.py# Database access tools
```

## Database Schema

- **ContentSource**: Tracks content sources (URLs, titles, categories)
- **PostHistory**: Records all posts with status and timestamps
- **ContentMetrics**: Stores engagement metrics (likes, comments, shares)
- **SafetyLog**: Logs safety check results

## Workflow

1. **Content Curation**: Gather trending news from configured sources
2. **Safety Check**: Verify content safety and platform compliance
3. **Content Generation**: Create platform-specific posts using LLM
4. **Posting**: Post to Twitter and LinkedIn with rate limiting
5. **Analytics**: Track and store performance metrics

## Development

Install development dependencies:

```bash
pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
