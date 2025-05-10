import streamlit as st
import pandas as pd
import re
import asyncio
import aiohttp
import orjson
import nest_asyncio
import logging
import pyperclip
import json
from typing import List, Dict, Set, Optional, Tuple, Any
from urllib.parse import urlparse, urljoin, urlunparse
from bs4 import BeautifulSoup
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
import xml.etree.ElementTree as ET
import os
from pathlib import Path
from st_copy import copy_button
import time
from urllib import robotparser
import concurrent.futures
import httpx  # Modern HTTP client
import asyncio_redis  # For distributed crawling
import uvloop  # Faster event loop
import aiodns  # Async DNS resolver
import cchardet  # Faster character encoding detection
import brotli  # Better compression
import orjson  # Faster JSON processing
from typing_extensions import TypedDict
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import mmh3  # MurmurHash3 for faster hashing
import aioredis  # Async Redis client
from aiohttp import ClientTimeout, TCPConnector
from aiohttp.resolver import AsyncResolver
import ssl
import certifi
from urllib3.util.retry import Retry
from urllib3.util import ssl_
import pyOpenSSL
import cryptography
from cryptography.fernet import Fernet
import base64
import secrets
import hmac
import hashlib
import time
import uuid
import random
import string
import re
import sys
import traceback
import warnings
import platform
import psutil
import GPUtil
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
import colorama
from colorama import Fore, Style
import rich
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.prompt import Prompt
from rich.style import Style
from rich.theme import Theme
from rich.traceback import install as install_rich_traceback
from rich.logging import RichHandler
import click
import typer
from typer import Typer, Option
import pydantic
from pydantic import BaseModel, Field, validator
import fastapi
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from uvicorn.config import Config
from uvicorn.lifespan.on import LifespanOn
from uvicorn.lifespan.off import LifespanOff
from uvicorn.lifespan.auto import LifespanAuto
from uvicorn.lifespan import Lifespan
from uvicorn.loops import auto, asyncio, uvloop
from uvicorn.loops.auto import auto_loop_setup
from uvicorn.loops.asyncio import asyncio_setup
from uvicorn.loops.uvloop import uvloop_setup
from uvicorn.protocols import auto, http, websockets
from uvicorn.protocols.auto import AutoProtocol
from uvicorn.protocols.http import HTTPProtocol
from uvicorn.protocols.websockets import WebSocketProtocol
from uvicorn.protocols.websockets.auto import AutoWebSocketsProtocol
from uvicorn.protocols.websockets.websockets_impl import WebSocketProtocol as WSProtocol
from uvicorn.protocols.websockets.websockets_impl import WebSocketState
from uvicorn.protocols.websockets.websockets_impl import WebSocketMessage
from uvicorn.protocols.websockets.websockets_impl import WebSocketClose
from uvicorn.protocols.websockets.websockets_impl import WebSocketPing
from uvicorn.protocols.websockets.websockets_impl import WebSocketPong
from uvicorn.protocols.websockets.websockets_impl import WebSocketText
from uvicorn.protocols.websockets.websockets_impl import WebSocketBinary
from uvicorn.protocols.websockets.websockets_impl import WebSocketConnection
from uvicorn.protocols.websockets.websockets_impl import WebSocketServer
from uvicorn.protocols.websockets.websockets_impl import WebSocketClient
from uvicorn.protocols.websockets.websockets_impl import WebSocketHandler
from uvicorn.protocols.websockets.websockets_impl import WebSocketMiddleware
from uvicorn.protocols.websockets.websockets_impl import WebSocketRoute
from uvicorn.protocols.websockets.websockets_impl import WebSocketRouter
from uvicorn.protocols.websockets.websockets_impl import WebSocketApplication
from uvicorn.protocols.websockets.websockets_impl import WebSocketServerProtocol
from uvicorn.protocols.websockets.websockets_impl import WebSocketClientProtocol
from uvicorn.protocols.websockets.websockets_impl import WebSocketServerFactory
from uvicorn.protocols.websockets.websockets_impl import WebSocketClientFactory
from uvicorn.protocols.websockets.websockets_impl import WebSocketServerTransport
from uvicorn.protocols.websockets.websockets_impl import WebSocketClientTransport

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Set up event loop policy for Windows
if os.name == 'nt':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
else:
    # Use uvloop for better performance on Unix systems
    uvloop.install()

# Set up rich logging
install_rich_traceback()
console = Console()

# Configure logging with rich
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)

# -----------------------------
# Constants
# -----------------------------
DEFAULT_TIMEOUT = 10
DEFAULT_MAX_URLS = 50000
MAX_REDIRECTS = 5
DEFAULT_USER_AGENT = "custom_adidas_seo_x3423/1.0"
SAVE_INTERVAL = 50
ERROR_THRESHOLD = 0.15
MIN_CONCURRENCY = 5
MAX_CONCURRENCY = 100
DEFAULT_CONCURRENCY = 20
BATCH_SIZE = 100
MAX_RETRIES = 3
RETRY_DELAYS = [1, 2, 4]
DNS_CACHE_TTL = 300
CONNECTION_POOL_SIZE = 100
COMPRESSION_LEVEL = 6  # Brotli compression level
CACHE_TTL = 3600  # Cache TTL in seconds
MAX_CACHE_SIZE = 10000  # Maximum number of items in cache
RATE_LIMIT_DELAY = 0.1  # Delay between requests in seconds
MAX_RATE_LIMIT_RETRIES = 3  # Maximum number of rate limit retries
RATE_LIMIT_WINDOW = 60  # Rate limit window in seconds
MAX_REQUESTS_PER_WINDOW = 1000  # Maximum requests per rate limit window
SSL_VERIFY = True  # SSL verification
SSL_CERT = certifi.where()  # SSL certificate path
SSL_CONTEXT = ssl.create_default_context(cafile=SSL_CERT)  # SSL context
SSL_OPTIONS = {
    'ssl_version': ssl.PROTOCOL_TLS,
    'cert_reqs': ssl.CERT_REQUIRED,
    'ca_certs': SSL_CERT,
    'verify': SSL_VERIFY
}  # SSL options

USER_AGENTS = {
    "Googlebot Desktop": (
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ),
    "Googlebot Mobile": (
        "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Mobile Safari/537.36 "
        "(compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ),
    "Chrome Desktop": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.100 Safari/537.36"
    ),
    "Chrome Mobile": (
        "Mozilla/5.0 (Linux; Android 10; Pixel 3) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.100 Mobile Safari/537.36"
    ),
    "Custom Adidas SEO Bot": DEFAULT_USER_AGENT,
}

# -----------------------------
# Models
# -----------------------------
class CrawlResult(BaseModel):
    """Model for crawl results."""
    url: str
    status_code: int
    content_type: str
    title: Optional[str]
    meta_description: Optional[str]
    h1: Optional[str]
    h1_count: int
    canonical_url: Optional[str]
    meta_robots: Optional[str]
    x_robots_tag: Optional[str]
    html_lang: Optional[str]
    blocked_by_robots: bool
    robots_block_rule: Optional[str]
    indexable: bool
    indexability_reason: Optional[str]
    last_modified: Optional[str]
    word_count: int
    crawl_time: float
    timestamp: datetime

    class Config:
        arbitrary_types_allowed = True

class CrawlConfig(BaseModel):
    """Model for crawl configuration."""
    user_agent: str
    concurrency: int
    timeout: int
    respect_robots: bool
    max_urls: int
    batch_size: int
    max_retries: int
    retry_delays: List[int]
    dns_cache_ttl: int
    connection_pool_size: int
    compression_level: int
    cache_ttl: int
    max_cache_size: int
    rate_limit_delay: float
    max_rate_limit_retries: int
    rate_limit_window: int
    max_requests_per_window: int
    ssl_verify: bool
    ssl_cert: str
    ssl_context: Any
    ssl_options: Dict[str, Any]

    @validator('concurrency')
    def validate_concurrency(cls, v):
        if v < MIN_CONCURRENCY or v > MAX_CONCURRENCY:
            raise ValueError(f'Concurrency must be between {MIN_CONCURRENCY} and {MAX_CONCURRENCY}')
        return v

    @validator('timeout')
    def validate_timeout(cls, v):
        if v < 1 or v > 60:
            raise ValueError('Timeout must be between 1 and 60 seconds')
        return v

    @validator('max_urls')
    def validate_max_urls(cls, v):
        if v < 1:
            raise ValueError('Max URLs must be greater than 0')
        return v

    @validator('batch_size')
    def validate_batch_size(cls, v):
        if v < 1:
            raise ValueError('Batch size must be greater than 0')
        return v

    @validator('max_retries')
    def validate_max_retries(cls, v):
        if v < 0:
            raise ValueError('Max retries must be greater than or equal to 0')
        return v

    @validator('retry_delays')
    def validate_retry_delays(cls, v):
        if not v:
            raise ValueError('Retry delays must not be empty')
        return v

    @validator('dns_cache_ttl')
    def validate_dns_cache_ttl(cls, v):
        if v < 0:
            raise ValueError('DNS cache TTL must be greater than or equal to 0')
        return v

    @validator('connection_pool_size')
    def validate_connection_pool_size(cls, v):
        if v < 1:
            raise ValueError('Connection pool size must be greater than 0')
        return v

    @validator('compression_level')
    def validate_compression_level(cls, v):
        if v < 0 or v > 11:
            raise ValueError('Compression level must be between 0 and 11')
        return v

    @validator('cache_ttl')
    def validate_cache_ttl(cls, v):
        if v < 0:
            raise ValueError('Cache TTL must be greater than or equal to 0')
        return v

    @validator('max_cache_size')
    def validate_max_cache_size(cls, v):
        if v < 1:
            raise ValueError('Max cache size must be greater than 0')
        return v

    @validator('rate_limit_delay')
    def validate_rate_limit_delay(cls, v):
        if v < 0:
            raise ValueError('Rate limit delay must be greater than or equal to 0')
        return v

    @validator('max_rate_limit_retries')
    def validate_max_rate_limit_retries(cls, v):
        if v < 0:
            raise ValueError('Max rate limit retries must be greater than or equal to 0')
        return v

    @validator('rate_limit_window')
    def validate_rate_limit_window(cls, v):
        if v < 1:
            raise ValueError('Rate limit window must be greater than 0')
        return v

    @validator('max_requests_per_window')
    def validate_max_requests_per_window(cls, v):
        if v < 1:
            raise ValueError('Max requests per window must be greater than 0')
        return v

# -----------------------------
# Helper Functions
# -----------------------------
def save_results_to_file(results: List[Dict], filename: str):
    """Save results to a JSON file with compression."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logging.info(f"Successfully saved {len(results)} results to {filename}")
    except Exception as e:
        logging.error(f"Error saving results to {filename}: {e}")

def load_results_from_file(filename: str) -> List[Dict]:
    """Load results from a JSON file with error handling."""
    try:
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logging.error(f"Error loading results from {filename}: {e}")
    return []

def calculate_error_rate(results: List[Dict]) -> float:
    """Calculate the error rate from recent results with weighted recent errors."""
    if not results:
        return 0.0
    
    # Weight recent errors more heavily
    recent_results = results[-100:] if len(results) > 100 else results
    error_count = sum(1 for r in recent_results if str(r.get("Final_Status_Code", "")).startswith(("4", "5")))
    return error_count / len(recent_results)

def adjust_concurrency(current_concurrency: int, error_rate: float) -> int:
    """Dynamically adjust concurrency based on error rate with more aggressive scaling."""
    if error_rate > ERROR_THRESHOLD:
        # More aggressive reduction when errors are high
        return max(MIN_CONCURRENCY, int(current_concurrency * 0.7))
    elif error_rate < ERROR_THRESHOLD / 2:
        # More aggressive increase when errors are low
        return min(MAX_CONCURRENCY, int(current_concurrency * 1.2))
    return current_concurrency

class URLChecker:
    def __init__(self, user_agent: str, concurrency: int, timeout: int, respect_robots: bool):
        self.config = CrawlConfig(
            user_agent=user_agent,
            concurrency=concurrency,
            timeout=timeout,
            respect_robots=respect_robots,
            max_urls=DEFAULT_MAX_URLS,
            batch_size=BATCH_SIZE,
            max_retries=MAX_RETRIES,
            retry_delays=RETRY_DELAYS,
            dns_cache_ttl=DNS_CACHE_TTL,
            connection_pool_size=CONNECTION_POOL_SIZE,
            compression_level=COMPRESSION_LEVEL,
            cache_ttl=CACHE_TTL,
            max_cache_size=MAX_CACHE_SIZE,
            rate_limit_delay=RATE_LIMIT_DELAY,
            max_rate_limit_retries=MAX_RATE_LIMIT_RETRIES,
            rate_limit_window=RATE_LIMIT_WINDOW,
            max_requests_per_window=MAX_REQUESTS_PER_WINDOW,
            ssl_verify=SSL_VERIFY,
            ssl_cert=SSL_CERT,
            ssl_context=SSL_CONTEXT,
            ssl_options=SSL_OPTIONS
        )
        
        self.robots_cache = {}
        self.robots_parser_cache = {}
        self.session = None
        self.semaphore = None
        self.failed_urls = set()
        self.recent_results = []
        self.last_save_time = datetime.now()
        self.save_filename = f"crawl_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.robots_semaphore = asyncio.Semaphore(10)
        self.stop_event = asyncio.Event()
        self.last_adjustment_time = datetime.now()
        self.adjustment_interval = 10
        self.rate_limit_semaphore = asyncio.Semaphore(self.config.max_requests_per_window)
        self.rate_limit_reset_time = time.time()
        self.redis_client = None
        self.cache = {}
        self.cache_lock = asyncio.Lock()
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        )

    async def setup(self):
        """Initialize the aiohttp session and semaphore with optimized settings."""
        try:
            # Configure DNS resolver
            resolver = AsyncResolver()
            
            # Configure TCP connector with optimized settings
            connector = TCPConnector(
                limit=self.config.connection_pool_size,
                ttl_dns_cache=self.config.dns_cache_ttl,
                enable_cleanup_closed=True,
                force_close=False,
                ssl=self.config.ssl_context,
                use_dns_cache=True,
                keepalive_timeout=30,
                resolver=resolver
            )
            
            # Configure timeout settings
            timeout_settings = ClientTimeout(
                total=None,
                connect=self.config.timeout,
                sock_read=self.config.timeout,
                sock_connect=self.config.timeout
            )
            
            # Configure session with optimized settings
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout_settings,
                json_serialize=orjson.dumps,
                headers={
                    "User-Agent": self.config.user_agent,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1"
                },
                trust_env=True,
                auto_decompress=True
            )
            
            # Initialize semaphore with initial concurrency
            self.semaphore = asyncio.Semaphore(self.config.concurrency)
            
            # Initialize Redis client for distributed crawling
            try:
                self.redis_client = await aioredis.create_redis_pool(
                    'redis://localhost',
                    encoding='utf-8',
                    maxsize=10
                )
            except Exception as e:
                logging.warning(f"Could not connect to Redis: {e}")
            
            logging.info(f"URLChecker initialized with concurrency {self.config.concurrency}")
        except Exception as e:
            logging.error(f"Error setting up URLChecker: {str(e)}")
            raise

    @lru_cache(maxsize=1000)
    def get_cache_key(self, url: str) -> str:
        """Generate a cache key for a URL."""
        return hashlib.md5(url.encode()).hexdigest()

    async def get_from_cache(self, url: str) -> Optional[Dict]:
        """Get a result from cache."""
        cache_key = self.get_cache_key(url)
        
        # Try Redis first
        if self.redis_client:
            try:
                cached = await self.redis_client.get(cache_key)
                if cached:
                    return orjson.loads(cached)
            except Exception as e:
                logging.warning(f"Redis cache error: {e}")
        
        # Try local cache
        async with self.cache_lock:
            if cache_key in self.cache:
                return self.cache[cache_key]
        
        return None

    async def set_in_cache(self, url: str, result: Dict):
        """Set a result in cache."""
        cache_key = self.get_cache_key(url)
        
        # Try Redis first
        if self.redis_client:
            try:
                await self.redis_client.set(
                    cache_key,
                    orjson.dumps(result),
                    expire=self.config.cache_ttl
                )
            except Exception as e:
                logging.warning(f"Redis cache error: {e}")
        
        # Update local cache
        async with self.cache_lock:
            self.cache[cache_key] = result
            if len(self.cache) > self.config.max_cache_size:
                # Remove oldest items
                oldest_key = next(iter(self.cache))
                del self.cache[oldest_key]

    async def check_rate_limit(self):
        """Check and enforce rate limits."""
        now = time.time()
        if now - self.rate_limit_reset_time >= self.config.rate_limit_window:
            self.rate_limit_reset_time = now
            self.rate_limit_semaphore = asyncio.Semaphore(self.config.max_requests_per_window)
        
        await self.rate_limit_semaphore.acquire()
        try:
            await asyncio.sleep(self.config.rate_limit_delay)
        finally:
            self.rate_limit_semaphore.release()

    async def fetch_and_parse(self, url: str) -> Dict:
        """Enhanced fetch_and_parse with modern features."""
        # Check cache first
        cached_result = await self.get_from_cache(url)
        if cached_result:
            return cached_result

        async with self.semaphore:
            # Check if we need to adjust concurrency
            await self.adjust_concurrency_if_needed()
            
            # Check rate limits
            await self.check_rate_limit()
            
            with self.progress.add_task(f"Crawling {url}", total=1) as task:
                logging.info(f"Fetching and parsing URL: {url}")
                data = {
                    "Original_URL": url,
                    "Content_Type": "",
                    "Initial_Status_Code": "",
                    "Initial_Status_Type": "",
                    "Final_URL": "",
                    "Final_Status_Code": "",
                    "Final_Status_Type": "",
                    "Title": "",
                    "Meta Description": "",
                    "H1": "",
                    "H1_Count": 0,
                    "Canonical_URL": "",
                    "Meta Robots": "",
                    "X-Robots-Tag": "",
                    "HTML Lang": "",
                    "Blocked by Robots.txt": "No",
                    "Robots Block Rule": "",
                    "Indexable": "Yes",
                    "Indexability Reason": "",
                    "Last Modified": "",
                    "Word Count": 0,
                    "Crawl Time": 0,
                    "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }

                try:
                    # Check robots.txt in parallel with other operations
                    robots_task = asyncio.create_task(self.check_robots(url))
                    
                    # Make the request with enhanced retry logic
                    for attempt, delay in enumerate(self.config.retry_delays, 1):
                        try:
                            async with self.session.get(
                                url,
                                ssl=self.config.ssl_context,
                                allow_redirects=True,
                                timeout=self.config.timeout,
                                headers={
                                    "User-Agent": self.config.user_agent,
                                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                                    "Accept-Language": "en-US,en;q=0.5",
                                    "Accept-Encoding": "gzip, deflate, br",
                                    "Connection": "keep-alive",
                                    "Upgrade-Insecure-Requests": "1"
                                }
                            ) as resp:
                                # Add Content-Type field before status codes
                                data["Content_Type"] = resp.headers.get("Content-Type", "")
                                init_str = str(resp.status)
                                data["Initial_Status_Code"] = init_str
                                data["Initial_Status_Type"] = self.status_label(resp.status)
                                data["Final_URL"] = str(resp.url)
                                data["Final_Status_Code"] = str(resp.status)
                                data["Final_Status_Type"] = self.status_label(resp.status)
                                data["Last Modified"] = resp.headers.get("Last-Modified", "")

                                if resp.status == 200 and resp.content_type and resp.content_type.startswith("text/html"):
                                    content = await resp.text(errors='replace')
                                    result = self.parse_html_content(data, content, resp.headers, resp.status, True)
                                    
                                    # Wait for robots.txt check to complete
                                    allowed = await robots_task
                                    if not allowed:
                                        result["Blocked by Robots.txt"] = "Yes"
                                        result["Robots Block Rule"] = "Disallow"
                                        result["Indexable"] = "No"
                                        result["Indexability Reason"] = "Blocked by robots.txt"
                                        if self.config.respect_robots:
                                            return result
                                    
                                    result = update_redirect_label(result, url)
                                    logging.info(f"Successfully parsed HTML content from {url}")
                                    
                                    # Cache the result
                                    await self.set_in_cache(url, result)
                                    
                                    # Add to recent results and check if we need to save
                                    self.recent_results.append(result)
                                    if len(self.recent_results) >= SAVE_INTERVAL:
                                        save_results_to_file(self.recent_results, self.save_filename)
                                        logging.info(f"Saved {len(self.recent_results)} results to {self.save_filename}")
                                        self.recent_results = []  # Clear after saving
                                        self.last_save_time = datetime.now()
                                    
                                    self.progress.update(task, completed=1)
                                    return result
                                else:
                                    data["Indexability Reason"] = "Non-200 or non-HTML"
                                    data = update_redirect_label(data, url)
                                    logging.info(f"Non-HTML or non-200 response from {url}: {init_str}")
                                    return data
                        except asyncio.TimeoutError:
                            if attempt == self.config.max_retries:
                                raise
                            logging.warning(f"Timeout on attempt {attempt}/{self.config.max_retries} for {url}, retrying in {delay}s...")
                            await asyncio.sleep(delay)
                        except Exception as e:
                            if attempt == self.config.max_retries:
                                raise
                            logging.warning(f"Error on attempt {attempt}/{self.config.max_retries} for {url}: {str(e)}, retrying in {delay}s...")
                            await asyncio.sleep(delay)

                except Exception as e:
                    logging.error(f"Error fetching {url}: {str(e)}")
                    data["Initial_Status_Code"] = "Error"
                    data["Initial_Status_Type"] = str(e)
                    data["Final_URL"] = url
                    data["Final_Status_Code"] = "Error"
                    data["Final_Status_Type"] = str(e)
                    data["Indexability Reason"] = f"Error: {str(e)}"
                    data["Indexable"] = "No"
                    self.failed_urls.add(url)  # Add to failed URLs for potential recrawl
                    return data

    def status_label(self, status_code: int) -> str:
        """Convert HTTP status code to a human-readable label."""
        if 200 <= status_code < 300:
            return "Success"
        elif 300 <= status_code < 400:
            return "Redirect"
        elif 400 <= status_code < 500:
            return "Client Error"
        elif 500 <= status_code < 600:
            return "Server Error"
        else:
            return "Unknown"

    def parse_html_content(self, data: Dict, content: str, headers: Dict, status_code: int, is_final: bool) -> Dict:
        """Parse HTML content and extract metadata."""
        try:
            soup = BeautifulSoup(content, "lxml")
            
            # Extract title
            title_tag = soup.find("title")
            data["Title"] = title_tag.text.strip() if title_tag else ""
            
            # Extract meta description
            meta_desc = soup.find("meta", attrs={"name": "description"})
            data["Meta Description"] = meta_desc["content"].strip() if meta_desc and "content" in meta_desc.attrs else ""
            
            # Extract H1
            h1_tags = soup.find_all("h1")
            data["H1_Count"] = len(h1_tags)
            data["H1"] = h1_tags[0].text.strip() if h1_tags else ""
            
            # Enhanced canonical URL extraction
            canonical = None
            # Check for canonical in link tag
            canonical_link = soup.find("link", attrs={"rel": "canonical"})
            if canonical_link and "href" in canonical_link.attrs:
                canonical = canonical_link["href"]
            # Check for canonical in meta tag
            if not canonical:
                canonical_meta = soup.find("meta", attrs={"name": "canonical"})
                if canonical_meta and "content" in canonical_meta.attrs:
                    canonical = canonical_meta["content"]
            
            # Normalize canonical URL
            if canonical:
                try:
                    # Convert relative URLs to absolute
                    if not canonical.startswith(('http://', 'https://')):
                        canonical = urljoin(data["Original_URL"], canonical)
                    # Remove fragments and normalize
                    parsed = urlparse(canonical)
                    canonical = urlunparse(parsed._replace(fragment=""))
                    data["Canonical_URL"] = canonical
                except Exception as e:
                    logging.warning(f"Error normalizing canonical URL {canonical}: {e}")
                    data["Canonical_URL"] = canonical
            else:
                data["Canonical_URL"] = ""
            
            # Extract meta robots
            meta_robots = soup.find("meta", attrs={"name": "robots"})
            data["Meta Robots"] = meta_robots["content"] if meta_robots and "content" in meta_robots.attrs else ""
            
            # Extract X-Robots-Tag
            data["X-Robots-Tag"] = headers.get("X-Robots-Tag", "")
            
            # Extract HTML lang
            html_tag = soup.find("html")
            data["HTML Lang"] = html_tag.get("lang", "") if html_tag else ""
            
            # Determine indexability
            data["Indexable"] = "Yes"
            data["Indexability Reason"] = ""
            
            # Check for noindex
            if "noindex" in data["Meta Robots"].lower() or "noindex" in data["X-Robots-Tag"].lower():
                data["Indexable"] = "No"
                data["Indexability Reason"] = "Noindex directive"
            
            # Calculate word count
            text = soup.get_text()
            words = text.split()
            data["Word Count"] = len(words)
            
            return data
        except Exception as e:
            logging.error(f"Error parsing HTML content: {str(e)}")
            return data

    def stop(self):
        """Set the stop event to halt the crawl."""
        self.stop_event.set()
        logging.info("Stop event set - crawl will halt after current tasks complete")

    def is_stopped(self) -> bool:
        """Check if the crawl should stop."""
        return self.stop_event.is_set()

async def dynamic_frontier_crawl(
    seed_url: str,
    checker: URLChecker,
    include_regex: Optional[str],
    exclude_regex: Optional[str],
    show_partial_callback=None
) -> List[Dict]:
    """
    Dynamic frontier crawl implementation with unique URL tracking for accurate progress.
    """
    visited: Set[str] = set()
    queued: Set[str] = set()
    results = []
    frontier = asyncio.PriorityQueue()
    
    # Normalize and validate seed URL
    seed_url = normalize_url(seed_url)
    if not seed_url:
        logging.error("Invalid seed URL provided")
        return results
        
    base_netloc = urlparse(seed_url).netloc.lower()
    if not base_netloc:
        logging.error(f"Could not parse netloc from seed URL: {seed_url}")
        return results
        
    logging.info(f"Starting dynamic frontier crawl from seed URL: {seed_url}")
    logging.info(f"Base netloc: {base_netloc}")
    
    # Initialize frontier and queued set with seed URL
    await frontier.put((0, seed_url))
    queued.add(seed_url)
    inc, exc = compile_filters(include_regex, exclude_regex)
    
    try:
        await checker.setup()
        while not frontier.empty() and len(visited) < DEFAULT_MAX_URLS:
            if checker.is_stopped():
                logging.info("Crawl stopped by user request")
                break

            depth, url = await frontier.get()
            norm_url = normalize_url(url)
            
            if not norm_url or norm_url in visited:
                continue
                
            visited.add(norm_url)
            logging.info(f"Crawling URL: {norm_url}")
            
            try:
                result = await checker.fetch_and_parse(norm_url)
                if result:
                    results.append(result)
                    logging.info(f"Successfully processed URL: {norm_url}")
                else:
                    logging.warning(f"No result returned for URL: {norm_url}")
            except Exception as e:
                logging.error(f"Error processing URL {norm_url}: {str(e)}")
                continue
            
            # Discover new links from the current page
            try:
                discovered_links = await discover_links(norm_url, checker.session, checker.config.user_agent)
                logging.info(f"Discovered {len(discovered_links)} links from {norm_url}")
                
                for link in discovered_links:
                    if checker.is_stopped():
                        break
                        
                    norm_link = normalize_url(link)
                    if not norm_link or norm_link in visited or norm_link in queued:
                        continue
                    parsed_link = urlparse(norm_link)
                    # Crawl only internal URLs (matching the seed's netloc)
                    if parsed_link.netloc.lower() != base_netloc:
                        continue
                    if not regex_filter(norm_link, inc, exc):
                        continue
                    await frontier.put((depth + 1, norm_link))
                    queued.add(norm_link)
            except Exception as e:
                logging.error(f"Error discovering links from {norm_url}: {str(e)}")
                continue
            
            # For progress: only count unique URLs
            crawled_count = len(visited)
            total_unique = len(queued)
            remaining = total_unique - crawled_count
            if show_partial_callback:
                show_partial_callback(results, crawled_count, total_unique)
        logging.info(f"Dynamic frontier crawl completed. Visited {len(visited)} unique URLs.")
        return results
        
    except Exception as e:
        logging.error(f"Error in dynamic frontier crawl: {str(e)}")
        return results
    finally:
        await checker.close()

async def discover_links(url: str, session: aiohttp.ClientSession, user_agent: str) -> List[str]:
    """
    Enhanced link discovery with optimized parsing and filtering.
    """
    out = set()  # Use set for faster lookups
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }
    
    try:
        async with session.get(url, headers=headers, ssl=False, allow_redirects=True) as resp:
            if resp.status == 200 and resp.content_type and resp.content_type.startswith("text/html"):
                text = await resp.text(errors='replace')
                soup = BeautifulSoup(text, "lxml")
                
                # Process links in parallel using a thread pool
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                    futures = []
                    for a in soup.find_all("a", href=True):
                        futures.append(executor.submit(process_link, a["href"], url))
                    
                    # Collect results
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            abs_link = future.result()
                            if abs_link:
                                out.add(abs_link)
                        except Exception as e:
                            logging.error(f"Error processing link: {e}")
                            continue
    except Exception as e:
        logging.error(f"Error discovering links from {url}: {str(e)}")
    
    return list(out)

def process_link(href: str, base_url: str) -> Optional[str]:
    """
    Process a single link with optimized URL normalization.
    """
    try:
        # Skip empty or javascript links
        if not href or href.startswith(('javascript:', 'mailto:', 'tel:', '#')):
            return None
            
        # Convert relative URLs to absolute
        abs_link = urljoin(base_url, href)
        
        # Basic URL validation
        if not abs_link.startswith(('http://', 'https://')):
            return None
            
        # Remove fragments and normalize
        parsed = urlparse(abs_link)
        normalized = urlunparse(parsed._replace(fragment=""))
        
        return normalized
    except Exception as e:
        logging.error(f"Error processing link {href}: {e}")
        return None

def compile_filters(include_pattern: str, exclude_pattern: str):
    """
    Compile regex patterns for URL filtering.
    """
    inc = re.compile(include_pattern) if include_pattern else None
    exc = re.compile(exclude_pattern) if exclude_pattern else None
    return inc, exc

def regex_filter(url: str, inc, exc) -> bool:
    """
    Filter URL based on regex patterns.
    """
    if inc and not inc.search(url):
        return False
    if exc and exc.search(url):
        return False
    return True

async def run_dynamic_crawl(seed_url: str, checker: URLChecker, include_pattern: str, exclude_pattern: str, show_partial_callback) -> List[Dict]:
    """Async wrapper for dynamic frontier crawl."""
    try:
        logging.info(f"Starting dynamic crawl for seed URL: {seed_url}")
        results = await dynamic_frontier_crawl(
            seed_url=seed_url.strip(),
            checker=checker,
            include_regex=include_pattern,
            exclude_regex=exclude_pattern,
            show_partial_callback=show_partial_callback
        )
        
        logging.info(f"Dynamic crawl completed. Found {len(results)} results.")
        
        # Recrawl failed URLs if any
        if checker.failed_urls:
            logging.info(f"Recrawling {len(checker.failed_urls)} failed URLs...")
            recrawl_results = await checker.recrawl_failed_urls()
            results.extend(recrawl_results)
            logging.info(f"Recrawl completed. Added {len(recrawl_results)} more results.")
        
        await checker.close()
        return results
    except Exception as e:
        logging.error(f"Error in dynamic crawl: {e}")
        await checker.close()
        return []

async def run_list_crawl(urls: List[str], checker: URLChecker, show_partial_callback) -> List[Dict]:
    """Async wrapper for list mode crawl."""
    try:
        results = await chunk_process(urls, checker, show_partial_callback=show_partial_callback)
        
        # Recrawl failed URLs if any
        if checker.failed_urls:
            recrawl_results = await checker.recrawl_failed_urls()
            results.extend(recrawl_results)
        
        await checker.close()
        return results
    except Exception as e:
        logging.error(f"Error in list crawl: {e}")
        await checker.close()
        return []

async def run_sitemap_crawl(urls: List[str], checker: URLChecker, show_partial_callback) -> List[Dict]:
    """Async wrapper for sitemap mode crawl."""
    try:
        results = await chunk_process(urls, checker, show_partial_callback=show_partial_callback)
        
        # Recrawl failed URLs if any
        if checker.failed_urls:
            recrawl_results = await checker.recrawl_failed_urls()
            results.extend(recrawl_results)
        
        await checker.close()
        return results
    except Exception as e:
        logging.error(f"Error in sitemap crawl: {e}")
        await checker.close()
        return []

async def process_sitemaps(sitemap_urls: List[str], show_partial_callback=None) -> List[str]:
    """
    Process multiple sitemap URLs concurrently and extract URLs.
    """
    all_urls = []
    tasks = []
    
    for sitemap_url in sitemap_urls:
        tasks.append(async_parse_sitemap(sitemap_url))
    
    for future in asyncio.as_completed(tasks):
        try:
            urls = await future
            all_urls.extend(urls)
            if show_partial_callback:
                show_partial_callback(all_urls)
        except Exception as e:
            logging.error(f"Error processing sitemap: {e}")
    
    return all_urls

async def async_parse_sitemap(url: str) -> List[str]:
    """
    Parse a single sitemap URL and extract URLs.
    Handles both regular sitemaps and sitemap indexes.
    """
    urls = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, ssl=False) as response:
                if response.status == 200:
                    content = await response.text()
                    if '<?xml' in content:
                        # Parse XML content
                        root = ET.fromstring(content)
                        
                        # Check if this is a sitemap index
                        if root.tag.endswith('sitemapindex'):
                            # This is a sitemap index, extract and process nested sitemaps
                            sitemap_urls = []
                            for sitemap in root.findall('.//{*}sitemap/{*}loc'):
                                if sitemap.text:
                                    sitemap_urls.append(sitemap.text.strip())
                            
                            # Process nested sitemaps concurrently
                            tasks = [async_parse_sitemap(sitemap_url) for sitemap_url in sitemap_urls]
                            nested_results = await asyncio.gather(*tasks, return_exceptions=True)
                            
                            # Combine results, handling any errors
                            for result in nested_results:
                                if isinstance(result, list):
                                    urls.extend(result)
                                else:
                                    logging.error(f"Error processing nested sitemap: {result}")
                            
                        else:
                            # Regular sitemap, extract URLs
                            for url_element in root.findall('.//{*}url/{*}loc'):
                                if url_element.text:
                                    urls.append(url_element.text.strip())
                    else:
                        # Try parsing as text sitemap (one URL per line)
                        for line in content.splitlines():
                            line = line.strip()
                            if line and not line.startswith('#'):
                                urls.append(line)
    except Exception as e:
        logging.error(f"Error parsing sitemap {url}: {e}")
    return urls

async def chunk_process(urls: List[str], checker: URLChecker, show_partial_callback=None) -> List[Dict]:
    """
    Enhanced batch processing with optimized chunking and error handling.
    """
    results = []
    total = len(urls)
    processed = 0
    
    try:
        await checker.setup()
        
        # Process URLs in batches
        for i in range(0, total, BATCH_SIZE):
            if checker.is_stopped():
                logging.info("Chunk processing stopped by user request")
                break
                
            batch = urls[i:i + BATCH_SIZE]
            tasks = []
            
            # Create tasks for the batch
            for url in batch:
                if checker.is_stopped():
                    break
                tasks.append(checker.fetch_and_parse(url))
            
            # Process batch concurrently
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for result in batch_results:
                if isinstance(result, Exception):
                    logging.error(f"Error in batch processing: {result}")
                    continue
                    
                results.append(result)
                processed += 1
                
                if show_partial_callback:
                    show_partial_callback(results, processed, total)
            
            # Small delay between batches to prevent overwhelming the server
            await asyncio.sleep(0.1)
            
    except Exception as e:
        logging.error(f"Error in chunk processing: {e}")
    finally:
        await checker.close()
    
    return results

def normalize_url(url: str) -> str:
    """
    Normalize a URL by removing fragments and ensuring proper format.
    """
    if not url:
        return ""
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    parsed = urlparse(url)
    parsed = parsed._replace(fragment="")
    return urlunparse(parsed)

def update_redirect_label(data: Dict, original_url: str) -> Dict:
    """
    Update the Final_Status_Type field based on redirect information and canonical URLs.
    """
    final_url = data.get("Final_URL", "")
    final_status = data.get("Final_Status_Code", "")
    canonical_url = data.get("Canonical_URL", "")
    
    try:
        final_code = int(final_status)
    except Exception:
        final_code = None

    # Check for canonical URL mismatch
    if canonical_url and canonical_url != final_url:
        data["Indexability Reason"] = "Canonical URL mismatch"
        data["Indexable"] = "No"

    if final_url == original_url:
        data["Final_Status_Type"] = "No Redirect"
    else:
        if final_code == 200:
            if canonical_url and canonical_url != final_url:
                data["Final_Status_Type"] = "Redirecting to Non-Canonical Page"
            else:
                data["Final_Status_Type"] = "Redirecting to Live Page"
        elif final_code in (301, 302):
            data["Final_Status_Type"] = "Temporary/Permanent Redirect"
        elif final_code == 404:
            data["Final_Status_Type"] = "Redirecting to Not Found Page"
        elif final_code == 500:
            data["Final_Status_Type"] = "Redirecting to Server Error Page"
        else:
            data["Final_Status_Type"] = f"Status {final_status}"
    return data

def format_and_reorder_df(df):
    """Helper function to reorder and format columns consistently."""
    # First, ensure consistent column names
    rename_map = {
        'Is_Blocked_by_Robots': 'Blocked by Robots.txt',
        'Is_Indexable': 'Indexable',
        'Indexability_Reason': 'Indexability Reason',
        'Meta_Description': 'Meta Description',
        'H1_Text': 'H1',
        'Meta_Robots': 'Meta Robots',
        'X_Robots_Tag': 'X-Robots-Tag',
        'HTML_Lang': 'HTML Lang',
        'HTTP_Last_Modified': 'Last Modified'
    }
    
    # Apply renaming
    df = df.rename(columns=rename_map)
    
    # Ensure Indexable values are consistent
    if 'Indexable' in df.columns:
        df['Indexable'] = df['Indexable'].map(lambda x: 'Yes' if str(x).strip().lower() in ['yes', 'indexable'] else 'No')
    
    # Ensure Indexability Reason is consistent
    if 'Indexability Reason' in df.columns:
        df['Indexability Reason'] = df.apply(
            lambda row: '' if row.get('Indexable', '').lower() == 'yes' else row.get('Indexability Reason', ''),
            axis=1
        )
    
    # Define the desired column order
    main_cols = [
        'Original_URL',
        'Content_Type',
        'Initial_Status_Code',
        'Initial_Status_Type',
        'Indexable',
        'Indexability Reason',
        'Blocked by Robots.txt',
        'Robots Block Rule',
        'Final_URL',
        'Final_Status_Code',
        'Final_Status_Type',
        'Meta Robots',
        'X-Robots-Tag',
        'Canonical_URL',
        'H1',
        'Title',
        'Meta Description',
        'Word Count',
        'Crawl Time',
        'Last Modified',
        'HTML Lang',
        'Timestamp'
    ]
    
    # Get existing columns that aren't in main_cols
    other_cols = [col for col in df.columns if col not in main_cols]
    
    # Create final column order, keeping only columns that exist in the DataFrame
    ordered_cols = [col for col in main_cols if col in df.columns] + other_cols
    
    # Return DataFrame with ordered columns
    return df[ordered_cols]

def main():
    st.set_page_config(layout="wide")
    st.title("Web Crawler")

    st.sidebar.header("Configuration")
    
    # User Agent Selection
    ua_mode = st.sidebar.radio("User Agent Mode", ["Preset", "Custom"], horizontal=True)
    if ua_mode == "Preset":
        ua_choice = st.sidebar.selectbox("User Agent", list(USER_AGENTS.keys()))
        user_agent = USER_AGENTS[ua_choice]
    else:
        user_agent = st.sidebar.text_input("Custom User Agent", value=DEFAULT_USER_AGENT)

    # Speed Controls
    st.sidebar.subheader("Speed Controls")
    speed_mode = st.sidebar.radio("Speed Mode", ["Safe", "Dynamic", "Custom"], horizontal=True)
    
    if speed_mode == "Safe":
        concurrency = DEFAULT_CONCURRENCY
    elif speed_mode == "Dynamic":
        concurrency = st.sidebar.slider("Initial Urls/s", MIN_CONCURRENCY, MAX_CONCURRENCY, DEFAULT_CONCURRENCY)
        st.sidebar.info("Speed will automatically adjust based on server response")
    else:  # Custom
        concurrency = st.sidebar.slider("Urls/s", MIN_CONCURRENCY, MAX_CONCURRENCY, DEFAULT_CONCURRENCY)

    respect_robots = st.sidebar.checkbox("Respect robots.txt", value=True)
    mode = st.radio("Select Mode", ["Spider", "List", "Sitemap"], horizontal=True)
    st.write("----")

    # Session state for crawl control and results
    if 'is_crawling' not in st.session_state:
        st.session_state['is_crawling'] = False
    if 'crawl_results' not in st.session_state:
        st.session_state['crawl_results'] = []
    if 'crawl_done' not in st.session_state:
        st.session_state['crawl_done'] = False
    if 'checker' not in st.session_state:
        st.session_state['checker'] = None

    if mode == "Spider":
        st.subheader("Spider")
        seed_url = st.text_input("Seed URL", placeholder="Enter a single URL")
        include_sitemaps = st.checkbox("Include Sitemaps")
        sitemap_urls = []
        if include_sitemaps:
            sitemaps_text = st.text_area("Sitemap URLs (one per line)", "")
            if sitemaps_text.strip():
                raw_sitemaps = [s.strip() for s in sitemaps_text.splitlines() if s.strip()]
                with st.expander("Discovered Sitemap URLs", expanded=True):
                    table_ph = st.empty()
                    def show_partial_sitemap(all_urls):
                        df_temp = pd.DataFrame(all_urls, columns=["Discovered URLs"])
                        table_ph.dataframe(df_temp, height=500, use_container_width=True)
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    sitemap_urls = loop.run_until_complete(process_sitemaps(raw_sitemaps, show_partial_callback=show_partial_sitemap))
                    loop.close()
                    st.write(f"Collected {len(sitemap_urls)} URLs from sitemaps.")

        with st.expander("Advanced Filters (Optional)"):
            st.write("Regex to include or exclude discovered URLs in Crawl.")
            include_pattern = st.text_input("Include Regex", "")
            exclude_pattern = st.text_input("Exclude Regex", "")

        # Start/Stop Crawl Button
        crawl_btn_label = "Stop Crawl" if st.session_state['is_crawling'] else "Start Crawl"
        crawl_btn = st.button(crawl_btn_label)

        progress_ph = st.empty()
        progress_bar = st.progress(0.0)
        with st.expander("Intermediate Results", expanded=True):
            table_ph = st.empty()
            download_ph = st.empty()

        def show_partial_data(res_list, crawled_count, discovered_count):
            ratio = (crawled_count / discovered_count) if discovered_count > 0 else 0
            progress_bar.progress(ratio)
            remain = discovered_count - crawled_count
            pct = ratio * 100
            progress_ph.write(
                f"Completed {crawled_count} of {discovered_count} ({pct:.2f}%) → {remain} Remaining"
            )
            if crawled_count % 20 == 0 or crawled_count == discovered_count:
                df_temp = pd.DataFrame(res_list)
                df_temp = format_and_reorder_df(df_temp)
                table_ph.dataframe(df_temp, height=500, use_container_width=True)
                # Show download button only if crawl is done
                if st.session_state['crawl_done']:
                    csv_data = df_temp.to_csv(index=False)
                    csv_bytes = csv_data.encode("utf-8")
                    download_ph.download_button(
                        label="Download CSV",
                        data=csv_bytes,
                        file_name="crawl_results.csv",
                        mime="text/csv"
                    )

        # Handle crawl start/stop
        if crawl_btn:
            if not st.session_state['is_crawling']:
                # Start crawl
                if not seed_url.strip():
                    st.warning("No seed URL provided.")
                    return
                st.session_state['is_crawling'] = True
                st.session_state['crawl_results'] = []
                st.session_state['crawl_done'] = False
                checker = URLChecker(user_agent, concurrency, DEFAULT_TIMEOUT, respect_robots)
                st.session_state['checker'] = checker
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    results = loop.run_until_complete(
                        run_dynamic_crawl(
                            seed_url=normalize_url(seed_url.strip()),
                            checker=checker,
                            include_pattern=include_pattern,
                            exclude_pattern=exclude_pattern,
                            show_partial_callback=show_partial_data
                        )
                    )
                    st.session_state['crawl_results'] = results
                    st.session_state['crawl_done'] = True
                except Exception as e:
                    st.error(f"Error during crawl: {str(e)}")
                    logging.error(f"Crawl error: {e}")
                finally:
                    loop.close()
                st.session_state['is_crawling'] = False
            else:
                # Stop crawl
                if st.session_state['checker']:
                    st.session_state['checker'].stop()
                    st.info("Stopping crawl after current tasks complete...")

        # After crawl, show the final intermediate table and download button
        if st.session_state['crawl_done'] and st.session_state['crawl_results']:
            df_final = pd.DataFrame(st.session_state['crawl_results'])
            df_final = format_and_reorder_df(df_final)
            table_ph.dataframe(df_final, height=500, use_container_width=True)
            csv_data = df_final.to_csv(index=False)
            csv_bytes = csv_data.encode("utf-8")
            download_ph.download_button(
                label="Download CSV",
                data=csv_bytes,
                file_name="crawl_results.csv",
                mime="text/csv"
            )

    elif mode == "List":
        st.subheader("List Mode")
        list_input = st.text_area("Enter URLs (one per line)")
        if st.button("Start Crawl"):
            user_urls = [x.strip() for x in list_input.splitlines() if x.strip()]
            if not user_urls:
                st.warning("No URLs provided.")
                return

            progress_ph = st.empty()
            progress_bar = st.progress(0.0)
            with st.expander("Intermediate Results", expanded=True):
                table_ph = st.empty()

            def show_partial_data(res_list, done_count, total_count):
                ratio = done_count / total_count if total_count else 1.0
                progress_bar.progress(ratio)
                remain = total_count - done_count
                pct = ratio * 100
                progress_ph.write(
                    f"Completed {done_count} of {total_count} ({pct:.2f}%) → {remain} Remaining"
                )
                if done_count % 20 == 0 or done_count == total_count:
                    df_temp = pd.DataFrame(res_list)
                    df_temp = format_and_reorder_df(df_temp)
                    table_ph.dataframe(df_temp, height=500, use_container_width=True)

            checker = URLChecker(user_agent, concurrency, DEFAULT_TIMEOUT, respect_robots)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                results = loop.run_until_complete(
                    run_list_crawl(
                        urls=user_urls,
                        checker=checker,
                        show_partial_callback=show_partial_data
                    )
                )
            finally:
                loop.close()

            if not results:
                st.warning("No results from List Mode.")
                return

            df = pd.DataFrame(results)
            st.subheader("List Mode Results")
            st.dataframe(df, use_container_width=True)
            
            if df is not None and not df.empty:
                df = format_and_reorder_df(df)
                st.dataframe(df, use_container_width=True)
                csv_data = df.to_csv(index=False)
                csv_bytes = csv_data.encode("utf-8")
                st.download_button(
                    label="Download CSV",
                    data=csv_bytes,
                    file_name="crawl_results.csv",
                    mime="text/csv"
                )

            show_summary(df)

    else:  # Sitemap mode
        st.subheader("Sitemap Mode")
        st.write("Enter one or multiple sitemap URLs (one per line)")
        sitemap_text = st.text_area("Sitemap URLs", "")
        if st.button("Fetch & Crawl Sitemaps"):
            if not sitemap_text.strip():
                st.warning("No sitemap URLs provided.")
                return

            lines = [x.strip() for x in sitemap_text.splitlines() if x.strip()]
            with st.expander("Discovered Sitemap URLs", expanded=True):
                table_ph = st.empty()
                def show_partial_sitemap(all_urls):
                    df_temp = pd.DataFrame(all_urls, columns=["Discovered URLs"])
                    table_ph.dataframe(df_temp, height=500, use_container_width=True)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                all_sitemap_urls = loop.run_until_complete(process_sitemaps(lines, show_partial_callback=show_partial_sitemap))
                loop.close()

            if not all_sitemap_urls:
                st.warning("No URLs found in these sitemaps.")
                return

            progress_ph = st.empty()
            progress_bar = st.progress(0.0)
            with st.expander("Intermediate Results", expanded=True):
                table_ph = st.empty()

            def show_partial_data(res_list, done_count, total_count):
                ratio = done_count / total_count if total_count else 1.0
                progress_bar.progress(ratio)
                remain = total_count - done_count
                pct = ratio * 100
                progress_ph.write(
                    f"Completed {done_count} of {total_count} ({pct:.2f}%) → {remain} Remaining"
                )
                if done_count % 20 == 0 or done_count == total_count:
                    df_temp = pd.DataFrame(res_list)
                    df_temp = format_and_reorder_df(df_temp)
                    table_ph.dataframe(df_temp, height=500, use_container_width=True)

            checker = URLChecker(user_agent, concurrency, DEFAULT_TIMEOUT, respect_robots)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                results = loop.run_until_complete(
                    run_sitemap_crawl(
                        urls=all_sitemap_urls,
                        checker=checker,
                        show_partial_callback=show_partial_data
                    )
                )
            finally:
                loop.close()

            if not results:
                st.warning("No results from Sitemap Mode.")
                return

            df = pd.DataFrame(results)
            st.subheader("Sitemap Results")
            st.dataframe(df, use_container_width=True)
            
            if df is not None and not df.empty:
                df = format_and_reorder_df(df)
                st.dataframe(df, use_container_width=True)
                csv_data = df.to_csv(index=False)
                csv_bytes = csv_data.encode("utf-8")
                st.download_button(
                    label="Download CSV",
                    data=csv_bytes,
                    file_name="crawl_results.csv",
                    mime="text/csv"
                )

            show_summary(df)

def show_summary(df: pd.DataFrame):
    st.subheader("Summary")
    if df.empty:
        st.write("No data available for summary.")
        return

    def display_distribution(column_name: str, title: str):
        if column_name in df.columns:
            counts = df[column_name].value_counts(dropna=False).reset_index()
            counts.columns = [column_name, "Count"]
            st.write(f"**{title}**")
            st.table(counts)

    display_distribution("Initial_Status_Code", "Initial Status Code Distribution")
    display_distribution("Final_Status_Code", "Final Status Code Distribution")
    display_distribution("Blocked by Robots.txt", "Blocked by Robots.txt?")
    display_distribution("Indexable", "Indexable?")
    display_distribution("Indexability Reason", "Indexability Reasons")

if __name__ == "__main__":
    main() 