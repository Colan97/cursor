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
from typing import List, Dict, Set, Optional, Tuple
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
import requests
import platform
import psutil
import numpy as np
from scipy import stats
from tqdm import tqdm
import colorama
from colorama import Fore, Style
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.panel import Panel
from rich.logging import RichHandler
from typing_extensions import TypedDict
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import ssl
import certifi
from urllib3.util.retry import Retry
from urllib3.util import ssl_
import urllib.parse

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Set up event loop policy for Windows
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Constants
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_URLS = 1000
SAVE_INTERVAL = 100
MAX_RETRIES = 3
INITIAL_BACKOFF = 1
MAX_BACKOFF = 60
DEFAULT_USER_AGENT = "custom_adidas_seo_x3423/1.0"
ERROR_THRESHOLD = 0.15
MIN_CONCURRENCY = 5
MAX_CONCURRENCY = 100
DEFAULT_CONCURRENCY = 20
BATCH_SIZE = 100
RETRY_DELAYS = [1, 2, 4]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger("rich")
console = Console()

# User Agents
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
# Helper Functions
# -----------------------------
def save_results_to_file(results: List[Dict], filename: str):
    """Save results to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

def load_results_from_file(filename: str) -> List[Dict]:
    """Load results from a JSON file."""
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def calculate_error_rate(results: List[Dict]) -> float:
    """Calculate the error rate from recent results."""
    if not results:
        return 0.0
    error_count = sum(1 for r in results if str(r.get("Final_Status_Code", "")).startswith(("4", "5")))
    return error_count / len(results)

def adjust_concurrency(current_concurrency: int, error_rate: float) -> int:
    """Dynamically adjust concurrency based on error rate."""
    if error_rate > ERROR_THRESHOLD:
        return max(MIN_CONCURRENCY, current_concurrency - 2)
    elif error_rate < ERROR_THRESHOLD / 2:
        return min(MAX_CONCURRENCY, current_concurrency + 1)
    return current_concurrency

class URLChecker:
    def __init__(self, user_agent: str, concurrency: int, timeout: int, respect_robots: bool):
        self.user_agent = user_agent
        self.concurrency = concurrency
        self.timeout = timeout
        self.respect_robots = respect_robots
        self.robots_parser_cache = {}
        self.robots_cache_timestamp = {}
        self.robots_content_cache = {}  # Cache for raw robots.txt content
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
        self._pending_tasks = set()
        self.CACHE_EXPIRY = 3600  # 1 hour cache expiry
        self.last_crawl_time = {}
        self.ssl_context = None
        
        # New rate limiting attributes
        self.domain_delays = {}
        self.min_delay = 1.0
        self.max_delay = 5.0
        self.current_delay = self.min_delay
        self.error_count = 0
        self.success_count = 0
        self.last_request_time = {}
        self.memory_threshold = 0.8

    async def adjust_concurrency_if_needed(self):
        """Dynamically adjust concurrency based on error rate and performance."""
        try:
            now = datetime.now()
            if (now - self.last_adjustment_time).total_seconds() < self.adjustment_interval:
                return

            total_requests = self.error_count + self.success_count
            if total_requests > 0:
                error_rate = self.error_count / total_requests
                new_concurrency = adjust_concurrency(self.concurrency, error_rate)
                
                if new_concurrency != self.concurrency:
                    logging.info(f"Adjusting concurrency from {self.concurrency} to {new_concurrency} (error rate: {error_rate:.2%})")
                    self.concurrency = new_concurrency
                    # Update semaphore
                    if self.semaphore:
                        self.semaphore = asyncio.Semaphore(self.concurrency)
            
            self.last_adjustment_time = now
        except Exception as e:
            logging.error(f"Error adjusting concurrency: {e}")

    async def setup(self):
        """Initialize the aiohttp session and semaphore."""
        try:
            # Create SSL context with proper verification
            self.ssl_context = ssl.create_default_context(cafile=certifi.where())
            self.ssl_context.check_hostname = True
            self.ssl_context.verify_mode = ssl.CERT_REQUIRED

            connector = aiohttp.TCPConnector(
                limit=9999,
                ttl_dns_cache=300,
                enable_cleanup_closed=True,
                force_close=False,
                ssl=self.ssl_context
            )
            timeout_settings = aiohttp.ClientTimeout(
                total=None,
                connect=self.timeout,
                sock_read=self.timeout
            )
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout_settings,
                json_serialize=orjson.dumps,
                headers={"User-Agent": self.user_agent}
            )
            self.semaphore = asyncio.Semaphore(self.concurrency)
            logging.info(f"URLChecker initialized with concurrency {self.concurrency}")
        except Exception as e:
            logging.error(f"Error setting up URLChecker: {str(e)}")
            raise

    async def cleanup(self):
        """Clean up resources and cancel pending tasks."""
        self.stop_event.set()
        
        # Cancel all pending tasks
        for task in self._pending_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self._pending_tasks.clear()
        
        # Close session if it exists
        if self.session:
            await self.session.close()
            self.session = None

        # Save any remaining results
        if self.recent_results:
            save_results_to_file(self.recent_results, self.save_filename)
            logging.info(f"Saved {len(self.recent_results)} results to {self.save_filename}")

    def _is_cache_valid(self, base_url: str) -> bool:
        """Check if the robots.txt cache is still valid."""
        if base_url not in self.robots_cache_timestamp:
            return False
        cache_age = (datetime.now() - self.robots_cache_timestamp[base_url]).total_seconds()
        return cache_age < self.CACHE_EXPIRY

    async def _apply_crawl_delay(self, base_url: str, delay: float):
        """Apply crawl delay if needed."""
        if delay <= 0:
            return

        now = datetime.now()
        if base_url in self.last_crawl_time:
            time_since_last = (now - self.last_crawl_time[base_url]).total_seconds()
            if time_since_last < delay:
                wait_time = delay - time_since_last
                logging.info(f"Applying crawl delay of {wait_time:.2f} seconds for {base_url}")
                await asyncio.sleep(wait_time)
        
        self.last_crawl_time[base_url] = now

    def _parse_robots_content(self, content: str, base_url: str) -> robotparser.RobotFileParser:
        """Enhanced robots.txt parsing with better error handling."""
        parser = robotparser.RobotFileParser()
        parser.set_url(base_url)
        
        try:
            # Parse the content properly
            parser.parse(content.splitlines())
            return parser
        except Exception as e:
            logging.error(f"Error parsing robots.txt content: {e}")
            return parser

    async def check_robots(self, url: str) -> Tuple[bool, str]:
        """Enhanced robots.txt checking with better caching and error handling."""
        try:
            parsed_url_obj = urlparse(url)
            base_url = f"{parsed_url_obj.scheme}://{parsed_url_obj.netloc}"
            
            # Check cache first
            if base_url in self.robots_parser_cache and self._is_cache_valid(base_url):
                parser = self.robots_parser_cache[base_url]
                allowed = parser.can_fetch(self.user_agent, url)
                return allowed, "" if allowed else f"Disallowed by robots.txt rules"

            async with self.robots_semaphore:
                if self.stop_event.is_set():
                    return True, ""

                # Try multiple robots.txt locations
                robots_locations = [
                    f"{base_url}/robots.txt",
                    f"{base_url}/ROBOTS.TXT",
                    f"{base_url}/robots.txt/",
                    f"{base_url}/robots"
                ]

                content = None
                for robots_url in robots_locations:
                    try:
                        if not self.session:
                            await self.setup()

                        async with self.session.get(
                            robots_url,
                            ssl=self.ssl_context,
                            timeout=aiohttp.ClientTimeout(total=10),
                            headers={"User-Agent": self.user_agent}
                        ) as resp:
                            if resp.status == 200:
                                content = await resp.text()
                                self.robots_content_cache[base_url] = content
                                self.robots_cache_timestamp[base_url] = datetime.now()
                                logging.info(f"Fetched robots.txt from {robots_url}")
                                break
                            elif resp.status in [401, 403]:
                                logging.warning(f"Access to {robots_url} forbidden (status {resp.status})")
                                return True, ""
                            elif resp.status in [404, 410]:
                                continue
                            else:
                                logging.warning(f"Unexpected status {resp.status} for {robots_url}")
                                continue
                    except Exception as e:
                        logging.warning(f"Error fetching {robots_url}: {e}")
                        continue

                if not content:
                    logging.info(f"No robots.txt found for {base_url}")
                    return True, ""

                # Parse robots.txt content
                parser = self._parse_robots_content(content, base_url)
                self.robots_parser_cache[base_url] = parser

                # Check if URL is allowed
                allowed = parser.can_fetch(self.user_agent, url)
                if not allowed:
                    logging.info(f"URL {url} blocked by robots.txt")
                    return False, f"Disallowed by robots.txt rules"

                # Apply crawl delay if specified
                crawl_delay = parser.crawl_delay(self.user_agent)
                if crawl_delay:
                    await self._apply_crawl_delay(base_url, float(crawl_delay))

                return True, ""

        except asyncio.CancelledError:
            logging.info(f"Robots check cancelled for {url}")
            return True, ""
        except Exception as e:
            logging.error(f"Error in check_robots for {url}: {e}", exc_info=True)
            return True, ""

    async def _check_memory_usage(self):
        """Check if memory usage is too high and adjust accordingly."""
        try:
            memory = psutil.Process().memory_percent()
            if memory > self.memory_threshold * 100:
                logging.warning(f"High memory usage detected: {memory:.1f}%")
                # Clear some caches
                self.robots_parser_cache.clear()
                self.robots_cache_timestamp.clear()
                # Force garbage collection
                import gc
                gc.collect()
                return True
        except Exception as e:
            logging.error(f"Error checking memory usage: {e}")
        return False

    async def _apply_rate_limit(self, domain: str):
        """Apply rate limiting based on domain and error rate."""
        try:
            now = time.time()
            if domain in self.last_request_time:
                time_since_last = now - self.last_request_time[domain]
                if time_since_last < self.current_delay:
                    await asyncio.sleep(self.current_delay - time_since_last)
            
            # Adjust delay based on error rate
            total_requests = self.error_count + self.success_count
            if total_requests > 0:
                error_rate = self.error_count / total_requests
                if error_rate > 0.1:  # If error rate is above 10%
                    self.current_delay = min(self.current_delay * 1.5, self.max_delay)
                elif error_rate < 0.05:  # If error rate is below 5%
                    self.current_delay = max(self.current_delay * 0.8, self.min_delay)
            
            self.last_request_time[domain] = time.time()
        except Exception as e:
            logging.error(f"Error in rate limiting: {e}")

    async def fetch_and_parse(self, url: str) -> Dict:
        async with self.semaphore:
            await self.adjust_concurrency_if_needed()
            
            # Check memory usage
            if await self._check_memory_usage():
                logging.warning("Memory usage high, clearing caches")
            
            # Apply rate limiting
            domain = urlparse(url).netloc
            await self._apply_rate_limit(domain)
            
            logging.info(f"Fetching and parsing URL: {url}")
            data = {
                "Original_URL": url,
                "Encoded_URL": "",
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
                "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }

            try:
                # Check robots.txt before making the request
                if self.respect_robots:
                    allowed, blocking_rule = await self.check_robots(url)
                    data["Blocked by Robots.txt"] = "No" if allowed else "Yes"
                    data["Robots Block Rule"] = blocking_rule if not allowed else ""
                    if not allowed:
                        data["Indexable"] = "No"
                        data["Indexability Reason"] = f"Blocked by robots.txt: {blocking_rule}"
                        logging.info(f"URL {url} blocked by robots.txt. Rule: {blocking_rule}")
                        return data

                # Make the request with retry logic
                for attempt in range(3):
                    try:
                        async with self.session.get(url, ssl=self.ssl_context, allow_redirects=True) as resp:
                            # Update success/error counts
                            if resp.status == 200:
                                self.success_count += 1
                            else:
                                self.error_count += 1

                            # Add Content-Type field before status codes
                            data["Content_Type"] = resp.headers.get("Content-Type", "")
                            init_str = str(resp.status)
                            data["Initial_Status_Code"] = init_str
                            data["Initial_Status_Type"] = self.status_label(resp.status)
                            data["Final_URL"] = str(resp.url)
                            data["Encoded_URL"] = urllib.parse.quote(str(resp.url), safe=':/?=&')
                            data["Final_Status_Code"] = str(resp.status)
                            data["Final_Status_Type"] = self.status_label(resp.status)
                            data["Last Modified"] = resp.headers.get("Last-Modified", "")

                            # Mark non-200 pages as non-indexable
                            if resp.status != 200:
                                data["Indexable"] = "No"
                                data["Indexability Reason"] = f"HTTP Status {resp.status}"

                            if resp.status == 200 and resp.content_type and resp.content_type.startswith("text/html"):
                                content = await resp.text(errors='replace')
                                result = self.parse_html_content(data, content, resp.headers, resp.status, True)
                                result = update_redirect_label(result, url)
                                logging.info(f"Successfully parsed HTML content from {url}")
                                
                                # Add to recent results and check if we need to save
                                self.recent_results.append(result)
                                if len(self.recent_results) >= SAVE_INTERVAL:
                                    save_results_to_file(self.recent_results, self.save_filename)
                                    logging.info(f"Saved {len(self.recent_results)} results to {self.save_filename}")
                                    self.recent_results = []  # Clear after saving
                                    self.last_save_time = datetime.now()
                                
                                return result
                            else:
                                data["Indexability Reason"] = "Non-200 or non-HTML"
                                data = update_redirect_label(data, url)
                                logging.info(f"Non-HTML or non-200 response from {url}: {init_str}")
                                return data
                    except asyncio.TimeoutError:
                        if attempt == 2:  # Last attempt
                            raise
                        logging.warning(f"Timeout on attempt {attempt + 1} for {url}, retrying...")
                        await asyncio.sleep(1)  # Wait before retry
                    except Exception as e:
                        if attempt == 2:  # Last attempt
                            raise
                        logging.warning(f"Error on attempt {attempt + 1} for {url}: {str(e)}, retrying...")
                        await asyncio.sleep(1)  # Wait before retry

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
            
            # Check for noindex in meta robots (case-insensitive)
            meta_robots_lower = data["Meta Robots"].lower()
            x_robots_lower = data["X-Robots-Tag"].lower()
            
            # Check for noindex directives
            if "noindex" in meta_robots_lower or "noindex" in x_robots_lower:
                data["Indexable"] = "No"
                data["Indexability Reason"] = "Noindex directive"
            
            # Check for nofollow directives
            if "nofollow" in meta_robots_lower or "nofollow" in x_robots_lower:
                if data["Indexability Reason"]:
                    data["Indexability Reason"] += " and Nofollow directive"
                else:
                    data["Indexable"] = "No"
                    data["Indexability Reason"] = "Nofollow directive"
            
            # Check for canonical URL mismatch
            if data["Canonical_URL"] and data["Canonical_URL"] != data["Final_URL"]:
                data["Indexable"] = "No"
                data["Indexability Reason"] = "Canonical URL mismatch"
            
            # Check for empty content
            if not data["Title"] and not data["H1"] and not data["Meta Description"]:
                data["Indexable"] = "No"
                data["Indexability Reason"] = "Empty content (no title, H1, or meta description)"
            
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
                discovered_links = await discover_links(norm_url, checker.session, checker.user_agent)
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
        await checker.cleanup()

async def discover_links(url: str, session: aiohttp.ClientSession, user_agent: str) -> List[str]:
    """
    Discover links from a page.
    """
    out = []
    headers = {"User-Agent": user_agent}
    try:
        async with session.get(url, headers=headers, ssl=False, allow_redirects=True) as resp:
            if resp.status == 200 and resp.content_type and resp.content_type.startswith("text/html"):
                text = await resp.text(errors='replace')
                soup = BeautifulSoup(text, "lxml")
                for a in soup.find_all("a", href=True):
                    try:
                        abs_link = urljoin(url, a["href"])
                        if abs_link and abs_link.startswith(('http://', 'https://')):
                            out.append(abs_link)
                    except Exception as e:
                        logging.error(f"Error processing link {a['href']}: {str(e)}")
                        continue
    except Exception as e:
        logging.error(f"Error discovering links from {url}: {str(e)}")
    return list(set(out))  # Remove duplicates

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
        
        await checker.cleanup()  # Use cleanup instead of close
        return results
    except Exception as e:
        logging.error(f"Error in dynamic crawl: {e}")
        await checker.cleanup()  # Ensure cleanup happens even on error
        return []

async def run_list_crawl(urls: List[str], checker: URLChecker, show_partial_callback) -> List[Dict]:
    """Async wrapper for list mode crawl."""
    try:
        results = await chunk_process(urls, checker, show_partial_callback=show_partial_callback)
        
        # Recrawl failed URLs if any
        if checker.failed_urls:
            recrawl_results = await checker.recrawl_failed_urls()
            results.extend(recrawl_results)
        
        await checker.cleanup()  # Use cleanup instead of close
        return results
    except Exception as e:
        logging.error(f"Error in list crawl: {e}")
        await checker.cleanup()  # Ensure cleanup happens even on error
        return []

async def run_sitemap_crawl(urls: List[str], checker: URLChecker, show_partial_callback) -> List[Dict]:
    """Async wrapper for sitemap mode crawl."""
    try:
        results = await chunk_process(urls, checker, show_partial_callback=show_partial_callback)
        
        # Recrawl failed URLs if any
        if checker.failed_urls:
            recrawl_results = await checker.recrawl_failed_urls()
            results.extend(recrawl_results)
        
        await checker.cleanup()  # Use cleanup instead of close
        return results
    except Exception as e:
        logging.error(f"Error in sitemap crawl: {e}")
        await checker.cleanup()  # Ensure cleanup happens even on error
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
    Process a list of URLs in chunks.
    """
    results = []
    total = len(urls)
    processed = 0
    
    try:
        await checker.setup()
        tasks = []
        for url in urls:
            tasks.append(checker.fetch_and_parse(url))
        
        for future in asyncio.as_completed(tasks):
            try:
                result = await future
                results.append(result)
                processed += 1
                if show_partial_callback:
                    show_partial_callback(results, processed, total)
            except Exception as e:
                logging.error(f"Error processing URL: {e}")
                processed += 1
    except Exception as e:
        logging.error(f"Error in chunk processing: {e}")
    finally:
        await checker.cleanup()
    
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
        'Last Modified',
        'HTML Lang',
        'Timestamp',
        'Encoded_URL'  # Moved to last position
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

        # Create a container for the button to maintain its position
        button_container = st.container()
        
        # Initialize session state for crawl control
        if 'is_crawling' not in st.session_state:
            st.session_state['is_crawling'] = False
        if 'crawl_results' not in st.session_state:
            st.session_state['crawl_results'] = []
        if 'crawl_done' not in st.session_state:
            st.session_state['crawl_done'] = False
        if 'checker' not in st.session_state:
            st.session_state['checker'] = None

        # Place the button in the container
        with button_container:
            if st.session_state['is_crawling']:
                if st.button("Stop Crawl", type="primary"):
                    if st.session_state['checker']:
                        st.session_state['checker'].stop()
                        st.session_state['is_crawling'] = False
                        # Save current results before stopping
                        if st.session_state['checker'].recent_results:
                            st.session_state['crawl_results'].extend(st.session_state['checker'].recent_results)
                            st.session_state['crawl_done'] = True
                        st.info("Stopping crawl after current tasks complete...")
                        st.rerun()
            else:
                if st.button("Start Crawl", type="primary"):
                    if not seed_url.strip():
                        st.warning("No seed URL provided.")
                        return
                    st.session_state['is_crawling'] = True
                    st.session_state['crawl_results'] = []
                    st.session_state['crawl_done'] = False
                    checker = URLChecker(user_agent, concurrency, DEFAULT_TIMEOUT, respect_robots)
                    st.session_state['checker'] = checker
                    st.rerun()

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
                # Combine current results with any previously saved results
                all_results = st.session_state['crawl_results'] + res_list
                df_temp = pd.DataFrame(all_results)
                df_temp = format_and_reorder_df(df_temp)
                table_ph.dataframe(df_temp, height=500, use_container_width=True)
                # Show download button if crawl is done or stopped
                if st.session_state['crawl_done']:
                    csv_data = df_temp.to_csv(index=False)
                    csv_bytes = csv_data.encode("utf-8")
                    download_ph.download_button(
                        label="Download CSV",
                        data=csv_bytes,
                        file_name="crawl_results.csv",
                        mime="text/csv"
                    )

        # Start the crawl if is_crawling is True
        if st.session_state['is_crawling']:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                results = loop.run_until_complete(
                    run_dynamic_crawl(
                        seed_url=normalize_url(seed_url.strip()),
                        checker=st.session_state['checker'],
                        include_pattern=include_pattern,
                        exclude_pattern=exclude_pattern,
                        show_partial_callback=show_partial_data
                    )
                )
                # Combine new results with any existing results
                st.session_state['crawl_results'].extend(results)
                st.session_state['crawl_done'] = True
                st.session_state['is_crawling'] = False
                st.rerun()
            except Exception as e:
                st.error(f"Error during crawl: {str(e)}")
                logging.error(f"Crawl error: {e}")
                # Preserve results even if there's an error
                if st.session_state['checker'].recent_results:
                    st.session_state['crawl_results'].extend(st.session_state['checker'].recent_results)
                    st.session_state['crawl_done'] = True
                st.session_state['is_crawling'] = False
                st.rerun()
            finally:
                loop.close()

        # Show final results if crawl is done (either completed or stopped)
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
        
        # Initialize session state for List mode
        if 'list_is_crawling' not in st.session_state:
            st.session_state['list_is_crawling'] = False
        if 'list_crawl_results' not in st.session_state:
            st.session_state['list_crawl_results'] = []
        if 'list_crawl_done' not in st.session_state:
            st.session_state['list_crawl_done'] = False
        if 'list_checker' not in st.session_state:
            st.session_state['list_checker'] = None

        # Create button container
        button_container = st.container()
        
        # Place the button in the container
        with button_container:
            if st.session_state['list_is_crawling']:
                if st.button("Stop Crawl", type="primary"):
                    if st.session_state['list_checker']:
                        st.session_state['list_checker'].stop()
                        st.session_state['list_is_crawling'] = False
                        # Save current results before stopping
                        if st.session_state['list_checker'].recent_results:
                            st.session_state['list_crawl_results'].extend(st.session_state['list_checker'].recent_results)
                            st.session_state['list_crawl_done'] = True
                        st.info("Stopping crawl after current tasks complete...")
                        st.rerun()
            else:
                if st.button("Start Crawl", type="primary"):
                    user_urls = [x.strip() for x in list_input.splitlines() if x.strip()]
                    if not user_urls:
                        st.warning("No URLs provided.")
                        return
                    st.session_state['list_is_crawling'] = True
                    st.session_state['list_crawl_results'] = []
                    st.session_state['list_crawl_done'] = False
                    checker = URLChecker(user_agent, concurrency, DEFAULT_TIMEOUT, respect_robots)
                    st.session_state['list_checker'] = checker
                    st.rerun()

        progress_ph = st.empty()
        progress_bar = st.progress(0.0)
        with st.expander("Results", expanded=True):
            table_ph = st.empty()
            download_ph = st.empty()

        def show_partial_data(res_list, done_count, total_count):
            ratio = done_count / total_count if total_count else 1.0
            progress_bar.progress(ratio)
            remain = total_count - done_count
            pct = ratio * 100
            progress_ph.write(
                f"Completed {done_count} of {total_count} ({pct:.2f}%) → {remain} Remaining"
            )
            if done_count % 20 == 0 or done_count == total_count:
                # Combine current results with any previously saved results
                all_results = st.session_state['list_crawl_results'] + res_list
                df_temp = pd.DataFrame(all_results)
                df_temp = format_and_reorder_df(df_temp)
                table_ph.dataframe(df_temp, height=500, use_container_width=True)
                if st.session_state['list_crawl_done']:
                    csv_data = df_temp.to_csv(index=False)
                    csv_bytes = csv_data.encode("utf-8")
                    download_ph.download_button(
                        label="Download CSV",
                        data=csv_bytes,
                        file_name="crawl_results.csv",
                        mime="text/csv"
                    )

        # Start the crawl if is_crawling is True
        if st.session_state['list_is_crawling']:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                results = loop.run_until_complete(
                    run_list_crawl(
                        urls=[x.strip() for x in list_input.splitlines() if x.strip()],
                        checker=st.session_state['list_checker'],
                        show_partial_callback=show_partial_data
                    )
                )
                # Combine new results with any existing results
                st.session_state['list_crawl_results'].extend(results)
                st.session_state['list_crawl_done'] = True
                st.session_state['list_is_crawling'] = False
                st.rerun()
            except Exception as e:
                st.error(f"Error during crawl: {str(e)}")
                logging.error(f"Crawl error: {e}")
                # Preserve results even if there's an error
                if st.session_state['list_checker'].recent_results:
                    st.session_state['list_crawl_results'].extend(st.session_state['list_checker'].recent_results)
                    st.session_state['list_crawl_done'] = True
                st.session_state['list_is_crawling'] = False
                st.rerun()
            finally:
                loop.close()

        # Show final results if crawl is done (either completed or stopped)
        if st.session_state['list_crawl_done'] and st.session_state['list_crawl_results']:
            df_final = pd.DataFrame(st.session_state['list_crawl_results'])
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

    else:  # Sitemap mode
        st.subheader("Sitemap Mode")
        st.write("Enter one or multiple sitemap URLs (one per line)")
        sitemap_text = st.text_area("Sitemap URLs", "")
        
        # Initialize session state for Sitemap mode
        if 'sitemap_is_crawling' not in st.session_state:
            st.session_state['sitemap_is_crawling'] = False
        if 'sitemap_crawl_results' not in st.session_state:
            st.session_state['sitemap_crawl_results'] = []
        if 'sitemap_crawl_done' not in st.session_state:
            st.session_state['sitemap_crawl_done'] = False
        if 'sitemap_checker' not in st.session_state:
            st.session_state['sitemap_checker'] = None
        if 'sitemap_urls' not in st.session_state:
            st.session_state['sitemap_urls'] = []

        # Create button container
        button_container = st.container()
        
        # Place the button in the container
        with button_container:
            if st.session_state['sitemap_is_crawling']:
                if st.button("Stop Crawl", type="primary"):
                    if st.session_state['sitemap_checker']:
                        st.session_state['sitemap_checker'].stop()
                        st.session_state['sitemap_is_crawling'] = False
                        # Save current results before stopping
                        if st.session_state['sitemap_checker'].recent_results:
                            st.session_state['sitemap_crawl_results'].extend(st.session_state['sitemap_checker'].recent_results)
                            st.session_state['sitemap_crawl_done'] = True
                        st.info("Stopping crawl after current tasks complete...")
                        st.rerun()
            else:
                if st.button("Start Crawl", type="primary"):
                    if not sitemap_text.strip():
                        st.warning("No sitemap URLs provided.")
                        return
                    
                    # First process sitemaps to get URLs
                    lines = [x.strip() for x in sitemap_text.splitlines() if x.strip()]
                    with st.expander("Discovered URLs", expanded=True):
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

                    st.session_state['sitemap_urls'] = all_sitemap_urls
                    st.session_state['sitemap_is_crawling'] = True
                    st.session_state['sitemap_crawl_results'] = []
                    st.session_state['sitemap_crawl_done'] = False
                    checker = URLChecker(user_agent, concurrency, DEFAULT_TIMEOUT, respect_robots)
                    st.session_state['sitemap_checker'] = checker
                    st.rerun()

        progress_ph = st.empty()
        progress_bar = st.progress(0.0)
        with st.expander("Results", expanded=True):
            table_ph = st.empty()
            download_ph = st.empty()

        def show_partial_data(res_list, done_count, total_count):
            ratio = done_count / total_count if total_count else 1.0
            progress_bar.progress(ratio)
            remain = total_count - done_count
            pct = ratio * 100
            progress_ph.write(
                f"Completed {done_count} of {total_count} ({pct:.2f}%) → {remain} Remaining"
            )
            if done_count % 20 == 0 or done_count == total_count:
                # Combine current results with any previously saved results
                all_results = st.session_state['sitemap_crawl_results'] + res_list
                df_temp = pd.DataFrame(all_results)
                df_temp = format_and_reorder_df(df_temp)
                table_ph.dataframe(df_temp, height=500, use_container_width=True)
                if st.session_state['sitemap_crawl_done']:
                    csv_data = df_temp.to_csv(index=False)
                    csv_bytes = csv_data.encode("utf-8")
                    download_ph.download_button(
                        label="Download CSV",
                        data=csv_bytes,
                        file_name="crawl_results.csv",
                        mime="text/csv"
                    )

        # Start the crawl if is_crawling is True
        if st.session_state['sitemap_is_crawling']:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                results = loop.run_until_complete(
                    run_sitemap_crawl(
                        urls=st.session_state['sitemap_urls'],
                        checker=st.session_state['sitemap_checker'],
                        show_partial_callback=show_partial_data
                    )
                )
                # Combine new results with any existing results
                st.session_state['sitemap_crawl_results'].extend(results)
                st.session_state['sitemap_crawl_done'] = True
                st.session_state['sitemap_is_crawling'] = False
                st.rerun()
            except Exception as e:
                st.error(f"Error during crawl: {str(e)}")
                logging.error(f"Crawl error: {e}")
                # Preserve results even if there's an error
                if st.session_state['sitemap_checker'].recent_results:
                    st.session_state['sitemap_crawl_results'].extend(st.session_state['sitemap_checker'].recent_results)
                    st.session_state['sitemap_crawl_done'] = True
                st.session_state['sitemap_is_crawling'] = False
                st.rerun()
            finally:
                loop.close()

        # Show final results if crawl is done (either completed or stopped)
        if st.session_state['sitemap_crawl_done'] and st.session_state['sitemap_crawl_results']:
            df_final = pd.DataFrame(st.session_state['sitemap_crawl_results'])
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
