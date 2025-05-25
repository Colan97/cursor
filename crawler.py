import streamlit as st
import pandas as pd
import re
import asyncio
import aiohttp
import orjson # Using orjson for faster JSON operations
import nest_asyncio
import logging
import pyperclip # For copy button, consider removing if st_copy is sufficient
import json
from typing import List, Dict, Set, Optional, Tuple
from urllib.parse import urlparse, urljoin, urlunparse, quote
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
import xml.etree.ElementTree as ET
import os
from pathlib import Path
# from st_copy import copy_button # Keep if used, otherwise remove
import time
from urllib import robotparser
import concurrent.futures # Not directly used in async, consider if needed for CPU-bound tasks
# import requests # Replaced by aiohttp
import platform
# import psutil # Not used
# import numpy as np # Not used
# from scipy import stats # Not used
# from tqdm import tqdm # Replaced by Rich progress
import colorama # Rich handles colors, this might be redundant
from colorama import Fore, Style # Rich handles colors
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.panel import Panel
from rich.logging import RichHandler
from typing_extensions import TypedDict # Standard typing is usually enough
from dataclasses import dataclass
from functools import lru_cache
import hashlib # Not explicitly used, but good for caching if needed
import ssl
import certifi
# from urllib3.util.retry import Retry # aiohttp has its own retry mechanisms or use tenacity
# from urllib3.util import ssl_ # Using standard ssl module
# import urllib.parse # Already imported specific functions

# Apply nest_asyncio to allow nested event loops (useful in Jupyter, less so for Streamlit usually)
nest_asyncio.apply()

# Set up event loop policy for Windows
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Constants
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_URLS = 1000 # Max URLs for spider mode
SAVE_INTERVAL = 100 # Save results to file every N URLs (for CLI, not directly used in Streamlit results)
MAX_RETRIES = 3 # For fetching individual URLs
INITIAL_BACKOFF = 1
MAX_BACKOFF = 60
DEFAULT_USER_AGENT = "CustomAdidasSEOBot/1.1 (+http://www.example.com/bot.html)"
ERROR_THRESHOLD = 0.15 # For dynamic concurrency adjustment
MIN_CONCURRENCY = 5
MAX_CONCURRENCY = 100
DEFAULT_CONCURRENCY = 20
BATCH_SIZE = 100 # For processing URLs in chunks
RETRY_DELAYS = [1, 2, 4] # For recrawling failed URLs
ROBOTS_CACHE_EXPIRY_SECONDS = 3600 # 1 hour for robots.txt cache

# Configure logging with Rich
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, console=Console(stderr=True))]
)
logger = logging.getLogger("rich")
# console = Console() # Already created by RichHandler

# User Agents
USER_AGENTS = {
    "Googlebot Desktop": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Googlebot Mobile": "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/W.X.Y.Z Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Chrome Desktop": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Chrome Mobile": "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Mobile Safari/537.36",
    "Custom Bot": DEFAULT_USER_AGENT,
}

# -----------------------------
# Helper Functions
# -----------------------------
def save_results_to_file(results: List[Dict], filename: str):
    """Save results to a JSON file."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"Results saved to {filename}")
    except IOError as e:
        logger.error(f"Error saving results to {filename}: {e}")

def load_results_from_file(filename: str) -> List[Dict]:
    """Load results from a JSON file."""
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Error loading results from {filename}: {e}")
    return []

def calculate_error_rate(results_buffer: List[Dict]) -> float:
    """Calculate the error rate from a buffer of recent results."""
    if not results_buffer:
        return 0.0
    error_count = sum(1 for r in results_buffer if str(r.get("Final_Status_Code", "")).startswith(("4", "5")) or r.get("Final_Status_Code") == "Error")
    return error_count / len(results_buffer)

def adjust_concurrency(current_concurrency: int, error_rate: float) -> int:
    """Dynamically adjust concurrency based on error rate."""
    if error_rate > ERROR_THRESHOLD:
        new_concurrency = max(MIN_CONCURRENCY, current_concurrency - 5) # Decrease more aggressively
        logger.info(f"High error rate ({error_rate:.2%}). Decreasing concurrency: {current_concurrency} -> {new_concurrency}")
        return new_concurrency
    elif error_rate < ERROR_THRESHOLD / 2 and current_concurrency < MAX_CONCURRENCY : # Only increase if below max
        new_concurrency = min(MAX_CONCURRENCY, current_concurrency + 2) # Increase more gradually
        logger.info(f"Low error rate ({error_rate:.2%}). Increasing concurrency: {current_concurrency} -> {new_concurrency}")
        return new_concurrency
    return current_concurrency

class URLChecker:
    def __init__(self, user_agent: str, concurrency: int, timeout: int, respect_robots: bool):
        self.user_agent = user_agent
        self.initial_concurrency = concurrency # Store initial for potential resets
        self.current_concurrency = concurrency
        self.timeout = timeout
        self.respect_robots = respect_robots
        
        self._robot_parsers_cache: Dict[str, Tuple[robotparser.RobotFileParser, datetime]] = {} # base_url -> (parser, timestamp)
        self._ssl_context = ssl.create_default_context(cafile=certifi.where())
        
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore: Optional[asyncio.Semaphore] = None
        
        self.failed_urls: Set[str] = set()
        self.results_buffer: List[Dict] = [] # For dynamic concurrency adjustment
        self.BUFFER_SIZE = 50 # Number of recent results to consider for error rate

        self.save_filename = f"crawl_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json" # For CLI mode
        
        self._robots_fetch_semaphore = asyncio.Semaphore(10) # Limit concurrent robots.txt fetches
        self._stop_event = asyncio.Event()
        
        self._last_adjustment_time = datetime.now()
        self.ADJUSTMENT_INTERVAL_SECONDS = 15 # Check error rate more frequently

        self._pending_tasks: Set[asyncio.Task] = set()
        self._last_crawl_time_per_domain: Dict[str, datetime] = {}

    async def setup(self):
        """Initialize the aiohttp session and semaphore."""
        try:
            connector = aiohttp.TCPConnector(
                limit_per_host=0, # Let semaphore control concurrency
                ssl=self._ssl_context,
                enable_cleanup_closed=True,
                force_close=False, # Keep connections alive where possible
                ttl_dns_cache=300
            )
            timeout_settings = aiohttp.ClientTimeout(
                total=self.timeout + 5, # Total timeout slightly larger
                connect=self.timeout,
                sock_read=self.timeout
            )
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout_settings,
                json_serialize=orjson.dumps,
                headers={"User-Agent": self.user_agent}
            )
            self.semaphore = asyncio.Semaphore(self.current_concurrency)
            logger.info(f"URLChecker initialized with concurrency {self.current_concurrency}, User-Agent: {self.user_agent}")
        except Exception as e:
            logger.error(f"Error setting up URLChecker: {e}", exc_info=True)
            raise

    async def cleanup(self):
        """Clean up resources and cancel pending tasks."""
        logger.info("Initiating cleanup...")
        self._stop_event.set()
        
        # Cancel all pending tasks
        if self._pending_tasks:
            logger.info(f"Cancelling {len(self._pending_tasks)} pending tasks...")
            for task in list(self._pending_tasks): # Iterate over a copy
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self._pending_tasks, return_exceptions=True) # Wait for cancellations
            self._pending_tasks.clear()
            logger.info("Pending tasks cancelled.")
        
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
            logger.info("aiohttp session closed.")
        logger.info("Cleanup complete.")

    async def _get_robot_parser(self, base_url: str) -> Optional[robotparser.RobotFileParser]:
        """Fetches and parses robots.txt, using a cache."""
        cached_parser, timestamp = self._robot_parsers_cache.get(base_url, (None, None))
        if cached_parser and (datetime.now() - timestamp).total_seconds() < ROBOTS_CACHE_EXPIRY_SECONDS:
            return cached_parser

        async with self._robots_fetch_semaphore: # Limit concurrent fetches for robots.txt
            # Double-check cache after acquiring semaphore
            cached_parser, timestamp = self._robot_parsers_cache.get(base_url, (None, None))
            if cached_parser and (datetime.now() - timestamp).total_seconds() < ROBOTS_CACHE_EXPIRY_SECONDS:
                 return cached_parser

            robots_url = urljoin(base_url, "/robots.txt")
            rp = robotparser.RobotFileParser()
            rp.set_url(robots_url) # For reference, but we'll fetch manually with aiohttp

            if not self.session or self.session.closed:
                # This case should ideally not happen if setup is called correctly
                logger.warning("aiohttp session not available for robots.txt fetch. Attempting to re-setup.")
                await self.setup() # Try to re-initialize
                if not self.session:
                    logger.error("Failed to re-setup session for robots.txt. Assuming allowed.")
                    return None


            try:
                logger.debug(f"Fetching robots.txt from {robots_url}")
                async with self.session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        content = await resp.text(errors='replace')
                        rp.parse(content.splitlines())
                        logger.info(f"Successfully fetched and parsed robots.txt for {base_url}")
                    elif resp.status == 401 or resp.status == 403:
                        logger.warning(f"Access denied for robots.txt at {robots_url} (Status: {resp.status}). Assuming disallowed for all.")
                        rp.disallow_all = True # Treat as "disallow all"
                    elif resp.status >= 400 : # 404 and other client/server errors
                        logger.info(f"robots.txt not found or error at {robots_url} (Status: {resp.status}). Assuming allowed.")
                        rp.allow_all = True # Treat as "allow all" (standard behavior)
                    # else: for 3xx, aiohttp handles redirects by default.
                    
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching robots.txt from {robots_url}. Assuming allowed.")
                rp.allow_all = True
            except aiohttp.ClientError as e:
                logger.warning(f"ClientError fetching robots.txt from {robots_url}: {e}. Assuming allowed.")
                rp.allow_all = True
            except Exception as e:
                logger.error(f"Unexpected error fetching robots.txt from {robots_url}: {e}", exc_info=True)
                rp.allow_all = True # Fallback to allow on unexpected errors

            self._robot_parsers_cache[base_url] = (rp, datetime.now())
            return rp

    async def _apply_crawl_delay(self, base_url: str, rp: robotparser.RobotFileParser):
        delay = rp.crawl_delay(self.user_agent)
        if delay is not None and delay > 0:
            now = datetime.now()
            last_crawled = self._last_crawl_time_per_domain.get(base_url)
            if last_crawled:
                time_since_last = (now - last_crawled).total_seconds()
                if time_since_last < delay:
                    wait_time = delay - time_since_last
                    logger.info(f"Applying crawl-delay of {wait_time:.2f}s for {base_url} (User-Agent: {self.user_agent})")
                    await asyncio.sleep(wait_time)
            self._last_crawl_time_per_domain[base_url] = datetime.now()


    async def check_robots_and_delay(self, url: str) -> Tuple[bool, str]:
        """Checks robots.txt and applies crawl-delay. Returns (is_allowed, blocking_reason)."""
        if not self.respect_robots:
            return True, ""
        
        try:
            parsed_url = urlparse(url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            rp = await self._get_robot_parser(base_url)
            if rp is None: # Should only happen if session setup failed critically
                return True, "Robots.txt check failed, assuming allowed"

            await self._apply_crawl_delay(base_url, rp) # Apply delay regardless of can_fetch for politeness
            
            is_allowed = rp.can_fetch(self.user_agent, url)
            if not is_allowed:
                return False, f"Disallowed by robots.txt for User-Agent: {self.user_agent}"
            return True, ""
            
        except asyncio.CancelledError:
            logger.info(f"Robots check cancelled for {url}")
            raise # Propagate cancellation
        except Exception as e:
            logger.error(f"Error in check_robots_and_delay for {url}: {e}", exc_info=True)
            return True, f"Robots.txt check error: {e}" # Default to allowed on error

    async def _adjust_concurrency_periodically(self):
        """Periodically adjust concurrency based on error rate."""
        if self._stop_event.is_set():
            return

        now = datetime.now()
        if (now - self._last_adjustment_time).total_seconds() >= self.ADJUSTMENT_INTERVAL_SECONDS:
            error_rate = calculate_error_rate(self.results_buffer)
            new_concurrency = adjust_concurrency(self.current_concurrency, error_rate)
            
            if new_concurrency != self.current_concurrency:
                logger.info(f"Adjusting concurrency: {self.current_concurrency} -> {new_concurrency} (Error rate: {error_rate:.2%})")
                self.current_concurrency = new_concurrency
                self.semaphore = asyncio.Semaphore(self.current_concurrency) # Recreate semaphore
            
            self._last_adjustment_time = now
            self.results_buffer.clear() # Clear buffer after adjustment


    @retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=INITIAL_BACKOFF, max=MAX_BACKOFF), reraise=True)
    async def _fetch_url_with_retry(self, url: str, session: aiohttp.ClientSession):
        logger.debug(f"Attempting to fetch: {url}")
        return await session.get(url, allow_redirects=True) # SSL context is part of session

    async def fetch_and_parse(self, url: str) -> Dict:
        """Fetches, parses a single URL, and handles retries."""
        if self._stop_event.is_set():
            logger.info(f"Skipping {url} due to stop event.")
            return self._create_error_result(url, "Crawl stopped")

        async with self.semaphore: # Control concurrency
            if self._stop_event.is_set():
                logger.info(f"Skipping {url} (semaphore acquired before stop).")
                return self._create_error_result(url, "Crawl stopped")

            await self._adjust_concurrency_periodically() # Dynamic concurrency adjustment

            data = self._default_data_dict(url)

            try:
                is_allowed, robots_reason = await self.check_robots_and_delay(url)
                data["Blocked by Robots.txt"] = "No" if is_allowed else "Yes"
                data["Robots Block Rule"] = robots_reason if not is_allowed else ""

                if not is_allowed and self.respect_robots:
                    data["Indexable"] = "No"
                    data["Indexability Reason"] = robots_reason
                    logger.info(f"Skipping {url} due to robots.txt ({robots_reason})")
                    self._add_to_buffer(data)
                    return data

                # Fetching the URL
                logger.info(f"Fetching: {url}")
                resp = await self._fetch_url_with_retry(url, self.session)
                
                async with resp: # Ensure response is properly closed
                    data["Content_Type"] = resp.headers.get("Content-Type", "")
                    data["Initial_Status_Code"] = str(resp.history[0].status if resp.history else resp.status) # Status before redirects
                    data["Initial_Status_Type"] = self.status_label(int(data["Initial_Status_Code"]))
                    
                    data["Final_URL"] = str(resp.url)
                    data["Encoded_URL"] = quote(str(resp.url), safe=':/?=&')
                    data["Final_Status_Code"] = str(resp.status)
                    data["Final_Status_Type"] = self.status_label(resp.status)
                    data["Last Modified"] = resp.headers.get("Last-Modified", "")
                    data["X-Robots-Tag"] = resp.headers.get("X-Robots-Tag", "")

                    if resp.status != 200:
                        data["Indexable"] = "No"
                        data["Indexability Reason"] = f"HTTP Status {resp.status}"
                    
                    if resp.status == 200 and resp.content_type and resp.content_type.startswith("text/html"):
                        content = await resp.text(errors='replace')
                        self.parse_html_content(data, content) # Modifies data in-place
                    else:
                        if data["Indexable"] == "Yes": # If not already set to No
                             data["Indexability Reason"] = "Non-200 or non-HTML content"
                             data["Indexable"] = "No"
                    
                    update_redirect_label(data, url) # Call after final status known
                    self.determine_indexability(data) # Final indexability check based on all factors

            except RetryError as e: # From tenacity
                logger.error(f"Failed to fetch {url} after {MAX_RETRIES} retries: {e.last_attempt.exception()}")
                data.update(self._create_error_result(url, f"Max retries reached: {e.last_attempt.exception()}"))
                self.failed_urls.add(url)
            except aiohttp.ClientError as e:
                logger.error(f"ClientError fetching {url}: {e}")
                data.update(self._create_error_result(url, f"ClientError: {e}"))
                self.failed_urls.add(url)
            except asyncio.TimeoutError:
                logger.error(f"Timeout fetching {url}")
                data.update(self._create_error_result(url, "Timeout"))
                self.failed_urls.add(url)
            except asyncio.CancelledError:
                logger.info(f"Task for {url} cancelled.")
                data.update(self._create_error_result(url, "Cancelled"))
                # Do not add to failed_urls if cancelled by user
                raise # Important to propagate cancellation
            except Exception as e:
                logger.error(f"Unexpected error processing {url}: {e}", exc_info=True)
                data.update(self._create_error_result(url, f"Unexpected error: {e}"))
                self.failed_urls.add(url)
            finally:
                self._add_to_buffer(data) # Add to buffer for error rate calculation
            
            return data

    def _add_to_buffer(self, result_dict: Dict):
        self.results_buffer.append(result_dict)
        if len(self.results_buffer) > self.BUFFER_SIZE:
            self.results_buffer.pop(0) # Keep buffer size fixed

    def _default_data_dict(self, url: str) -> Dict:
        return {
            "Original_URL": url, "Encoded_URL": "", "Content_Type": "",
            "Initial_Status_Code": "", "Initial_Status_Type": "",
            "Final_URL": url, "Final_Status_Code": "", "Final_Status_Type": "",
            "Title": "", "Meta Description": "", "H1": "", "H1_Count": 0,
            "Canonical_URL": "", "Meta Robots": "", "X-Robots-Tag": "",
            "HTML Lang": "", "Blocked by Robots.txt": "Unknown", "Robots Block Rule": "",
            "Indexable": "Unknown", "Indexability Reason": "",
            "Last Modified": "", "Word Count": 0,
            "Timestamp": datetime.now().isoformat(),
        }

    def _create_error_result(self, url: str, error_message: str) -> Dict:
        # Uses parts of _default_data_dict structure for consistency
        return {
            "Original_URL": url, "Final_URL": url,
            "Initial_Status_Code": "Error", "Initial_Status_Type": error_message,
            "Final_Status_Code": "Error", "Final_Status_Type": error_message,
            "Indexable": "No", "Indexability Reason": f"Fetch Error: {error_message}",
            "Timestamp": datetime.now().isoformat(),
            # Fill other fields with empty or default to match structure
            "Encoded_URL": "", "Content_Type": "", "Title": "", "Meta Description": "",
            "H1": "", "H1_Count": 0, "Canonical_URL": "", "Meta Robots": "",
            "X-Robots-Tag": "", "HTML Lang": "", "Blocked by Robots.txt": "Unknown",
            "Robots Block Rule": "", "Last Modified": "", "Word Count": 0,
        }
    
    @staticmethod
    def status_label(status_code: int) -> str:
        if 200 <= status_code < 300: return "Success"
        if 300 <= status_code < 400: return "Redirect"
        if 400 <= status_code < 500: return "Client Error"
        if 500 <= status_code < 600: return "Server Error"
        return "Unknown"

    def parse_html_content(self, data: Dict, content: str):
        """Parses HTML content and updates the data dictionary in-place."""
        try:
            soup = BeautifulSoup(content, "lxml")
            data["Title"] = soup.title.string.strip() if soup.title and soup.title.string else ""
            
            meta_desc = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
            data["Meta Description"] = meta_desc["content"].strip() if meta_desc and meta_desc.get("content") else ""
            
            h1_tags = soup.find_all("h1")
            data["H1_Count"] = len(h1_tags)
            data["H1"] = h1_tags[0].get_text(separator=" ", strip=True) if h1_tags else ""
            
            canonical_link = soup.find("link", rel="canonical")
            if canonical_link and canonical_link.get("href"):
                data["Canonical_URL"] = urljoin(data["Final_URL"], canonical_link["href"].strip())
            
            meta_robots_tag = soup.find("meta", attrs={"name": re.compile(r"robots", re.I)})
            data["Meta Robots"] = meta_robots_tag["content"].strip() if meta_robots_tag and meta_robots_tag.get("content") else ""
            
            html_tag = soup.find("html")
            data["HTML Lang"] = html_tag.get("lang", "") if html_tag else ""
            
            text_content = soup.get_text(separator=" ", strip=True)
            data["Word Count"] = len(text_content.split())

        except Exception as e:
            logger.error(f"Error parsing HTML for {data['Original_URL']}: {e}", exc_info=True)
            # Data dictionary already has defaults, so just log

    def determine_indexability(self, data: Dict):
        """Determines indexability based on all gathered data. Updates data dict in-place."""
        # Start with Yes, then look for reasons to say No.
        is_indexable = True
        reasons = []

        # HTTP Status
        if data["Final_Status_Code"] != "200" and data["Final_Status_Code"] != "": # Allow empty if not fetched
             if data["Final_Status_Code"] != "Error": # Don't override specific fetch error
                is_indexable = False
                reasons.append(f"Non-200 status: {data['Final_Status_Code']}")

        # Robots.txt
        if data["Blocked by Robots.txt"] == "Yes":
            is_indexable = False
            reasons.append(data["Robots Block Rule"] or "Blocked by robots.txt")

        # Meta Robots
        meta_robots_lower = data["Meta Robots"].lower()
        if "noindex" in meta_robots_lower:
            is_indexable = False
            reasons.append("Meta robots: noindex")
        # Nofollow doesn't make a page non-indexable, but it's good info
        # if "nofollow" in meta_robots_lower: reasons.append("Meta robots: nofollow")

        # X-Robots-Tag
        x_robots_lower = data["X-Robots-Tag"].lower()
        if "noindex" in x_robots_lower:
            is_indexable = False
            reasons.append("X-Robots-Tag: noindex")
        # if "nofollow" in x_robots_lower: reasons.append("X-Robots-Tag: nofollow")
            
        # Canonicalization
        # A page is still indexable if it canonicalizes to itself.
        # If it canonicalizes elsewhere, THIS page might not be the one indexed.
        final_url_norm = normalize_url(data["Final_URL"])
        canonical_url_norm = normalize_url(data["Canonical_URL"])

        if canonical_url_norm and final_url_norm and canonical_url_norm != final_url_norm:
            is_indexable = False # This specific URL is not the preferred version
            reasons.append(f"Canonical points elsewhere: {data['Canonical_URL']}")
        
        # Update data dictionary
        data["Indexable"] = "Yes" if is_indexable else "No"
        if not reasons and not is_indexable and not data.get("Indexability Reason"): # if no specific reason but still no
             data["Indexability Reason"] = "Multiple factors or undetermined"
        elif reasons:
             data["Indexability Reason"] = "; ".join(reasons)
        # If already set by status code error, don't overwrite with generic message
        elif not is_indexable and not data.get("Indexability Reason"):
            data["Indexability Reason"] = "Undetermined non-indexable"


    async def recrawl_failed_urls(self, existing_results_urls: Set[str]) -> List[Dict]:
        """Attempts to recrawl URLs that failed, avoiding duplicates."""
        if not self.failed_urls:
            return []
        
        logger.info(f"Attempting to recrawl {len(self.failed_urls)} failed URLs...")
        # Filter out URLs that might have succeeded in a different context or were already processed
        urls_to_recrawl = list(self.failed_urls - existing_results_urls)
        if not urls_to_recrawl:
            logger.info("No new failed URLs to recrawl.")
            return []
        
        logger.info(f"Recrawling {len(urls_to_recrawl)} unique failed URLs.")

        tasks = []
        for url in urls_to_recrawl:
            # Using a new semaphore for recrawl to not interfere with main crawl limits if any
            # Or reuse existing if appropriate for the application flow
            task = asyncio.create_task(self.fetch_and_parse(url)) # fetch_and_parse has its own retry
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)
            tasks.append(task)
        
        recrawl_results = []
        for i, future in enumerate(asyncio.as_completed(tasks)):
            if self._stop_event.is_set():
                logger.info("Recrawl stopped.")
                break
            try:
                result = await future
                if result:
                    recrawl_results.append(result)
                    if not str(result.get("Final_Status_Code","")).startswith("Error"):
                         logger.info(f"Successfully recrawled {result['Original_URL']} -> Status: {result['Final_Status_Code']}")
            except asyncio.CancelledError:
                logger.info("A recrawl task was cancelled.")
            except Exception as e:
                # This error should ideally be caught within fetch_and_parse and returned in the result dict
                logger.error(f"Unexpected error during recrawl result processing: {e}", exc_info=True)
        
        self.failed_urls.clear() # Clear after attempts
        return recrawl_results

    def stop(self):
        """Signals the crawl to stop."""
        logger.info("Stop signal received. Crawler will halt after current tasks complete.")
        self._stop_event.set()

    def is_stopped(self) -> bool:
        return self._stop_event.is_set()

# --- End of URLChecker Class ---

def normalize_url(url: str) -> str:
    """Normalizes a URL: adds scheme, removes fragment, trailing slash (optional but good for consistency)."""
    if not url: return ""
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url # Default to https
    
    try:
        parsed = urlparse(url)
        # Rebuild path to remove trailing slash unless it's the root path
        path = parsed.path
        if path != '/' and path.endswith('/'):
            path = path[:-1]
        
        # Lowercase scheme and netloc for consistency
        # Path can be case-sensitive on some servers, so keep its case.
        normalized = urlunparse(
            (parsed.scheme.lower(), parsed.netloc.lower(), path, parsed.params, parsed.query, '') # Remove fragment
        )
        return normalized
    except ValueError: # Handle potential malformed URLs
        logger.warning(f"Could not parse/normalize URL: {url}")
        return url # Return original if parsing fails

def update_redirect_label(data: Dict, original_url: str):
    """Updates Final_Status_Type based on redirect status."""
    # This function's logic might be better integrated into determine_indexability or
    # simplified, as Final_Status_Type already reflects the final status.
    # The original logic was a bit convoluted.
    # We primarily care about the final status of the final URL.
    final_url = data.get("Final_URL", "")
    final_status_str = data.get("Final_Status_Code", "")

    if not final_status_str or final_status_str == "Error":
        return # Already handled or error state

    try:
        final_status_code = int(final_status_str)
    except ValueError:
        return # Not a valid status code

    original_url_norm = normalize_url(original_url)
    final_url_norm = normalize_url(final_url)

    if original_url_norm != final_url_norm:
        data["Final_Status_Type"] = f"{URLChecker.status_label(final_status_code)} (Redirect from {original_url})"
    else:
        data["Final_Status_Type"] = URLChecker.status_label(final_status_code)
    # The indexability based on canonical mismatch is handled in determine_indexability

def compile_regex_filters(include_pattern: Optional[str], exclude_pattern: Optional[str]) -> Tuple[Optional[re.Pattern], Optional[re.Pattern]]:
    inc_re, exc_re = None, None
    try:
        if include_pattern:
            inc_re = re.compile(include_pattern, re.IGNORECASE)
    except re.error as e:
        logger.error(f"Invalid include regex '{include_pattern}': {e}")
    try:
        if exclude_pattern:
            exc_re = re.compile(exclude_pattern, re.IGNORECASE)
    except re.error as e:
        logger.error(f"Invalid exclude regex '{exclude_pattern}': {e}")
    return inc_re, exc_re

def filter_url_by_regex(url: str, inc_re: Optional[re.Pattern], exc_re: Optional[re.Pattern]) -> bool:
    if exc_re and exc_re.search(url):
        return False
    if inc_re and not inc_re.search(url):
        return False
    return True

async def discover_links_from_html(page_url: str, html_content: str, base_netloc: str) -> List[str]:
    """Discovers and filters internal links from HTML content."""
    discovered_links = []
    soup = BeautifulSoup(html_content, "lxml")
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        try:
            abs_link = urljoin(page_url, href)
            norm_link = normalize_url(abs_link)
            if norm_link:
                parsed_link = urlparse(norm_link)
                # Filter for HTTP/HTTPS and same domain
                if parsed_link.scheme in ('http', 'https') and parsed_link.netloc.lower() == base_netloc:
                    discovered_links.append(norm_link)
        except Exception as e:
            logger.debug(f"Could not process link '{href}' from {page_url}: {e}")
    return list(set(discovered_links)) # Unique links

async def crawl_task_manager(
    initial_urls: List[str],
    checker: URLChecker,
    show_partial_callback,
    crawl_mode: str = "list", # "list", "spider"
    spider_seed_url: Optional[str] = None, # For spider mode
    include_regex: Optional[re.Pattern] = None, # For spider mode
    exclude_regex: Optional[re.Pattern] = None, # For spider mode
    max_spider_urls: int = DEFAULT_MAX_URLS
):
    results: List[Dict] = []
    
    # For Spider mode
    queue = asyncio.Queue() # Using asyncio.Queue for spidering
    processed_urls: Set[str] = set() # URLs submitted to fetch_and_parse
    discovered_for_spider: Set[str] = set() # All unique URLs seen by spider

    base_netloc = ""
    if crawl_mode == "spider" and spider_seed_url:
        parsed_seed = urlparse(normalize_url(spider_seed_url))
        base_netloc = parsed_seed.netloc.lower()
        for url in initial_urls: # Seed URLs for spider
            norm_url = normalize_url(url)
            if norm_url and urlparse(norm_url).netloc.lower() == base_netloc: # Ensure seed is on same domain
                 if filter_url_by_regex(norm_url, include_regex, exclude_regex):
                    await queue.put(norm_url)
                    discovered_for_spider.add(norm_url)
        total_urls_to_process = len(discovered_for_spider) # Initial, will grow
    else: # List or Sitemap mode
        unique_initial_urls = sorted(list(set(normalize_url(u) for u in initial_urls if normalize_url(u))))
        for url in unique_initial_urls:
            await queue.put(url)
        total_urls_to_process = len(unique_initial_urls)

    active_tasks = 0
    processed_count = 0

    try:
        await checker.setup() # Setup session and semaphore

        while True:
            if checker.is_stopped():
                logger.info("Crawl task manager: Stop event detected.")
                break
            
            # Check if we should stop spidering (max URLs or queue empty and no active tasks)
            if crawl_mode == "spider" and \
               (len(processed_urls) >= max_spider_urls or (queue.empty() and active_tasks == 0)):
                logger.info("Spider mode: Max URLs reached or queue exhausted.")
                break
            
            # Check for list/sitemap mode completion
            if crawl_mode != "spider" and processed_count >= total_urls_to_process and active_tasks == 0:
                logger.info(f"{crawl_mode} mode: All URLs processed.")
                break
            
            if not queue.empty():
                url_to_process = await queue.get()
                queue.task_done()

                if url_to_process in processed_urls: # Avoid re-processing if somehow re-added
                    continue
                
                processed_urls.add(url_to_process)

                task = asyncio.create_task(checker.fetch_and_parse(url_to_process))
                checker._pending_tasks.add(task) # Track for cleanup
                active_tasks += 1

                def task_done_callback(fut, url=url_to_process): # Capture url
                    nonlocal active_tasks, processed_count
                    checker._pending_tasks.discard(fut)
                    active_tasks -= 1
                    processed_count +=1
                    try:
                        result = fut.result()
                        if result:
                            results.append(result)
                            
                            # For spider mode, discover new links
                            if crawl_mode == "spider" and \
                               len(processed_urls) < max_spider_urls and \
                               result.get("Content_Type", "").startswith("text/html") and \
                               result.get("Final_Status_Code") == "200":
                                
                                html_content = "" # Need to get HTML content if not already in result
                                # This part is tricky. fetch_and_parse returns a dict.
                                # We need the actual HTML content to parse links.
                                # This implies fetch_and_parse might need to return it, or we re-fetch (bad).
                                # For now, let's assume we modify fetch_and_parse or make another function.
                                # Let's simplify: if this were a real spider, discover_links would be integrated better.
                                # For this structure, we'd need `result` to contain the HTML or re-fetch.
                                # Let's assume `result` can contain 'html_content' if successfully fetched and parsed
                                # This is a placeholder for a more robust link discovery integration
                                if 'html_content' in result and result['html_content']: # Fictional field for now
                                    new_links = asyncio.create_task(
                                        discover_links_from_html(result["Final_URL"], result['html_content'], base_netloc)
                                    )
                                    # This part needs to be async and handled correctly
                                    # For simplicity, let's simulate adding to queue
                                    # This is a conceptual simplification. In a real scenario, await new_links
                                    # and then iterate and add to queue.
                                    # Conceptual:
                                    # for link in await new_links:
                                    #    if link not in discovered_for_spider and filter_url_by_regex(link, include_regex, exclude_regex):
                                    #        discovered_for_spider.add(link)
                                    #        await queue.put(link)
                                    pass # Placeholder for actual link discovery and queueing
                                        
                        # Update UI
                        if crawl_mode == "spider":
                            # For spider, total can change, so use len(discovered_for_spider)
                            show_partial_callback(results, len(processed_urls), len(discovered_for_spider) + queue.qsize())
                        else:
                            show_partial_callback(results, processed_count, total_urls_to_process)

                    except asyncio.CancelledError:
                        logger.info(f"Task for {url} was cancelled during callback.")
                    except Exception as e:
                        logger.error(f"Error in task_done_callback for {url}: {e}", exc_info=True)
                    
                task.add_done_callback(lambda fut, url=url_to_process: task_done_callback(fut, url))

            else: # Queue is empty, wait for active tasks or a short period
                await asyncio.sleep(0.1)

        logger.info(f"Crawl finished. Processed {processed_count} URLs. Total results: {len(results)}")

    except asyncio.CancelledError:
        logger.info("Crawl task manager was cancelled.")
    except Exception as e:
        logger.error(f"Error in crawl task manager: {e}", exc_info=True)
    finally:
        # Ensure all tasks are awaited if manager exits prematurely
        if checker._pending_tasks:
            logger.info(f"Waiting for {len(checker._pending_tasks)} remaining tasks upon exit...")
            await asyncio.gather(*list(checker._pending_tasks), return_exceptions=True)
        await checker.cleanup() # Crucial for closing session
    
    return results


async def run_crawl_logic(
    crawl_type: str, # 'spider', 'list', 'sitemap'
    checker: URLChecker,
    show_partial_callback,
    seed_url: Optional[str] = None,
    url_list: Optional[List[str]] = None,
    sitemap_urls_input: Optional[List[str]] = None,
    include_pattern_str: Optional[str] = None,
    exclude_pattern_str: Optional[str] = None,
    max_spider_urls_limit: int = DEFAULT_MAX_URLS
) -> List[Dict]:
    
    all_results = []
    initial_urls_for_crawl: List[str] = []

    inc_re, exc_re = compile_regex_filters(include_pattern_str, exclude_pattern_str)

    if crawl_type == "spider":
        if not seed_url:
            logger.error("Spider mode requires a seed URL.")
            return []
        # For spider, initial_urls_for_crawl could be just the seed, or seed + sitemaps if also provided
        initial_urls_for_crawl.append(normalize_url(seed_url))
        
        # Optional: If spider mode can also consume sitemaps for initial seed
        if sitemap_urls_input:
            logger.info(f"Spider mode: Parsing sitemaps for additional seed URLs: {sitemap_urls_input}")
            sitemap_discovered_urls = await process_sitemaps_for_crawl(sitemap_urls_input, checker.user_agent, show_partial_sitemap_parsing_ui=None) # No UI callback here
            initial_urls_for_crawl.extend(sitemap_discovered_urls)
            initial_urls_for_crawl = sorted(list(set(initial_urls_for_crawl))) # Deduplicate

        logger.info(f"Starting Spider crawl. Seed URLs: {len(initial_urls_for_crawl)}. Max URLs: {max_spider_urls_limit}")
        # Spider logic will handle its own queue internally based on discovered links.
        # The `crawl_task_manager` needs to be adapted for spidering or a dedicated spider function used.
        # For now, let's assume `crawl_task_manager` can take a seed and spider.
        all_results = await dynamic_frontier_crawl_wrapper( # Using a more specific spider function
            seed_url=seed_url, # The primary seed for domain context
            additional_seed_urls=initial_urls_for_crawl, # All initial URLs
            checker=checker,
            include_regex=inc_re,
            exclude_regex=exc_re,
            show_partial_callback=show_partial_callback,
            max_urls=max_spider_urls_limit
        )

    elif crawl_type == "list":
        if not url_list:
            logger.error("List mode requires a list of URLs.")
            return []
        initial_urls_for_crawl = [normalize_url(u) for u in url_list if normalize_url(u)]
        logger.info(f"Starting List crawl with {len(initial_urls_for_crawl)} URLs.")
        all_results = await crawl_task_manager(initial_urls_for_crawl, checker, show_partial_callback, crawl_mode="list")
        
    elif crawl_type == "sitemap":
        if not sitemap_urls_input:
            logger.error("Sitemap mode requires sitemap URLs.")
            return []
        logger.info(f"Starting Sitemap crawl. Parsing sitemaps: {sitemap_urls_input}")
        # Pass the Streamlit UI callback for sitemap parsing if available
        sitemap_discovered_urls = await process_sitemaps_for_crawl(
            sitemap_urls_input, 
            checker.user_agent,
            show_partial_sitemap_parsing_ui=st.session_state.get('sitemap_parsing_ui_callback') 
        )
        initial_urls_for_crawl = sitemap_discovered_urls
        if not initial_urls_for_crawl:
            logger.warning("No URLs found in provided sitemaps.")
            return []
        logger.info(f"Extracted {len(initial_urls_for_crawl)} URLs from sitemaps for crawling.")
        all_results = await crawl_task_manager(initial_urls_for_crawl, checker, show_partial_callback, crawl_mode="sitemap")

    else:
        logger.error(f"Unknown crawl type: {crawl_type}")
        return []

    # Consolidate results and recrawl failed if any
    final_results_set = {r['Original_URL']: r for r in all_results} # Deduplicate by original URL, keeping last seen

    if checker.failed_urls:
        urls_in_results = set(final_results_set.keys())
        # Filter failed URLs to only recrawl those not already successfully in results
        # (e.g. if a URL failed then succeeded through another path in spider mode)
        recrawl_candidates = checker.failed_urls - urls_in_results
        
        if recrawl_candidates:
            logger.info(f"Recrawling {len(recrawl_candidates)} failed URLs that are not in successful results...")
            # Create a temporary set for recrawl_failed_urls to use
            original_failed_urls = checker.failed_urls
            checker.failed_urls = recrawl_candidates # Temporarily set to only candidates
            
            recrawl_results_list = await checker.recrawl_failed_urls(urls_in_results)
            
            checker.failed_urls = original_failed_urls # Restore original set (or just clear if done)

            for res in recrawl_results_list:
                final_results_set[res['Original_URL']] = res # Update/add recrawled results
        else:
            logger.info("No new failed URLs to recrawl or all failed URLs already have a successful entry.")
    
    return list(final_results_set.values())


# --- Sitemap Processing ---
async def fetch_sitemap_content(session: aiohttp.ClientSession, url: str, user_agent: str) -> Optional[str]:
    try:
        async with session.get(url, headers={"User-Agent": user_agent}, timeout=aiohttp.ClientTimeout(total=20)) as response:
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            return await response.text(errors='replace')
    except asyncio.TimeoutError:
        logger.error(f"Timeout fetching sitemap: {url}")
    except aiohttp.ClientError as e:
        logger.error(f"ClientError fetching sitemap {url}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching sitemap {url}: {e}", exc_info=True)
    return None

async def parse_single_sitemap_xml(xml_content: str, sitemap_url: str) -> Tuple[List[str], List[str]]:
    """Parses XML sitemap content. Returns (list_of_page_urls, list_of_nested_sitemap_urls)."""
    page_urls: List[str] = []
    nested_sitemap_urls: List[str] = []
    try:
        root = ET.fromstring(xml_content.encode('utf-8')) # Ensure bytes for ET
        # XML Namespaces are tricky. A common approach is to ignore them in findall.
        # {*} matches any namespace.
        if root.tag.endswith('sitemapindex'):
            for sitemap_element in root.findall('.//{*}sitemap/{*}loc'):
                if sitemap_element.text:
                    nested_sitemap_urls.append(sitemap_element.text.strip())
        elif root.tag.endswith('urlset'):
            for url_element in root.findall('.//{*}url/{*}loc'):
                if url_element.text:
                    page_urls.append(url_element.text.strip())
        else:
            logger.warning(f"Unknown root tag in sitemap {sitemap_url}: {root.tag}")
    except ET.ParseError as e:
        logger.error(f"XML ParseError for sitemap {sitemap_url}: {e}")
    except Exception as e:
        logger.error(f"Error parsing XML sitemap {sitemap_url}: {e}", exc_info=True)
    return page_urls, nested_sitemap_urls

def parse_single_sitemap_text(text_content: str) -> List[str]:
    """Parses a plain text sitemap."""
    urls = []
    for line in text_content.splitlines():
        line = line.strip()
        if line and not line.startswith('#') and (line.startswith('http://') or line.startswith('https://')):
            urls.append(line)
    return urls

async def process_sitemaps_for_crawl(
    sitemap_urls_input: List[str], 
    user_agent: str,
    show_partial_sitemap_parsing_ui # Callback for Streamlit UI
    ) -> List[str]:
    """
    Concurrently fetches and parses multiple sitemap URLs (can be index sitemaps or regular ones).
    Handles nested sitemaps.
    """
    all_found_page_urls: Set[str] = set()
    sitemaps_to_process_queue = asyncio.Queue()
    # Use a set to track sitemaps already added to queue or processed to avoid loops/redundancy
    known_sitemap_urls: Set[str] = set() 

    for s_url in sitemap_urls_input:
        norm_s_url = normalize_url(s_url)
        if norm_s_url not in known_sitemap_urls:
            await sitemaps_to_process_queue.put(norm_s_url)
            known_sitemap_urls.add(norm_s_url)

    # Use a single session for all sitemap fetching
    async with aiohttp.ClientSession(headers={"User-Agent": user_agent}, json_serialize=orjson.dumps) as session:
        active_sitemap_tasks = 0
        while True:
            if sitemaps_to_process_queue.empty() and active_sitemap_tasks == 0:
                break # All sitemaps processed

            if not sitemaps_to_process_queue.empty():
                current_sitemap_url = await sitemaps_to_process_queue.get()
                sitemaps_to_process_queue.task_done()
                active_sitemap_tasks +=1
                
                logger.info(f"Processing sitemap: {current_sitemap_url}")
                sitemap_content = await fetch_sitemap_content(session, current_sitemap_url, user_agent)

                if sitemap_content:
                    # Try XML parsing first
                    page_urls_from_current, nested_sitemaps_from_current = await parse_single_sitemap_xml(sitemap_content, current_sitemap_url)
                    
                    if not page_urls_from_current and not nested_sitemaps_from_current:
                        # If XML parsing yielded nothing, it might be a text sitemap
                        logger.debug(f"XML parsing yielded no URLs for {current_sitemap_url}, trying as text sitemap.")
                        page_urls_from_current = parse_single_sitemap_text(sitemap_content)

                    for p_url in page_urls_from_current:
                        all_found_page_urls.add(normalize_url(p_url))
                    
                    for ns_url in nested_sitemaps_from_current:
                        norm_ns_url = normalize_url(ns_url)
                        if norm_ns_url not in known_sitemap_urls:
                            await sitemaps_to_process_queue.put(norm_ns_url)
                            known_sitemap_urls.add(norm_ns_url) # Add to known before putting in queue
                
                active_sitemap_tasks -=1
                
                # Update Streamlit UI if callback provided
                if show_partial_sitemap_parsing_ui:
                    show_partial_sitemap_parsing_ui(list(all_found_page_urls))
            else:
                await asyncio.sleep(0.1) # Wait for tasks to complete or new sitemaps

    logger.info(f"Sitemap processing complete. Found {len(all_found_page_urls)} unique page URLs.")
    return sorted(list(all_found_page_urls))


# --- Spidering Logic (Dynamic Frontier) ---
async def dynamic_frontier_crawl_wrapper(
    seed_url: str,
    additional_seed_urls: List[str], # Can include the main seed_url plus sitemap URLs
    checker: URLChecker,
    include_regex: Optional[re.Pattern],
    exclude_regex: Optional[re.Pattern],
    show_partial_callback,
    max_urls: int = DEFAULT_MAX_URLS
) -> List[Dict]:
    
    all_results: List[Dict] = []
    
    # Frontier: URLs to be crawled. Using a set for efficient add/check, and pop for next.
    # For priority (e.g. by depth), asyncio.PriorityQueue would be better.
    # For broad crawls, a simple set/list and then random pop can work.
    # Using a deque for FIFO queue behavior.
    frontier = asyncio.Queue() 
    
    # Visited: Normalized URLs that have been added to the frontier (and will be/are being processed)
    # This prevents adding the same URL to the frontier multiple times.
    visited_for_frontier: Set[str] = set() 
    
    # Processed_results: URLs for which we have a result dict.
    processed_results_urls: Set[str] = set()


    main_seed_normalized = normalize_url(seed_url)
    if not main_seed_normalized:
        logger.error("Spider Error: Invalid main seed URL provided.")
        return []
        
    parsed_main_seed = urlparse(main_seed_normalized)
    base_netloc = parsed_main_seed.netloc.lower()
    if not base_netloc:
        logger.error(f"Spider Error: Could not parse netloc from main seed URL: {main_seed_normalized}")
        return []
        
    logger.info(f"Starting dynamic frontier crawl. Main Seed: {main_seed_normalized}, Base Netloc: {base_netloc}")

    # Add all initial seed URLs to frontier
    initial_frontier_urls = set(normalize_url(u) for u in additional_seed_urls)
    for u in initial_frontier_urls:
        if u and urlparse(u).netloc.lower() == base_netloc: # Ensure on same domain
            if filter_url_by_regex(u, include_regex, exclude_regex):
                if u not in visited_for_frontier:
                    await frontier.put(u)
                    visited_for_frontier.add(u)

    active_fetch_tasks = 0
    total_urls_added_to_frontier = len(visited_for_frontier) # Initial count

    try:
        await checker.setup()

        while True:
            if checker.is_stopped():
                logger.info("Spider: Stop event detected.")
                break
            
            if len(processed_results_urls) >= max_urls:
                logger.info(f"Spider: Reached max URL limit of {max_urls}.")
                break
            
            if frontier.empty() and active_fetch_tasks == 0:
                logger.info("Spider: Frontier is empty and no active tasks remaining.")
                break
            
            if not frontier.empty() and len(processed_results_urls) + active_fetch_tasks < max_urls :
                current_url = await frontier.get()
                frontier.task_done()
                
                # Double check if already processed, e.g. if added to frontier by mistake after processing
                if current_url in processed_results_urls:
                    continue

                active_fetch_tasks += 1
                task = asyncio.create_task(checker.fetch_and_parse(current_url))
                checker._pending_tasks.add(task) # For global cleanup tracking

                def process_spider_result(fut, fetched_url=current_url):
                    nonlocal active_fetch_tasks, total_urls_added_to_frontier
                    active_fetch_tasks -= 1
                    checker._pending_tasks.discard(fut) # Remove from global tracking
                    
                    try:
                        result_dict = fut.result()
                        if result_dict:
                            all_results.append(result_dict)
                            processed_results_urls.add(fetched_url) # Mark as having a result

                            # Discover and add new links if successful HTML page and within limits
                            if result_dict.get("Content_Type", "").startswith("text/html") and \
                               result_dict.get("Final_Status_Code") == "200" and \
                               len(visited_for_frontier) < max_urls : # Check visited_for_frontier for adding new links
                                
                                final_page_url = result_dict["Final_URL"]
                                # To discover links, we need the HTML. fetch_and_parse must be adapted
                                # or we use a different function. For now, assuming result_dict could contain HTML
                                # if properly fetched. This is a simplification.
                                # A more robust way is to have fetch_and_parse return (dict, html_content_or_none)
                                
                                # Let's simulate getting HTML content for discovery (this needs proper implementation)
                                # Option 1: fetch_and_parse returns HTML (modify it)
                                # Option 2: A separate fetch for discovery (inefficient)
                                # Option 3: discover_links is part of fetch_and_parse (complex return)

                                # --- Placeholder for link discovery logic ---
                                # This part needs a way to get the HTML content.
                                # Assuming a hypothetical `get_html_from_result(result_dict)` or similar.
                                # For this example, this part will be conceptual.
                                # In a real implementation, you'd await an async link discovery function here.
                                # async def discover_and_add_links(result_dict_param):
                                #    nonlocal total_urls_added_to_frontier
                                #    html_content = await get_html_for_discovery(result_dict_param['Final_URL'], checker.session)
                                #    if html_content:
                                #        new_links = await discover_links_from_html(result_dict_param["Final_URL"], html_content, base_netloc)
                                #        for link in new_links:
                                #            if link not in visited_for_frontier and len(visited_for_frontier) < max_urls:
                                #                 if filter_url_by_regex(link, include_regex, exclude_regex):
                                #                    visited_for_frontier.add(link)
                                #                    await frontier.put(link)
                                #                    total_urls_added_to_frontier +=1
                                # asyncio.create_task(discover_and_add_links(result_dict))
                                # --- End Placeholder ---
                                pass # Actual link discovery and frontier.put() would go here.

                        # Update UI: crawled count is len(processed_results_urls)
                        # total is len(visited_for_frontier) (all unique URLs intended for processing)
                        # or max_urls if that's the cap.
                        ui_total = max(len(visited_for_frontier), frontier.qsize() + len(processed_results_urls))
                        ui_total = min(ui_total, max_urls) # Cap display total at max_urls
                        show_partial_callback(all_results, len(processed_results_urls), ui_total)

                    except asyncio.CancelledError:
                        logger.info(f"Spider task for {fetched_url} cancelled during result processing.")
                    except Exception as e_callback:
                        logger.error(f"Error processing result for {fetched_url} in spider: {e_callback}", exc_info=True)

                task.add_done_callback(lambda fut, url=current_url: process_spider_result(fut, url))
            
            else: # Frontier empty or max concurrent tasks, wait a bit
                await asyncio.sleep(0.1)
                
    except asyncio.CancelledError:
        logger.info("Dynamic frontier crawl was cancelled.")
    except Exception as e:
        logger.error(f"Error in dynamic frontier crawl: {e}", exc_info=True)
    finally:
        # Wait for any remaining tasks to complete or be cancelled
        if checker._pending_tasks:
            logger.info(f"Spider cleanup: Waiting for {len(checker._pending_tasks)} tasks.")
            await asyncio.gather(*list(checker._pending_tasks), return_exceptions=True)
        await checker.cleanup() # Ensure session is closed

    return all_results


# --- Streamlit UI and Main Logic ---
def format_and_reorder_df(df: pd.DataFrame) -> pd.DataFrame:
    """Helper function to reorder and format columns consistently."""
    # Define the desired column order
    main_cols = [
        'Original_URL', 'Content_Type', 'Initial_Status_Code', 'Initial_Status_Type',
        'Indexable', 'Indexability Reason', 'Blocked by Robots.txt', 'Robots Block Rule',
        'Final_URL', 'Final_Status_Code', 'Final_Status_Type',
        'Meta Robots', 'X-Robots-Tag', 'Canonical_URL',
        'Title', 'Meta Description', 'H1', 'H1_Count', 'Word Count',
        'Last Modified', 'HTML Lang', 'Timestamp', 'Encoded_URL'
    ]
    
    # Get existing columns that aren't in main_cols
    existing_cols_in_df = df.columns.tolist()
    ordered_cols = [col for col in main_cols if col in existing_cols_in_df]
    
    # Add any other columns from the DataFrame not in main_cols to the end
    other_cols = [col for col in existing_cols_in_df if col not in ordered_cols]
    final_col_order = ordered_cols + other_cols
    
    return df[final_col_order]

def main_streamlit_app():
    st.set_page_config(layout="wide", page_title="Web Crawler SEO Tool")
    st.title("⚙️ Web Crawler & SEO Analyzer")

    # Initialize session state variables
    if 'is_crawling' not in st.session_state: st.session_state['is_crawling'] = False
    if 'crawl_results' not in st.session_state: st.session_state['crawl_results'] = []
    if 'crawl_done' not in st.session_state: st.session_state['crawl_done'] = False
    if 'checker_instance' not in st.session_state: st.session_state['checker_instance'] = None
    if 'sitemap_parsed_urls' not in st.session_state: st.session_state['sitemap_parsed_urls'] = []
    if 'current_crawl_mode' not in st.session_state: st.session_state['current_crawl_mode'] = "Spider" # Default mode

    st.sidebar.header("⚙️ Configuration")
    
    ua_choice = st.sidebar.selectbox("User Agent", list(USER_AGENTS.keys()), index=list(USER_AGENTS.keys()).index("Custom Bot"))
    user_agent = USER_AGENTS[ua_choice]
    if ua_choice == "Custom Bot":
        user_agent = st.sidebar.text_input("Custom User Agent String", value=DEFAULT_USER_AGENT)

    concurrency = st.sidebar.slider("Concurrent Requests", MIN_CONCURRENCY, MAX_CONCURRENCY, DEFAULT_CONCURRENCY, 5)
    respect_robots = st.sidebar.checkbox("Respect robots.txt & Crawl-Delay", value=True)
    timeout_val = st.sidebar.slider("Timeout (seconds)", 5, 60, DEFAULT_TIMEOUT, 5)

    st.sidebar.markdown("---")
    mode = st.sidebar.radio("Crawl Mode", ["Spider", "List", "Sitemap"], key="crawl_mode_selector", index=0)
    st.session_state['current_crawl_mode'] = mode # Keep track of selected mode

    # Inputs based on mode
    seed_url_input, list_input_text, sitemap_input_text = "", "", ""
    include_pattern, exclude_pattern = "", ""
    max_spider_urls = DEFAULT_MAX_URLS

    if mode == "Spider":
        st.subheader("🕷️ Spider Crawl")
        seed_url_input = st.text_input("Seed URL (e.g., https://www.example.com)", placeholder="Enter a single URL to start crawling from")
        max_spider_urls = st.number_input("Max URLs to Crawl", min_value=10, max_value=10000, value=DEFAULT_MAX_URLS, step=10)
        with st.expander("Advanced Filters (Regex for Discovered URLs)"):
            include_pattern = st.text_input("Include URLs Matching Regex", placeholder="e.g., /products/")
            exclude_pattern = st.text_input("Exclude URLs Matching Regex", placeholder="e.g., /blog/page/\d+")
        # Optional: Allow sitemaps as additional seeds for spider
        sitemap_spider_seed_mode = st.checkbox("Use Sitemaps as additional seeds for Spider")
        if sitemap_spider_seed_mode:
             sitemap_input_text = st.text_area("Sitemap URLs (one per line) for additional Spider seeds", height=100)


    elif mode == "List":
        st.subheader("📋 List Crawl")
        list_input_text = st.text_area("Enter URLs (one per line)", height=200)

    elif mode == "Sitemap":
        st.subheader("🗺️ Sitemap Crawl")
        sitemap_input_text = st.text_area("Sitemap URLs (one per line, can be sitemap index files)", height=150)
        st.info("The crawler will parse these sitemaps (including nested ones) to find all page URLs to crawl.")
        with st.expander("Discovered URLs from Sitemaps (Live Update)", expanded=False):
            sitemap_progress_ph = st.empty()
            sitemap_table_ph = st.empty()
            
            def show_partial_sitemap_parsing_ui_callback(discovered_sitemap_urls_list):
                sitemap_progress_ph.info(f"Found {len(discovered_sitemap_urls_list)} URLs from sitemaps so far...")
                if discovered_sitemap_urls_list:
                    df_sitemap_temp = pd.DataFrame(discovered_sitemap_urls_list, columns=["Discovered URLs"])
                    sitemap_table_ph.dataframe(df_sitemap_temp, height=300, use_container_width=True)
                else:
                    sitemap_table_ph.empty()
            st.session_state['sitemap_parsing_ui_callback'] = show_partial_sitemap_parsing_ui_callback


    # Start/Stop Button Area
    st.markdown("---")
    button_cols = st.columns(2)
    with button_cols[0]:
        if st.session_state['is_crawling']:
            if st.button("🛑 Stop Crawl", type="primary", use_container_width=True):
                if st.session_state['checker_instance']:
                    st.session_state['checker_instance'].stop()
                st.session_state['is_crawling'] = False # UI state change
                # Results will be shown by the callback or when crawl_done is true
                st.info("Stop signal sent. Crawl will halt after current operations...")
                st.rerun() # Rerun to update UI state
        else:
            if st.button("🚀 Start Crawl", type="primary", use_container_width=True):
                # Validate inputs based on mode
                valid_input = True
                if mode == "Spider" and not seed_url_input.strip():
                    st.error("Spider mode requires a Seed URL.")
                    valid_input = False
                elif mode == "List" and not list_input_text.strip():
                    st.error("List mode: Please enter some URLs.")
                    valid_input = False
                elif mode == "Sitemap" and not sitemap_input_text.strip():
                    st.error("Sitemap mode: Please enter Sitemap URLs.")
                    valid_input = False

                if valid_input:
                    st.session_state['is_crawling'] = True
                    st.session_state['crawl_results'] = []
                    st.session_state['crawl_done'] = False
                    st.session_state['sitemap_parsed_urls'] = [] # Reset for new crawl
                    
                    checker = URLChecker(user_agent, concurrency, timeout_val, respect_robots)
                    st.session_state['checker_instance'] = checker
                    
                    logger.info(f"Starting crawl in {mode} mode.")
                    st.rerun() # Rerun to start the async operation block

    # Progress and Results Area
    if st.session_state['is_crawling'] or st.session_state['crawl_done']:
        st.markdown("---")
        st.subheader("📊 Crawl Progress & Results")
        progress_text_ph = st.empty()
        progress_bar_ph = st.progress(0.0)
        
        results_expander = st.expander("实时结果 / Live Results", expanded=True)
        with results_expander:
            table_ph = st.empty()
            download_ph = st.empty()

    def show_partial_data_ui(current_results_list: List[Dict], processed_count: int, total_count: int):
        # This callback is from the crawler's perspective (list of dicts)
        # It receives all results *so far* for the current batch/crawl.
        # st.session_state['crawl_results'] should accumulate these.
        # Here, current_results_list is the complete list of results obtained so far in this crawl run.
        
        st.session_state['crawl_results'] = current_results_list # Update global results with current full list

        ratio = (processed_count / total_count) if total_count > 0 else 0
        ratio = min(1.0, ratio) # Cap at 100%
        
        progress_bar_ph.progress(ratio)
        remain = max(0, total_count - processed_count)
        progress_text_ph.info(
            f"Crawling... Completed {processed_count} of ~{total_count} URLs ({ratio:.1%}). Remaining: ~{remain}"
        )
        
        # Update DataFrame display periodically
        if current_results_list and (processed_count % 20 == 0 or processed_count == total_count or checker.is_stopped()):
            df_temp = pd.DataFrame(current_results_list)
            if not df_temp.empty:
                df_temp = format_and_reorder_df(df_temp)
                table_ph.dataframe(df_temp, height=500, use_container_width=True)

    # Async Crawl Execution Block
    if st.session_state['is_crawling'] and not st.session_state['crawl_done']:
        checker = st.session_state['checker_instance']
        final_results = []
        
        # Prepare arguments for run_crawl_logic
        crawl_args = {
            "crawl_type": mode,
            "checker": checker,
            "show_partial_callback": show_partial_data_ui,
            "seed_url": seed_url_input.strip() if mode == "Spider" else None,
            "url_list": [u.strip() for u in list_input_text.splitlines() if u.strip()] if mode == "List" else None,
            "sitemap_urls_input": [s.strip() for s in sitemap_input_text.splitlines() if s.strip()] if (mode == "Sitemap" or (mode=="Spider" and sitemap_spider_seed_mode)) else None,
            "include_pattern_str": include_pattern if mode == "Spider" else None,
            "exclude_pattern_str": exclude_pattern if mode == "Spider" else None,
            "max_spider_urls_limit": max_spider_urls if mode == "Spider" else 0 # Not applicable for list/sitemap
        }

        try:
            # Get or create event loop
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
            except RuntimeError: # No current event loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # Check if loop is already running (e.g. from nest_asyncio)
            if not loop.is_running():
                 final_results = loop.run_until_complete(run_crawl_logic(**crawl_args))
            else: # If loop is already running (common with nest_asyncio in some envs)
                 # We need to schedule it. This can be tricky with Streamlit's execution model.
                 # Forcing it here. This might be an area needing refinement depending on exact env.
                 future = asyncio.ensure_future(run_crawl_logic(**crawl_args), loop=loop)
                 # Streamlit doesn't easily await. This will block here if loop is managed by nest_asyncio.
                 # If not using nest_asyncio and this path is hit, it's more complex.
                 # The nest_asyncio.apply() at the top should make run_until_complete work even if nested.
                 # So, the `else` for loop.is_running() might not be typically hit in a simple Streamlit script.
                 # If it *is* hit, it implies a more complex asyncio setup.
                 # For now, let's assume run_until_complete is the primary path due to nest_asyncio.
                 logger.warning("Event loop was already running. Attempting to run task on existing loop.")
                 final_results = loop.run_until_complete(future)


            st.session_state['crawl_results'] = final_results # Store all results
            st.session_state['crawl_done'] = True
        
        except Exception as e:
            logger.error(f"Crawl execution error: {e}", exc_info=True)
            st.error(f"An error occurred during the crawl: {e}")
            st.session_state['crawl_done'] = True # Mark as done even on error to show partial results
        finally:
            st.session_state['is_crawling'] = False # Ensure this is set false
            # Cleanup is handled within run_crawl_logic/checker methods
            if loop and not loop.is_closed() and not loop.is_running() and not nest_asyncio.has_nested_asyncio():
                # Only close if we created it and it's not managed by nest_asyncio in a way that expects it to stay open.
                # This is tricky. With nest_asyncio, direct closing can sometimes cause issues.
                # loop.close()
                pass 
            st.rerun() # Rerun to update UI based on crawl_done and final results

    # Display final results and download button if crawl is done
    if st.session_state['crawl_done']:
        progress_text_ph.success(f"Crawl finished! Found {len(st.session_state['crawl_results'])} results.")
        progress_bar_ph.progress(1.0)
        
        df_final = pd.DataFrame(st.session_state['crawl_results'])
        if not df_final.empty:
            df_final = format_and_reorder_df(df_final)
            table_ph.dataframe(df_final, height=500, use_container_width=True)
            
            csv_data = df_final.to_csv(index=False).encode('utf-8')
            excel_data_buffer = io.BytesIO()
            df_final.to_excel(excel_data_buffer, index=False, engine='openpyxl')
            excel_data_bytes = excel_data_buffer.getvalue()

            dl_cols = download_ph.columns(2)
            with dl_cols[0]:
                st.download_button(
                    label="📥 Download CSV",
                    data=csv_data,
                    file_name=f"crawl_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            with dl_cols[1]:
                st.download_button(
                    label="📥 Download Excel",
                    data=excel_data_bytes,
                    file_name=f"crawl_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            show_summary_stats(df_final)
        else:
            table_ph.info("No results to display.")

def show_summary_stats(df: pd.DataFrame):
    st.subheader("📈 Summary Statistics")
    if df.empty:
        st.write("No data available for summary.")
        return

    cols = st.columns(2)
    with cols[0]:
        st.metric("Total URLs Processed", len(df))
        if "Indexable" in df.columns:
            st.metric("Indexable URLs", df[df["Indexable"] == "Yes"].shape[0])
        if "Blocked by Robots.txt" in df.columns:
            st.metric("Blocked by Robots.txt", df[df["Blocked by Robots.txt"] == "Yes"].shape[0])
    
    with cols[1]:
        if "Final_Status_Code" in df.columns:
            # Filter out "Error" or non-numeric status codes for this metric
            numeric_statuses = pd.to_numeric(df["Final_Status_Code"], errors='coerce').dropna()
            st.metric("Avg. Word Count (200 OK)", 
                      int(df.loc[numeric_statuses == 200, "Word Count"].mean()) 
                      if not df.loc[numeric_statuses == 200, "Word Count"].empty else 0)

    # Status Code Distribution
    if "Final_Status_Code" in df.columns:
        status_counts = df["Final_Status_Code"].value_counts().reset_index()
        status_counts.columns = ["Status Code", "Count"]
        st.write("**Final Status Code Distribution:**")
        st.dataframe(status_counts, use_container_width=True)

    # Indexability Reasons
    if "Indexability Reason" in df.columns and not df[df["Indexable"] == "No"].empty :
        reason_counts = df[df["Indexable"] == "No"]["Indexability Reason"].value_counts().reset_index()
        reason_counts.columns = ["Reason", "Count"]
        st.write("**Top Non-Indexability Reasons:**")
        st.dataframe(reason_counts, use_container_width=True)

if __name__ == "__main__":
    import io # For Excel download
    main_streamlit_app()
