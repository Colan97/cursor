import streamlit as st
import pandas as pd
import re
import asyncio
import aiohttp
import orjson # Using orjson for faster JSON operations
import nest_asyncio
import logging
# import pyperclip # For copy button, consider removing if st_copy is sufficient
import json
from typing import List, Dict, Set, Optional, Tuple
from urllib.parse import urlparse, urljoin, urlunparse, quote
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
import xml.etree.ElementTree as ET
import os
# from pathlib import Path # Not strictly used directly now
# from st_copy import copy_button # Keep if used, otherwise remove
import time # Used for sleep
from urllib import robotparser
# import concurrent.futures # Not directly used in async, consider if needed for CPU-bound tasks
# import requests # Replaced by aiohttp
import platform
# import psutil # Not used
# import numpy as np # Not used
# from scipy import stats # Not used
# from tqdm import tqdm # Replaced by Rich progress
# import colorama # Rich handles colors, this might be redundant
# from colorama import Fore, Style # Rich handles colors
from rich.console import Console
# from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn # Not used directly in ST
# from rich.table import Table # Not used directly in ST
# from rich.panel import Panel # Not used directly in ST
from rich.logging import RichHandler
# from typing_extensions import TypedDict # Standard typing is usually enough
# from dataclasses import dataclass
# from functools import lru_cache
# import hashlib # Not explicitly used, but good for caching if needed
import ssl
import certifi
# from urllib3.util.retry import Retry # aiohttp has its own retry mechanisms or use tenacity
# from urllib3.util import ssl_ # Using standard ssl module
import io # For Excel download

# Apply nest_asyncio to allow nested event loops (useful in Jupyter, less so for Streamlit usually)
nest_asyncio.apply()

# Set up event loop policy for Windows
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Constants
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_URLS = 1000 # Max URLs for spider mode
# SAVE_INTERVAL = 100 # Save results to file every N URLs (for CLI, not directly used in Streamlit results)
MAX_RETRIES = 3 # For fetching individual URLs
INITIAL_BACKOFF = 1
MAX_BACKOFF = 60
DEFAULT_USER_AGENT = "CustomSEOBot/1.2 (+http://www.example.com/bot.html)"
ERROR_THRESHOLD = 0.15 # For dynamic concurrency adjustment
MIN_CONCURRENCY = 5
MAX_CONCURRENCY = 50 # Reduced max slightly for typical shared environments
DEFAULT_CONCURRENCY = 10 # Reduced default slightly
# BATCH_SIZE = 100 # For processing URLs in chunks
# RETRY_DELAYS = [1, 2, 4] # For recrawling failed URLs (tenacity handles this now)
ROBOTS_CACHE_EXPIRY_SECONDS = 3600 # 1 hour for robots.txt cache

# Configure logging with Rich
# Ensure console for RichHandler is stderr for Streamlit Cloud compatibility
# Streamlit's own logging might capture stdout, so stderr for app logs is safer.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", # Added timestamp and level to format
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, console=Console(stderr=True), markup=True)]
)
logger = logging.getLogger("rich")


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
    """Save results to a JSON file (mostly for debugging/CLI)."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"Results saved to {filename}")
    except IOError as e:
        logger.error(f"Error saving results to {filename}: {e}")

def calculate_error_rate(results_buffer: List[Dict]) -> float:
    """Calculate the error rate from a buffer of recent results."""
    if not results_buffer:
        return 0.0
    error_count = sum(1 for r in results_buffer if str(r.get("Final_Status_Code", "")).startswith(("4", "5")) or r.get("Final_Status_Code") == "Error")
    return error_count / len(results_buffer)

def adjust_concurrency(current_concurrency: int, error_rate: float) -> int:
    """Dynamically adjust concurrency based on error rate."""
    if error_rate > ERROR_THRESHOLD:
        new_concurrency = max(MIN_CONCURRENCY, current_concurrency - 5)
        if new_concurrency != current_concurrency:
            logger.info(f"High error rate ({error_rate:.2%}). Decreasing concurrency: {current_concurrency} -> {new_concurrency}")
        return new_concurrency
    elif error_rate < ERROR_THRESHOLD / 2 and current_concurrency < MAX_CONCURRENCY :
        new_concurrency = min(MAX_CONCURRENCY, current_concurrency + 2)
        if new_concurrency != current_concurrency:
            logger.info(f"Low error rate ({error_rate:.2%}). Increasing concurrency: {current_concurrency} -> {new_concurrency}")
        return new_concurrency
    return current_concurrency

class URLChecker:
    def __init__(self, user_agent: str, concurrency: int, timeout: int, respect_robots: bool):
        self.user_agent = user_agent
        self.initial_concurrency = concurrency
        self.current_concurrency = concurrency
        self.timeout = timeout
        self.respect_robots = respect_robots
        
        self._robot_parsers_cache: Dict[str, Tuple[robotparser.RobotFileParser, datetime]] = {}
        self._ssl_context = ssl.create_default_context(cafile=certifi.where())
        
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore: Optional[asyncio.Semaphore] = None
        
        self.failed_urls: Set[str] = set()
        self.results_buffer: List[Dict] = []
        self.BUFFER_SIZE = 50

        # self.save_filename = f"crawl_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json" # For CLI mode
        
        self._robots_fetch_semaphore = asyncio.Semaphore(5) # Reduced for shared environments
        self._stop_event = asyncio.Event()
        
        self._last_adjustment_time = datetime.now()
        self.ADJUSTMENT_INTERVAL_SECONDS = 20 # Check error rate

        self._pending_tasks: Set[asyncio.Task] = set()
        self._last_crawl_time_per_domain: Dict[str, datetime] = {}

    async def setup(self):
        try:
            connector = aiohttp.TCPConnector(
                limit_per_host=0, 
                ssl=self._ssl_context,
                enable_cleanup_closed=True,
                force_close=False, 
                ttl_dns_cache=300
            )
            timeout_settings = aiohttp.ClientTimeout(
                total=self.timeout + 10, # Total timeout slightly larger
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
            logger.info(f"URLChecker initialized. Concurrency: {self.current_concurrency}, User-Agent: '{self.user_agent}'")
        except Exception as e:
            logger.error(f"Error setting up URLChecker: {e}", exc_info=True)
            raise

    async def cleanup(self):
        logger.info("Initiating URLChecker cleanup...")
        self._stop_event.set()
        
        if self._pending_tasks:
            logger.info(f"Cancelling {len(self._pending_tasks)} pending tasks...")
            for task in list(self._pending_tasks):
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)
            self._pending_tasks.clear()
            logger.info("Pending tasks processed.")
        
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
            logger.info("aiohttp session closed.")
        logger.info("URLChecker cleanup complete.")

    async def _get_robot_parser(self, base_url: str) -> Optional[robotparser.RobotFileParser]:
        cached_parser_tuple = self._robot_parsers_cache.get(base_url)
        if cached_parser_tuple:
            cached_parser, timestamp = cached_parser_tuple
            if (datetime.now() - timestamp).total_seconds() < ROBOTS_CACHE_EXPIRY_SECONDS:
                return cached_parser

        async with self._robots_fetch_semaphore:
            cached_parser_tuple = self._robot_parsers_cache.get(base_url) # Re-check after acquiring lock
            if cached_parser_tuple:
                cached_parser, timestamp = cached_parser_tuple
                if (datetime.now() - timestamp).total_seconds() < ROBOTS_CACHE_EXPIRY_SECONDS:
                    return cached_parser

            robots_url = urljoin(base_url, "/robots.txt")
            rp = robotparser.RobotFileParser()
            rp.set_url(robots_url)

            if not self.session or self.session.closed:
                logger.warning("aiohttp session not available for robots.txt. Attempting to re-setup.")
                await self.setup()
                if not self.session or self.session.closed: # Check again
                    logger.error("Failed to re-setup session for robots.txt. Assuming allowed by default.")
                    return None # Indicates failure to check robots.txt

            try:
                logger.debug(f"Fetching robots.txt: {robots_url}")
                async with self.session.get(robots_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        content = await resp.text(errors='replace')
                        rp.parse(content.splitlines())
                        logger.info(f"Fetched and parsed robots.txt for {base_url}")
                    elif resp.status in (401, 403):
                        logger.warning(f"Access denied for {robots_url} (Status: {resp.status}). Assuming all disallowed.")
                        # Create a parser that disallows everything for this agent
                        rp.parse([f"User-agent: {self.user_agent}", "Disallow: /"])
                    else: # 404, other errors
                        logger.info(f"robots.txt not found or error at {robots_url} (Status: {resp.status}). Assuming all allowed.")
                        # Default RobotFileParser allows all if not parsed / file not found
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching robots.txt from {robots_url}. Assuming all allowed.")
            except aiohttp.ClientError as e:
                logger.warning(f"ClientError fetching {robots_url}: {e}. Assuming all allowed.")
            except Exception as e:
                logger.error(f"Unexpected error fetching {robots_url}: {e}", exc_info=True)
            
            self._robot_parsers_cache[base_url] = (rp, datetime.now())
            return rp

    async def _apply_crawl_delay(self, base_url: str, rp: Optional[robotparser.RobotFileParser]):
        if not rp: return

        delay = rp.crawl_delay(self.user_agent)
        if delay is not None and delay > 0:
            now = datetime.now()
            last_crawled = self._last_crawl_time_per_domain.get(base_url)
            if last_crawled:
                time_since_last = (now - last_crawled).total_seconds()
                if time_since_last < delay:
                    wait_time = delay - time_since_last
                    logger.info(f"Applying crawl-delay of {wait_time:.2f}s for {base_url} (User-Agent: '{self.user_agent}')")
                    await asyncio.sleep(wait_time)
            self._last_crawl_time_per_domain[base_url] = datetime.now() # Update after waiting or if no wait needed

    async def check_robots_and_delay(self, url: str) -> Tuple[bool, str]:
        if not self.respect_robots:
            return True, ""
        
        try:
            parsed_url = urlparse(url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            rp = await self._get_robot_parser(base_url)
            if rp is None: # Critical failure in getting parser
                logger.error(f"Could not obtain robot parser for {base_url}. Assuming allowed to proceed with caution.")
                return True, "Robots.txt check failed critically, assuming allowed"

            await self._apply_crawl_delay(base_url, rp)
            
            is_allowed = rp.can_fetch(self.user_agent, url)
            reason = f"Disallowed by robots.txt for User-Agent: '{self.user_agent}'" if not is_allowed else ""
            return is_allowed, reason
            
        except asyncio.CancelledError:
            logger.info(f"Robots check cancelled for {url}")
            raise
        except Exception as e:
            logger.error(f"Error in check_robots_and_delay for {url}: {e}", exc_info=True)
            return True, f"Robots.txt check error: {e}"

    async def _adjust_concurrency_periodically(self):
        if self._stop_event.is_set(): return

        now = datetime.now()
        if (now - self._last_adjustment_time).total_seconds() >= self.ADJUSTMENT_INTERVAL_SECONDS:
            if not self.results_buffer: # Avoid division by zero if buffer empty
                self._last_adjustment_time = now
                return

            error_rate = calculate_error_rate(self.results_buffer)
            new_concurrency = adjust_concurrency(self.current_concurrency, error_rate)
            
            if new_concurrency != self.current_concurrency:
                # logger.info(f"Adjusting concurrency: {self.current_concurrency} -> {new_concurrency} (Error rate: {error_rate:.2%})")
                self.current_concurrency = new_concurrency
                # Recreate semaphore with new concurrency
                # Important: Ensure old semaphore is drained or tasks finish before replacing if tasks hold it.
                # Simpler: just replace. New tasks will use the new one. Old ones complete with old limit.
                self.semaphore = asyncio.Semaphore(self.current_concurrency)
            
            self._last_adjustment_time = now
            self.results_buffer.clear()

    @retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=INITIAL_BACKOFF, min=1, max=MAX_BACKOFF), reraise=True)
    async def _fetch_url_with_retry(self, url: str, session: aiohttp.ClientSession):
        logger.debug(f"Attempting to fetch: {url}")
        # The session itself has the User-Agent header and SSL context.
        return await session.get(url, allow_redirects=True) 

    async def fetch_and_parse(self, url: str) -> Dict:
        if self._stop_event.is_set():
            # logger.info(f"Skipping {url} due to stop event.")
            return self._create_error_result(url, "Crawl stopped")

        # Semaphore acquisition should be the first async operation for this task
        async with self.semaphore:
            if self._stop_event.is_set():
                # logger.info(f"Skipping {url} (semaphore acquired but stop event set).")
                return self._create_error_result(url, "Crawl stopped")

            await self._adjust_concurrency_periodically()
            data = self._default_data_dict(url)

            try:
                is_allowed, robots_reason = await self.check_robots_and_delay(url)
                data["Blocked by Robots.txt"] = "No" if is_allowed else "Yes"
                data["Robots Block Rule"] = robots_reason

                if not is_allowed: # No need to check self.respect_robots, check_robots_and_delay handles it
                    data["Indexable"] = "No"
                    data["Indexability Reason"] = robots_reason or "Blocked by robots.txt"
                    logger.info(f"Skipping fetch for {url} due to robots.txt: {robots_reason}")
                    self._add_to_buffer(data)
                    return data
                
                # logger.info(f"Fetching: {url}") # Reducing log verbosity
                resp = await self._fetch_url_with_retry(url, self.session)
                
                html_content_for_discovery = None # For spider mode link discovery

                async with resp:
                    data["Content_Type"] = resp.headers.get("Content-Type", "")
                    # Initial status is from the first response in history, or current if no redirects
                    initial_resp_for_status = resp.history[0] if resp.history else resp
                    data["Initial_Status_Code"] = str(initial_resp_for_status.status)
                    data["Initial_Status_Type"] = self.status_label(initial_resp_for_status.status)
                    
                    data["Final_URL"] = str(resp.url)
                    data["Encoded_URL"] = quote(str(resp.url), safe=':/?=&')
                    data["Final_Status_Code"] = str(resp.status)
                    data["Final_Status_Type"] = self.status_label(resp.status)
                    data["Last Modified"] = resp.headers.get("Last-Modified", "")
                    data["X-Robots-Tag"] = resp.headers.get("X-Robots-Tag", "")
                    
                    if resp.status == 200 and resp.content_type and resp.content_type.lower().startswith("text/html"):
                        # Limit reading large non-HTML files if needed, but for HTML we need it all.
                        # Consider adding a max content size check here if memory is a concern.
                        html_content_for_discovery = await resp.text(errors='replace')
                        self.parse_html_content(data, html_content_for_discovery)
                        data["html_content_for_discovery"] = html_content_for_discovery # Pass for spider
                    elif resp.status != 200:
                        data["Indexable"] = "No" # Default for non-200
                        data["Indexability Reason"] = f"HTTP Status {resp.status}"
                    else: # 200 but not HTML
                        data["Indexable"] = "No"
                        data["Indexability Reason"] = f"Non-HTML Content-Type: {data['Content_Type']}"


            except RetryError as e:
                logger.error(f"Failed to fetch {url} after {MAX_RETRIES} retries: {e.last_attempt.exception()}")
                data.update(self._create_error_result(url, f"Max retries: {type(e.last_attempt.exception()).__name__}"))
                self.failed_urls.add(url)
            except aiohttp.ClientError as e: # Includes sub-errors like ServerDisconnectedError, etc.
                logger.error(f"ClientError fetching {url}: {type(e).__name__} - {e}")
                data.update(self._create_error_result(url, f"ClientError: {type(e).__name__}"))
                self.failed_urls.add(url)
            except asyncio.TimeoutError: # This can be aiohttp's or a broader asyncio timeout
                logger.error(f"Timeout fetching {url}")
                data.update(self._create_error_result(url, "Timeout"))
                self.failed_urls.add(url)
            except asyncio.CancelledError:
                logger.info(f"Task for {url} cancelled.")
                data.update(self._create_error_result(url, "Cancelled"))
                raise 
            except Exception as e:
                logger.error(f"Unexpected error processing {url}: {type(e).__name__} - {e}", exc_info=True)
                data.update(self._create_error_result(url, f"Error: {type(e).__name__}"))
                self.failed_urls.add(url)
            finally:
                # This should be called regardless of success/failure of this specific URL
                update_redirect_label(data, url) 
                self.determine_indexability(data) # Final indexability based on all factors
                self._add_to_buffer(data)
            
            return data

    def _add_to_buffer(self, result_dict: Dict):
        self.results_buffer.append(result_dict)
        if len(self.results_buffer) > self.BUFFER_SIZE:
            self.results_buffer.pop(0)

    def _default_data_dict(self, url: str) -> Dict:
        return {
            "Original_URL": url, "Encoded_URL": "", "Content_Type": "",
            "Initial_Status_Code": "", "Initial_Status_Type": "",
            "Final_URL": url, "Final_Status_Code": "", "Final_Status_Type": "", # Final_URL default to Original
            "Title": "", "Meta Description": "", "H1": "", "H1_Count": 0,
            "Canonical_URL": "", "Meta Robots": "", "X-Robots-Tag": "",
            "HTML Lang": "", "Blocked by Robots.txt": "Unknown", "Robots Block Rule": "",
            "Indexable": "Unknown", "Indexability Reason": "",
            "Last Modified": "", "Word Count": 0,
            "Timestamp": datetime.now().isoformat(),
            "html_content_for_discovery": None # Helper for spider
        }

    def _create_error_result(self, url: str, error_message: str) -> Dict:
        # Create a base dict and update specific error fields
        base_error_dict = self._default_data_dict(url)
        base_error_dict.update({
            "Initial_Status_Code": "Error", "Initial_Status_Type": error_message,
            "Final_Status_Code": "Error", "Final_Status_Type": error_message,
            "Indexable": "No", "Indexability Reason": f"Fetch Error: {error_message}",
        })
        return base_error_dict
    
    @staticmethod
    def status_label(status_code: int) -> str:
        if 200 <= status_code < 300: return "Success"
        if 300 <= status_code < 400: return "Redirect"
        if 400 <= status_code < 500: return "Client Error"
        if 500 <= status_code < 600: return "Server Error"
        return "Unknown"

    def parse_html_content(self, data: Dict, content: str):
        try:
            soup = BeautifulSoup(content, "lxml")
            data["Title"] = soup.title.string.strip() if soup.title and soup.title.string else ""
            
            meta_desc = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
            data["Meta Description"] = meta_desc["content"].strip() if meta_desc and meta_desc.get("content") else ""
            
            h1_tags = soup.find_all("h1")
            data["H1_Count"] = len(h1_tags)
            data["H1"] = h1_tags[0].get_text(separator=" ", strip=True) if h1_tags else ""
            
            canonical_link = soup.find("link", rel=re.compile(r"canonical", re.I)) # Case insensitive rel
            if canonical_link and canonical_link.get("href"):
                # Ensure canonical is absolute
                data["Canonical_URL"] = normalize_url(urljoin(data["Final_URL"], canonical_link["href"].strip()))
            
            meta_robots_tag = soup.find("meta", attrs={"name": re.compile(r"robots", re.I)})
            data["Meta Robots"] = meta_robots_tag["content"].strip().lower() if meta_robots_tag and meta_robots_tag.get("content") else "" # Store lowercase
            
            html_tag = soup.find("html")
            data["HTML Lang"] = html_tag.get("lang", "") if html_tag else ""
            
            text_content = soup.get_text(separator=" ", strip=True)
            data["Word Count"] = len(text_content.split())

        except Exception as e:
            logger.error(f"Error parsing HTML for {data['Original_URL']}: {e}", exc_info=True)

    def determine_indexability(self, data: Dict):
        # If already set to "No" by a fetch error or robots.txt, don't override unless a stronger "No" signal appears
        if data["Indexable"] == "No" and data["Indexability Reason"].startswith("Fetch Error"):
            return # Critical error, don't evaluate further indexability signals

        is_indexable_so_far = data.get("Indexable", "Unknown") != "No"
        reasons = []
        if data.get("Indexability Reason"): # Preserve existing reason if any
            reasons.append(data["Indexability Reason"])

        # HTTP Status (already partially handled, but good for final check)
        if data["Final_Status_Code"] != "200" and data["Final_Status_Code"] != "" and data["Final_Status_Code"] != "Error":
            is_indexable_so_far = False
            if f"HTTP Status {data['Final_Status_Code']}" not in reasons:
                 reasons.append(f"HTTP Status {data['Final_Status_Code']}")
        
        # Robots.txt (already handled by fetch_and_parse logic)
        if data["Blocked by Robots.txt"] == "Yes":
            is_indexable_so_far = False
            # Reason already in data["Robots Block Rule"] or data["Indexability Reason"]
            if data["Robots Block Rule"] and data["Robots Block Rule"] not in reasons:
                 reasons.append(data["Robots Block Rule"])
            elif "Blocked by robots.txt" not in reasons and not data["Robots Block Rule"]:
                 reasons.append("Blocked by robots.txt")


        # Meta Robots (ensure it's lowercase from parsing)
        meta_robots_lower = data["Meta Robots"] # Should be lowercase from parse_html_content
        if "noindex" in meta_robots_lower:
            is_indexable_so_far = False
            if "Meta robots: noindex" not in reasons: reasons.append("Meta robots: noindex")
        
        # X-Robots-Tag (ensure it's lowercase)
        x_robots_lower = data["X-Robots-Tag"].lower()
        if "noindex" in x_robots_lower:
            is_indexable_so_far = False
            if "X-Robots-Tag: noindex" not in reasons: reasons.append("X-Robots-Tag: noindex")
            
        # Canonicalization
        final_url_norm = normalize_url(data["Final_URL"])
        canonical_url_norm = normalize_url(data["Canonical_URL"])

        if canonical_url_norm and final_url_norm and canonical_url_norm != final_url_norm:
            is_indexable_so_far = False 
            reason_canonical = f"Canonical to: {data['Canonical_URL']}"
            if reason_canonical not in reasons: reasons.append(reason_canonical)
        
        data["Indexable"] = "Yes" if is_indexable_so_far else "No"
        
        # Consolidate reasons, removing duplicates and "Unknown" if other reasons exist
        unique_reasons = sorted(list(set(r for r in reasons if r and r != "Unknown")))
        if not unique_reasons and not is_indexable_so_far and data["Final_Status_Code"] == "200":
            # If it's a 200 OK, was not blocked by robots, no noindex, but still non-indexable (e.g. canonical)
            # and no reason was added yet (should be rare if logic above is complete)
            data["Indexability Reason"] = "Non-indexable (check canonical/content)"
        elif unique_reasons:
            data["Indexability Reason"] = "; ".join(unique_reasons)
        elif not is_indexable_so_far and not data.get("Indexability Reason"): # Fallback if no specific reason found yet
            data["Indexability Reason"] = "Non-indexable (undetermined)"
        elif is_indexable_so_far:
            data["Indexability Reason"] = "" # Clear if indexable

    async def recrawl_failed_urls(self, existing_results_urls: Set[str]) -> List[Dict]:
        if not self.failed_urls: return []
        
        urls_to_recrawl = list(self.failed_urls - existing_results_urls)
        if not urls_to_recrawl:
            logger.info("No new unique failed URLs to recrawl.")
            return []
        
        logger.info(f"Recrawling {len(urls_to_recrawl)} unique failed URLs...")
        tasks = []
        for url in urls_to_recrawl:
            task = asyncio.create_task(self.fetch_and_parse(url))
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)
            tasks.append(task)
        
        recrawl_results: List[Dict] = []
        for future in asyncio.as_completed(tasks):
            if self._stop_event.is_set():
                logger.info("Recrawl stopped by event.")
                break
            try:
                result = await future
                if result: recrawl_results.append(result)
            except asyncio.CancelledError:
                logger.info("A recrawl task was cancelled.")
            except Exception as e:
                logger.error(f"Error processing recrawl result: {e}", exc_info=True)
        
        # self.failed_urls.clear() # Clear only if all attempts are exhausted per URL
        return recrawl_results

    def stop(self):
        logger.info("Stop signal received. Crawler will halt after current tasks complete.")
        self._stop_event.set()

    def is_stopped(self) -> bool:
        return self._stop_event.is_set()

# --- End of URLChecker Class ---

def normalize_url(url: str) -> str:
    if not url: return ""
    url = url.strip()
    parsed_url = urlparse(url)
    
    scheme = parsed_url.scheme.lower()
    if not scheme: scheme = 'https' # Default to https
    
    netloc = parsed_url.netloc.lower()
    path = parsed_url.path
    if not path: path = '/' # Ensure path is at least '/'
    
    # Remove trailing slash from path unless it's the root path
    if path != '/' and path.endswith('/'):
        path = path[:-1]
        
    # Reconstruct URL without fragment and with normalized components
    try:
        # Query parameters are case-sensitive in value, but often not in key. Sorting helps normalize.
        # For simplicity, not sorting query params here as it can be complex.
        normalized = urlunparse(
            (scheme, netloc, path, parsed_url.params, parsed_url.query, '') # fragment is empty
        )
        return normalized
    except ValueError:
        logger.warning(f"Could not normalize URL: {url}, returning as is.")
        return url # Return original if unparse fails

def update_redirect_label(data: Dict, original_url: str):
    # This is mostly informational. Indexability itself is handled by determine_indexability.
    final_url = data.get("Final_URL", "")
    final_status_str = data.get("Final_Status_Code", "")

    if not final_status_str or final_status_str == "Error": return

    original_url_norm = normalize_url(original_url)
    final_url_norm = normalize_url(final_url)

    if original_url_norm != final_url_norm:
        # If it redirected, Final_Status_Type should reflect the status of the *final* page
        # The fact it came from a redirect is implicit.
        # The label in URLChecker.status_label is sufficient.
        # data["Final_Status_Type"] = f"{data['Final_Status_Type']} (from {original_url_norm})" # Optional: add redirect source
        pass # Current Final_Status_Type from status_label is likely fine
    else:
        # If no redirect, Final_Status_Type is already set by status_label
        pass


def compile_regex_filters(include_pattern: Optional[str], exclude_pattern: Optional[str]) -> Tuple[Optional[re.Pattern], Optional[re.Pattern]]:
    inc_re, exc_re = None, None
    try:
        if include_pattern: inc_re = re.compile(include_pattern, re.IGNORECASE)
    except re.error as e: logger.error(f"Invalid include regex '{include_pattern}': {e}")
    try:
        if exclude_pattern: exc_re = re.compile(exclude_pattern, re.IGNORECASE)
    except re.error as e: logger.error(f"Invalid exclude regex '{exclude_pattern}': {e}")
    return inc_re, exc_re

def filter_url_by_regex(url: str, inc_re: Optional[re.Pattern], exc_re: Optional[re.Pattern]) -> bool:
    if exc_re and exc_re.search(url): return False
    if inc_re and not inc_re.search(url): return False
    return True

async def discover_links_from_html(page_url: str, html_content: str, base_netloc: str) -> List[str]:
    discovered_links: Set[str] = set() # Use set for uniqueness
    soup = BeautifulSoup(html_content, "lxml")
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        try:
            abs_link = urljoin(page_url, href) # Handles relative links
            norm_link = normalize_url(abs_link)
            if norm_link:
                parsed_link = urlparse(norm_link)
                if parsed_link.scheme in ('http', 'https') and parsed_link.netloc.lower() == base_netloc:
                    discovered_links.add(norm_link)
        except Exception: # Broad catch for malformed hrefs
            # logger.debug(f"Could not process link '{href}' from {page_url}: {e}")
            pass
    return list(discovered_links)


async def run_crawl_logic(
    crawl_type: str, 
    checker: URLChecker,
    show_partial_callback, # UI callback
    seed_url: Optional[str] = None,
    url_list: Optional[List[str]] = None,
    sitemap_urls_input: Optional[List[str]] = None,
    include_pattern_str: Optional[str] = None,
    exclude_pattern_str: Optional[str] = None,
    max_spider_urls_limit: int = DEFAULT_MAX_URLS
) -> List[Dict]:
    
    all_results: List[Dict] = []
    initial_urls_for_crawl: List[str] = []
    inc_re, exc_re = compile_regex_filters(include_pattern_str, exclude_pattern_str)

    if crawl_type == "spider":
        if not seed_url:
            logger.error("Spider mode requires a seed URL.")
            return []
        
        normalized_seed = normalize_url(seed_url)
        initial_urls_for_crawl.append(normalized_seed) # Main seed first
        
        if sitemap_urls_input: # Spider can also use sitemaps as additional seeds
            logger.info(f"Spider mode: Parsing sitemaps for additional seeds: {sitemap_urls_input}")
            sitemap_discovered_urls = await process_sitemaps_for_crawl(
                sitemap_urls_input, 
                checker.user_agent,
                st.session_state.get('sitemap_parsing_ui_callback') # Use UI callback if available
            )
            # Filter sitemap URLs by spider's domain and regex
            base_netloc_spider = urlparse(normalized_seed).netloc.lower()
            for s_url in sitemap_discovered_urls:
                norm_s_url = normalize_url(s_url)
                if urlparse(norm_s_url).netloc.lower() == base_netloc_spider and \
                   filter_url_by_regex(norm_s_url, inc_re, exc_re):
                    initial_urls_for_crawl.append(norm_s_url)
            initial_urls_for_crawl = sorted(list(set(initial_urls_for_crawl))) # Deduplicate
        
        logger.info(f"Starting Spider crawl. Seed URLs: {len(initial_urls_for_crawl)}. Max URLs: {max_spider_urls_limit}")
        all_results = await dynamic_frontier_crawl_manager(
            seed_urls_for_frontier=initial_urls_for_crawl, # Pass all seeds
            main_domain_seed_url_for_context=normalized_seed, # For base_netloc context
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
        initial_urls_for_crawl = sorted(list(set(normalize_url(u) for u in url_list if normalize_url(u))))
        logger.info(f"Starting List crawl with {len(initial_urls_for_crawl)} URLs.")
        all_results = await list_or_sitemap_crawl_manager(initial_urls_for_crawl, checker, show_partial_callback)
        
    elif crawl_type == "sitemap":
        if not sitemap_urls_input:
            logger.error("Sitemap mode requires sitemap URLs.")
            return []
        logger.info(f"Starting Sitemap crawl. Parsing sitemaps: {sitemap_urls_input}")
        initial_urls_for_crawl = await process_sitemaps_for_crawl(
            sitemap_urls_input, 
            checker.user_agent,
            st.session_state.get('sitemap_parsing_ui_callback')
        )
        if not initial_urls_for_crawl:
            logger.warning("No URLs found in provided sitemaps.")
            return []
        logger.info(f"Extracted {len(initial_urls_for_crawl)} URLs from sitemaps for crawling.")
        all_results = await list_or_sitemap_crawl_manager(initial_urls_for_crawl, checker, show_partial_callback)

    else:
        # This path should not be reached if called with mode.lower()
        logger.error(f"Internal error: Unknown crawl type '{crawl_type}' in run_crawl_logic.")
        return []

    # Consolidate results and recrawl failed if any
    # Use a dict to ensure unique URLs by Original_URL, keeping the last encountered version
    final_results_map = {r['Original_URL']: r for r in all_results} 

    if checker.failed_urls:
        successful_urls_in_results = {r['Original_URL'] for r in final_results_map.values() if r.get("Final_Status_Code") not in ["Error", ""]}
        recrawl_candidates = checker.failed_urls - successful_urls_in_results
        
        if recrawl_candidates:
            logger.info(f"Recrawling {len(recrawl_candidates)} URLs that previously failed and are not in successful results...")
            # Temporarily assign only candidates for recrawl to checker's failed_urls
            original_failed_set = checker.failed_urls
            checker.failed_urls = recrawl_candidates
            
            recrawled_data = await checker.recrawl_failed_urls(successful_urls_in_results)
            
            checker.failed_urls = original_failed_set # Restore or clear as appropriate
            
            for res in recrawled_data:
                final_results_map[res['Original_URL']] = res # Update/add recrawled results
        else:
            logger.info("No new failed URLs to recrawl or all previously failed URLs now have a successful entry.")
    
    return list(final_results_map.values())


async def list_or_sitemap_crawl_manager(
    urls_to_crawl: List[str],
    checker: URLChecker,
    show_partial_callback # UI callback
):
    """Manages crawling for a fixed list of URLs (List or Sitemap mode)."""
    results: List[Dict] = []
    total_urls = len(urls_to_crawl)
    
    await checker.setup() # Ensure session is ready

    tasks = []
    for i, url in enumerate(urls_to_crawl):
        if checker.is_stopped(): break
        task = asyncio.create_task(checker.fetch_and_parse(url))
        checker._pending_tasks.add(task) # Track for cleanup
        
        # Add callback to process result as soon as it's done
        def task_completed_callback(fut, _url=url, _idx=i):
            checker._pending_tasks.discard(fut) # Untrack
            try:
                result = fut.result()
                if result: results.append(result)
            except asyncio.CancelledError:
                logger.info(f"Task for {_url} was cancelled.")
                # Add a "cancelled" result to maintain counts if needed, or just log
                results.append(checker._create_error_result(_url, "Cancelled by user"))
            except Exception as e_cb: # Should be rare as fetch_and_parse catches most
                logger.error(f"Error in task callback for {_url}: {e_cb}")
                results.append(checker._create_error_result(_url, f"Callback Error: {e_cb}"))
            
            # Update UI (results list is growing)
            # Pass a copy of results list to UI callback
            show_partial_callback(list(results), len(results), total_urls)

        task.add_done_callback(task_completed_callback)
        tasks.append(task) # Keep reference to await all at end if needed

    # Wait for all initial tasks to be submitted and their callbacks to potentially run
    # However, the actual processing completion is driven by the callbacks.
    # We need to ensure all tasks are truly finished or cancelled before cleanup.
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True) # Wait for all spawned tasks

    # Final UI update after all tasks are gathered (callbacks should have mostly updated)
    show_partial_callback(list(results), len(results), total_urls)
    
    # Cleanup is called by the main Streamlit logic after run_crawl_logic finishes
    # await checker.cleanup() # Do not call cleanup here, called by outer function
    return results


# --- Sitemap Processing ---
async def fetch_sitemap_content(session: aiohttp.ClientSession, url: str, user_agent: str) -> Optional[str]:
    try:
        async with session.get(url, headers={"User-Agent": user_agent}, timeout=aiohttp.ClientTimeout(total=20)) as response:
            response.raise_for_status()
            return await response.text(errors='replace')
    except asyncio.TimeoutError: logger.error(f"Timeout fetching sitemap: {url}")
    except aiohttp.ClientError as e: logger.error(f"ClientError fetching sitemap {url}: {e}")
    except Exception as e: logger.error(f"Error fetching sitemap {url}: {e}", exc_info=True)
    return None

async def parse_single_sitemap_xml(xml_content: str, sitemap_url: str) -> Tuple[List[str], List[str]]:
    page_urls: List[str] = []
    nested_sitemap_urls: List[str] = []
    try:
        # ET.fromstring expects bytes if there's an XML declaration with encoding,
        # or a string if not. Giving it UTF-8 encoded bytes is safer.
        xml_bytes = xml_content.encode('utf-8')
        root = ET.fromstring(xml_bytes)
        
        # Simplify namespace handling: look for local names 'sitemapindex', 'urlset', 'loc'
        # This is a common way to bypass full namespace declaration.
        is_sitemap_index = "sitemapindex" in root.tag.lower()
        is_urlset = "urlset" in root.tag.lower()

        if is_sitemap_index:
            for sitemap_loc_element in root.findall('.//{*}loc'): # Find all 'loc' tags anywhere under a 'sitemap' tag
                if sitemap_loc_element.text:
                    nested_sitemap_urls.append(sitemap_loc_element.text.strip())
        elif is_urlset:
            for url_loc_element in root.findall('.//{*}loc'): # Find all 'loc' tags anywhere under a 'url' tag
                if url_loc_element.text:
                    page_urls.append(url_loc_element.text.strip())
        else: # Try a more generic search if specific tags not found (e.g. atom feeds sometimes used as sitemaps)
            for link_element in root.findall(".//{*}link[@rel='alternate']/[@href]"): # Atom feed <link rel="alternate" href="..."/>
                 if link_element.get('href'): page_urls.append(link_element.get('href').strip())
            if not page_urls: # If still nothing, log warning
                logger.warning(f"Unknown root tag or structure in XML sitemap {sitemap_url}: {root.tag}")

    except ET.ParseError as e: logger.error(f"XML ParseError for sitemap {sitemap_url}: {e}")
    except Exception as e: logger.error(f"Error parsing XML sitemap {sitemap_url}: {e}", exc_info=True)
    return page_urls, nested_sitemap_urls

def parse_single_sitemap_text(text_content: str) -> List[str]:
    urls = []
    for line in text_content.splitlines():
        line = line.strip()
        # Basic validation: not empty, not a comment, looks like a URL
        if line and not line.startswith('#') and (line.startswith('http://') or line.startswith('https://')):
            urls.append(line)
    return urls

async def process_sitemaps_for_crawl(
    sitemap_urls_input: List[str], 
    user_agent: str,
    show_partial_sitemap_parsing_ui # Callback for Streamlit UI
    ) -> List[str]:
    
    all_found_page_urls: Set[str] = set()
    sitemaps_to_process_queue = asyncio.Queue()
    processed_sitemap_urls: Set[str] = set() # Track sitemaps already processed to avoid loops

    for s_url in sitemap_urls_input:
        norm_s_url = normalize_url(s_url)
        if norm_s_url not in processed_sitemap_urls:
            await sitemaps_to_process_queue.put(norm_s_url)
            processed_sitemap_urls.add(norm_s_url) # Mark as "to be processed"

    # Use a single session for all sitemap fetching operations
    connector = aiohttp.TCPConnector(ssl=ssl.create_default_context(cafile=certifi.where()))
    async with aiohttp.ClientSession(connector=connector, json_serialize=orjson.dumps) as session:
        active_sitemap_processing_tasks = 0
        MAX_CONCURRENT_SITEMAP_FETCHES = 5 # Limit for fetching sitemap files themselves

        while True:
            # Condition to break: queue is empty AND no sitemap tasks are active
            if sitemaps_to_process_queue.empty() and active_sitemap_processing_tasks == 0:
                break

            # Launch new tasks if queue has items and we are below concurrent fetch limit
            while not sitemaps_to_process_queue.empty() and active_sitemap_processing_tasks < MAX_CONCURRENT_SITEMAP_FETCHES:
                current_sitemap_url = await sitemaps_to_process_queue.get()
                sitemaps_to_process_queue.task_done()
                active_sitemap_processing_tasks +=1
                
                # Define the actual processing task for a single sitemap URL
                async def process_one_sitemap(s_url):
                    nonlocal active_sitemap_processing_tasks # To decrement when done
                    try:
                        # logger.info(f"Processing sitemap: {s_url}")
                        sitemap_content = await fetch_sitemap_content(session, s_url, user_agent)
                        if sitemap_content:
                            page_urls_from_current, nested_sitemaps_from_current = await parse_single_sitemap_xml(sitemap_content, s_url)
                            
                            if not page_urls_from_current and not nested_sitemaps_from_current: # Try as text if XML failed
                                page_urls_from_current = parse_single_sitemap_text(sitemap_content)

                            for p_url in page_urls_from_current:
                                all_found_page_urls.add(normalize_url(p_url))
                            
                            for ns_url_raw in nested_sitemaps_from_current:
                                norm_ns_url = normalize_url(ns_url_raw)
                                if norm_ns_url not in processed_sitemap_urls: # Check before adding to queue
                                    await sitemaps_to_process_queue.put(norm_ns_url)
                                    processed_sitemap_urls.add(norm_ns_url) # Mark to avoid re-queueing
                        
                        if show_partial_sitemap_parsing_ui: # Update UI with current found URLs
                            show_partial_sitemap_parsing_ui(list(all_found_page_urls))
                    finally:
                        active_sitemap_processing_tasks -=1
                
                asyncio.create_task(process_one_sitemap(current_sitemap_url))
            
            await asyncio.sleep(0.1) # Short sleep to yield control and check loop conditions

    logger.info(f"Sitemap processing complete. Found {len(all_found_page_urls)} unique page URLs.")
    return sorted(list(all_found_page_urls))


# --- Spidering Logic (Dynamic Frontier) ---
async def dynamic_frontier_crawl_manager(
    seed_urls_for_frontier: List[str], # All initial URLs (main seed + sitemap seeds)
    main_domain_seed_url_for_context: str, # The primary seed URL for domain context
    checker: URLChecker,
    include_regex: Optional[re.Pattern],
    exclude_regex: Optional[re.Pattern],
    show_partial_callback, # UI callback
    max_urls: int = DEFAULT_MAX_URLS
) -> List[Dict]:
    
    all_crawl_results: List[Dict] = []
    frontier = asyncio.Queue() 
    # URLs that have been added to the frontier (or already processed from it)
    # This prevents adding the same URL to the frontier multiple times and re-crawling.
    urls_in_frontier_or_processed: Set[str] = set() 
    
    parsed_main_seed = urlparse(main_domain_seed_url_for_context)
    base_netloc_for_spider = parsed_main_seed.netloc.lower()
    if not base_netloc_for_spider:
        logger.error("Spider Error: Invalid main seed URL for domain context.")
        return []
        
    logger.info(f"Spider: Initializing with base netloc '{base_netloc_for_spider}'. Max URLs: {max_urls}.")

    for u_seed in seed_urls_for_frontier: # Add initial seeds
        # Ensure seed is normalized, on same domain, and passes filters
        norm_u_seed = normalize_url(u_seed)
        if norm_u_seed and urlparse(norm_u_seed).netloc.lower() == base_netloc_for_spider and \
           filter_url_by_regex(norm_u_seed, include_regex, exclude_regex):
            if norm_u_seed not in urls_in_frontier_or_processed:
                await frontier.put(norm_u_seed)
                urls_in_frontier_or_processed.add(norm_u_seed)

    await checker.setup() # Ensure session is ready
    
    active_fetch_tasks = 0
    # Loop while (there are URLs in frontier OR active tasks) AND (not stopped) AND (below max_urls)
    while (not frontier.empty() or active_fetch_tasks > 0) and \
          not checker.is_stopped() and \
          len(all_crawl_results) < max_urls:

        if not frontier.empty() and len(all_crawl_results) + active_fetch_tasks < max_urls :
            current_url_to_crawl = await frontier.get()
            frontier.task_done()
            
            # It's possible a URL was added to frontier then processed by another means, or added twice.
            # This check is a safeguard, primary prevention is at point of adding to urls_in_frontier_or_processed
            # if current_url_to_crawl is already in a set of "URLs for which we have a result", skip.
            # For simplicity, we rely on urls_in_frontier_or_processed.

            active_fetch_tasks += 1
            task = asyncio.create_task(checker.fetch_and_parse(current_url_to_crawl))
            checker._pending_tasks.add(task)

            def spider_task_result_processor(fut, fetched_url=current_url_to_crawl):
                nonlocal active_fetch_tasks # Allow modification
                active_fetch_tasks -= 1
                checker._pending_tasks.discard(fut)
                
                try:
                    result_dict = fut.result()
                    if result_dict:
                        all_crawl_results.append(result_dict)

                        # Discover new links if successful HTML, within limits, and on same domain
                        if result_dict.get("Content_Type", "").lower().startswith("text/html") and \
                           result_dict.get("Final_Status_Code") == "200" and \
                           len(urls_in_frontier_or_processed) < max_urls : # Check total known URLs before adding more
                            
                            html_for_links = result_dict.get("html_content_for_discovery")
                            if html_for_links:
                                # Run link discovery as a separate async sub-task to not block this callback
                                async def discover_and_queue_links():
                                    newly_discovered_links = await discover_links_from_html(
                                        result_dict["Final_URL"], 
                                        html_for_links, 
                                        base_netloc_for_spider
                                    )
                                    for link in newly_discovered_links:
                                        if len(urls_in_frontier_or_processed) >= max_urls: break # Check global limit
                                        if link not in urls_in_frontier_or_processed and \
                                           filter_url_by_regex(link, include_regex, exclude_regex):
                                            urls_in_frontier_or_processed.add(link)
                                            await frontier.put(link)
                                asyncio.create_task(discover_and_queue_links())
                
                except asyncio.CancelledError:
                    logger.info(f"Spider task for {fetched_url} was cancelled.")
                    all_crawl_results.append(checker._create_error_result(fetched_url, "Cancelled by user"))
                except Exception as e_cb_spider:
                    logger.error(f"Error in spider task callback for {fetched_url}: {e_cb_spider}", exc_info=True)
                    all_crawl_results.append(checker._create_error_result(fetched_url, f"Spider Callback Error: {e_cb_spider}"))
                finally:
                     # Update UI: crawled count is len(all_crawl_results)
                     # total is len(urls_in_frontier_or_processed) (all unique URLs intended for processing or in queue)
                     # or max_urls if that's the cap.
                    ui_total_estimate = max(len(urls_in_frontier_or_processed), frontier.qsize() + len(all_crawl_results))
                    ui_total_display = min(ui_total_estimate, max_urls) # Cap display total at max_urls
                    show_partial_callback(list(all_crawl_results), len(all_crawl_results), ui_total_display)

            task.add_done_callback(spider_task_result_processor)
        
        else: # Frontier empty or max concurrent tasks, or max_urls nearly hit by active tasks
            if checker.is_stopped() or \
               (frontier.empty() and active_fetch_tasks == 0) or \
               len(all_crawl_results) >= max_urls:
                break # Exit outer while loop
            await asyncio.sleep(0.1) # Yield control, wait for tasks to finish or new items in queue
                
    logger.info(f"Spider crawl finished. URLs processed for results: {len(all_crawl_results)}. Total unique URLs considered: {len(urls_in_frontier_or_processed)}")
    # await checker.cleanup() # Cleanup called by outer function
    return all_crawl_results


# --- Streamlit UI and Main Logic ---
def format_and_reorder_df(df: pd.DataFrame) -> pd.DataFrame:
    main_cols = [
        'Original_URL', 'Content_Type', 'Initial_Status_Code', 'Initial_Status_Type',
        'Indexable', 'Indexability Reason', 'Blocked by Robots.txt', 'Robots Block Rule',
        'Final_URL', 'Final_Status_Code', 'Final_Status_Type',
        'Meta Robots', 'X-Robots-Tag', 'Canonical_URL',
        'Title', 'Meta Description', 'H1', 'H1_Count', 'Word Count',
        'Last Modified', 'HTML Lang', 'Timestamp', 'Encoded_URL'
    ]
    existing_cols_in_df = df.columns.tolist()
    ordered_cols = [col for col in main_cols if col in existing_cols_in_df]
    other_cols = [col for col in existing_cols_in_df if col not in ordered_cols]
    final_col_order = ordered_cols + other_cols
    return df[final_col_order]

def main_streamlit_app():
    st.set_page_config(layout="wide", page_title="Web Crawler SEO Tool")
    st.title("⚙️ Web Crawler & SEO Analyzer")

    # UI Change: Mode selection in main area
    mode = st.radio(
        "Select Crawl Mode:", 
        ["Spider", "List", "Sitemap"], 
        key="crawl_mode_selector", 
        index=0, 
        horizontal=True
    )

    # Initialize session state variables
    if 'is_crawling' not in st.session_state: st.session_state['is_crawling'] = False
    if 'crawl_results' not in st.session_state: st.session_state['crawl_results'] = []
    if 'crawl_done' not in st.session_state: st.session_state['crawl_done'] = False
    if 'checker_instance' not in st.session_state: st.session_state['checker_instance'] = None
    # if 'sitemap_parsed_urls' not in st.session_state: st.session_state['sitemap_parsed_urls'] = [] # Not directly used globally now
    st.session_state['current_crawl_mode'] = mode # Keep track of selected mode

    st.sidebar.header("⚙️ Configuration")
    ua_choice = st.sidebar.selectbox("User Agent", list(USER_AGENTS.keys()), index=list(USER_AGENTS.keys()).index("Custom Bot"))
    user_agent = USER_AGENTS[ua_choice]
    if ua_choice == "Custom Bot":
        user_agent = st.sidebar.text_input("Custom User Agent String", value=DEFAULT_USER_AGENT)

    concurrency = st.sidebar.slider("Concurrent Requests", MIN_CONCURRENCY, MAX_CONCURRENCY, DEFAULT_CONCURRENCY, 1) # Step 1 for finer control
    respect_robots = st.sidebar.checkbox("Respect robots.txt & Crawl-Delay", value=True)
    timeout_val = st.sidebar.slider("Timeout per URL (seconds)", 5, 60, DEFAULT_TIMEOUT, 5)

    # Inputs based on mode
    seed_url_input, list_input_text, sitemap_input_text = "", "", ""
    include_pattern, exclude_pattern = "", ""
    max_spider_urls = DEFAULT_MAX_URLS # Default

    if mode == "Spider":
        st.subheader("🕷️ Spider Crawl Settings")
        seed_url_input = st.text_input("Seed URL (e.g., https://www.example.com)", placeholder="Enter a single URL to start crawling from")
        max_spider_urls = st.number_input("Max URLs to Crawl", min_value=10, max_value=50000, value=DEFAULT_MAX_URLS, step=100)
        with st.expander("Advanced Filters (Regex for Discovered URLs)"):
            include_pattern = st.text_input("Include URLs Matching Regex", placeholder="e.g., /products/")
            # Fix: Use raw string for regex placeholder
            exclude_pattern = st.text_input("Exclude URLs Matching Regex", placeholder=r"e.g., /blog/page/\d+")
        
        sitemap_spider_seed_mode = st.checkbox("Use Sitemaps as additional seeds for Spider")
        if sitemap_spider_seed_mode:
             sitemap_input_text = st.text_area("Sitemap URLs for Spider (one per line)", height=100, key="sitemap_spider_seeds")

    elif mode == "List":
        st.subheader("📋 List Crawl Settings")
        list_input_text = st.text_area("Enter URLs (one per line)", height=200)

    elif mode == "Sitemap":
        st.subheader("🗺️ Sitemap Crawl Settings")
        sitemap_input_text = st.text_area("Sitemap URLs (one per line, can be sitemap index files)", height=150, key="sitemap_main_input")
        
        # This expander is for showing sitemap parsing progress
        with st.expander("Discovered URLs from Sitemaps (Parsing Live Update)", expanded=False):
            sitemap_progress_ph = st.empty()
            sitemap_table_ph = st.empty()
            
            def show_partial_sitemap_parsing_ui_callback(discovered_sitemap_urls_list):
                sitemap_progress_ph.info(f"Parsing sitemaps... Found {len(discovered_sitemap_urls_list)} URLs so far.")
                if discovered_sitemap_urls_list:
                    df_sitemap_temp = pd.DataFrame(sorted(list(set(discovered_sitemap_urls_list))), columns=["Discovered URLs"])
                    sitemap_table_ph.dataframe(df_sitemap_temp, height=300, use_container_width=True)
                else:
                    sitemap_table_ph.empty()
            st.session_state['sitemap_parsing_ui_callback'] = show_partial_sitemap_parsing_ui_callback


    st.markdown("---")
    button_cols = st.columns(2)
    with button_cols[0]:
        if st.session_state['is_crawling']:
            if st.button("🛑 Stop Crawl", type="primary", use_container_width=True):
                if st.session_state['checker_instance']:
                    st.session_state['checker_instance'].stop()
                # is_crawling will be set to False by the crawl logic itself when it acknowledges stop
                st.info("Stop signal sent. Crawl will halt after current operations...")
                # Don't rerun here, let the crawl loop break and then rerun.
        else:
            if st.button("🚀 Start Crawl", type="primary", use_container_width=True):
                valid_input = True
                if mode == "Spider" and not seed_url_input.strip():
                    st.error("Spider mode: Seed URL is required."); valid_input = False
                elif mode == "List" and not list_input_text.strip():
                    st.error("List mode: Please enter URLs."); valid_input = False
                elif mode == "Sitemap" and not sitemap_input_text.strip():
                    st.error("Sitemap mode: Please enter Sitemap URLs."); valid_input = False
                elif mode == "Spider" and sitemap_spider_seed_mode and not sitemap_input_text.strip():
                    st.error("Spider mode with sitemap seeds: Please enter Sitemap URLs or uncheck the option."); valid_input = False


                if valid_input:
                    st.session_state['is_crawling'] = True
                    st.session_state['crawl_results'] = [] # Reset results for new crawl
                    st.session_state['crawl_done'] = False
                    
                    checker = URLChecker(user_agent, concurrency, timeout_val, respect_robots)
                    st.session_state['checker_instance'] = checker
                    
                    logger.info(f"Attempting to start crawl in {mode} mode.")
                    st.rerun()

    if st.session_state['is_crawling'] or st.session_state['crawl_done']:
        st.markdown("---")
        st.subheader("📊 Crawl Progress & Results")
        progress_text_ph = st.empty()
        progress_bar_ph = st.progress(0.0)
        
        results_expander = st.expander("Live Results Table", expanded=True)
        with results_expander:
            table_ph = st.empty()
        download_ph = st.empty() # Placeholder for download buttons after completion

    def show_partial_data_ui(current_results_list_from_crawler: List[Dict], processed_count: int, total_count_estimate: int):
        # This callback receives the *cumulative* list of results from the active crawl.
        st.session_state['crawl_results'] = current_results_list_from_crawler

        ratio = (processed_count / total_count_estimate) if total_count_estimate > 0 else 0
        ratio = min(1.0, max(0.0, ratio)) 
        
        progress_bar_ph.progress(ratio)
        remain_estimate = max(0, total_count_estimate - processed_count)
        progress_text_ph.info(
            f"Crawling... Processed {processed_count} of ~{total_count_estimate} URLs ({ratio:.1%}). Remaining: ~{remain_estimate}"
        )
        
        # Update DataFrame display (can be throttled if too frequent for large crawls)
        if current_results_list_from_crawler: # and (processed_count % 10 == 0 or processed_count == total_count_estimate or st.session_state['checker_instance'].is_stopped()):
            df_temp = pd.DataFrame(current_results_list_from_crawler)
            if not df_temp.empty:
                df_temp = format_and_reorder_df(df_temp)
                table_ph.dataframe(df_temp, height=400, use_container_width=True)

    if st.session_state['is_crawling'] and not st.session_state['crawl_done']:
        checker = st.session_state['checker_instance']
        final_results_agg: List[Dict] = [] # To store results from the crawl
        
        # Prepare sitemap URLs if spider mode with sitemap seeds is active
        sitemap_urls_for_spider_seeds = None
        if mode == "Spider" and sitemap_spider_seed_mode and sitemap_input_text.strip():
            sitemap_urls_for_spider_seeds = [s.strip() for s in sitemap_input_text.splitlines() if s.strip()]


        crawl_args = {
            "crawl_type": mode.lower(), # Fix: Pass lowercase mode
            "checker": checker,
            "show_partial_callback": show_partial_data_ui,
            "seed_url": seed_url_input.strip() if mode == "Spider" else None,
            "url_list": [u.strip() for u in list_input_text.splitlines() if u.strip()] if mode == "List" else None,
            "sitemap_urls_input": sitemap_urls_for_spider_seeds if mode == "Spider" and sitemap_spider_seed_mode else ([s.strip() for s in sitemap_input_text.splitlines() if s.strip()] if mode == "Sitemap" else None),
            "include_pattern_str": include_pattern if mode == "Spider" else None,
            "exclude_pattern_str": exclude_pattern if mode == "Spider" else None,
            "max_spider_urls_limit": max_spider_urls if mode == "Spider" else 0 
        }

        current_loop : Optional[asyncio.AbstractEventLoop] = None
        try:
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError: # No running loop
                current_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(current_loop)

            # nest_asyncio.apply() should make run_until_complete work even if loop is 'running' from Streamlit's perspective
            final_results_agg = current_loop.run_until_complete(run_crawl_logic(**crawl_args))
            
            st.session_state['crawl_results'] = final_results_agg
            st.session_state['crawl_done'] = True
        
        except Exception as e_crawl:
            logger.error(f"Crawl execution error: {e_crawl}", exc_info=True)
            st.error(f"An error occurred during the crawl: {e_crawl}")
            st.session_state['crawl_done'] = True # Mark as done to show partial if any
        finally:
            st.session_state['is_crawling'] = False
            # Cleanup for checker (like closing session) should be handled within run_crawl_logic
            # or explicitly called on checker if run_crawl_logic might not await it on error.
            # The checker.cleanup() is called in the final part of run_crawl_logic's constituent functions.
            
            # Fix: Simplified loop management, avoid nest_asyncio.has_nested_asyncio()
            # and manual loop.close() when nest_asyncio is active.
            # `nest_asyncio` is designed to manage the loop lifecycle after `apply()`.
            # No explicit loop closing here to prevent conflicts.
            st.rerun()

    if st.session_state['crawl_done']:
        final_results_to_display = st.session_state['crawl_results']
        progress_text_ph.success(f"Crawl finished! Found {len(final_results_to_display)} results.")
        progress_bar_ph.progress(1.0)
        
        df_final = pd.DataFrame(final_results_to_display)
        if not df_final.empty:
            df_final_formatted = format_and_reorder_df(df_final)
            table_ph.dataframe(df_final_formatted, height=500, use_container_width=True) # Display in main table placeholder
            
            csv_data = df_final_formatted.to_csv(index=False).encode('utf-8')
            excel_buffer = io.BytesIO()
            df_final_formatted.to_excel(excel_buffer, index=False, engine='openpyxl')
            excel_data_bytes = excel_buffer.getvalue()

            dl_cols = download_ph.columns(2) # Use the download placeholder
            with dl_cols[0]:
                dl_cols[0].download_button(
                    label="📥 Download CSV", data=csv_data,
                    file_name=f"crawl_results_{mode.lower()}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv", use_container_width=True
                )
            with dl_cols[1]:
                dl_cols[1].download_button(
                    label="📥 Download Excel", data=excel_data_bytes,
                    file_name=f"crawl_results_{mode.lower()}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True
                )
            show_summary_stats(df_final_formatted)
        else:
            table_ph.info("No results to display.")
            download_ph.empty() # Clear download area if no results

def show_summary_stats(df: pd.DataFrame):
    st.subheader("📈 Summary Statistics")
    if df.empty:
        st.write("No data available for summary."); return

    # Basic counts
    total_processed = len(df)
    indexable_count = df[df["Indexable"] == "Yes"].shape[0] if "Indexable" in df.columns else 0
    blocked_robots_count = df[df["Blocked by Robots.txt"] == "Yes"].shape[0] if "Blocked by Robots.txt" in df.columns else 0
    
    summary_cols = st.columns(3)
    summary_cols[0].metric("Total URLs Analyzed", total_processed)
    summary_cols[1].metric("Indexable URLs", indexable_count)
    summary_cols[2].metric("Blocked by Robots.txt", blocked_robots_count)

    # Status Code Distribution
    if "Final_Status_Code" in df.columns:
        st.write("**Final Status Code Distribution:**")
        status_counts = df["Final_Status_Code"].value_counts().reset_index()
        status_counts.columns = ["Status Code", "Count"]
        st.dataframe(status_counts, use_container_width=True)

    # Indexability Reasons for Non-Indexable URLs
    if "Indexability Reason" in df.columns and "Indexable" in df.columns:
        non_indexable_df = df[df["Indexable"] == "No"]
        if not non_indexable_df.empty:
            st.write("**Top Non-Indexability Reasons:**")
            reason_counts = non_indexable_df["Indexability Reason"].value_counts().nlargest(10).reset_index() # Top 10
            reason_counts.columns = ["Reason", "Count"]
            st.dataframe(reason_counts, use_container_width=True)

if __name__ == "__main__":
    main_streamlit_app()
