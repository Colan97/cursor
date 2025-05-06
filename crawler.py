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

nest_asyncio.apply()

# -----------------------------
# Logging Config
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='url_checker.log'
)

# -----------------------------
# Constants
# -----------------------------
DEFAULT_TIMEOUT = 15
DEFAULT_MAX_URLS = 25000
MAX_REDIRECTS = 5
DEFAULT_USER_AGENT = "custom_adidas_seo_x3423/1.0"
SAVE_INTERVAL = 100  # Save results every 100 URLs
ERROR_THRESHOLD = 0.1  # 10% error rate threshold
MIN_CONCURRENCY = 1
MAX_CONCURRENCY = 50
DEFAULT_CONCURRENCY = 10

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
        self.robots_cache = {}
        self.session = None
        self.semaphore = None
        self.failed_urls = set()
        self.recent_results = []
        self.last_save_time = datetime.now()
        self.save_filename = f"crawl_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    async def setup(self):
        connector = aiohttp.TCPConnector(
            limit=9999,  
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            force_close=False
        )
        timeout_settings = aiohttp.ClientTimeout(
            total=None,
            connect=self.timeout,
            sock_read=self.timeout
        )
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout_settings,
            json_serialize=orjson.dumps
        )
        self.semaphore = asyncio.Semaphore(self.concurrency)

    async def close(self):
        if self.session:
            await self.session.close()
        # Save any remaining results
        if self.recent_results:
            save_results_to_file(self.recent_results, self.save_filename)

    def update_concurrency(self):
        """Update concurrency based on recent error rate."""
        error_rate = calculate_error_rate(self.recent_results[-100:] if self.recent_results else [])
        new_concurrency = adjust_concurrency(self.concurrency, error_rate)
        if new_concurrency != self.concurrency:
            self.concurrency = new_concurrency
            self.semaphore = asyncio.Semaphore(self.concurrency)
            logging.info(f"Adjusted concurrency to {new_concurrency} based on error rate {error_rate:.2%}")

    def should_save_results(self) -> bool:
        """Check if it's time to save results."""
        now = datetime.now()
        if (now - self.last_save_time).seconds >= 300 or len(self.recent_results) >= SAVE_INTERVAL:
            self.last_save_time = now
            return True
        return False

    async def fetch_and_parse(self, url: str) -> Dict:
        async with self.semaphore:
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
                "Meta_Description": "",
                "H1_Text": "",
                "H1_Count": 0,
                "Canonical_URL": "",
                "Meta_Robots": "",
                "X_Robots_Tag": "",
                "HTML_Lang": "",
                "Is_Blocked_by_Robots": "",
                "Robots_Block_Rule": "",
                "Is_Indexable": "No",
                "Indexability_Reason": "",
                "HTTP_Last_Modified": "",
                "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }

            allowed = await self.check_robots(url)
            data["Is_Blocked_by_Robots"] = "No" if allowed else "Yes"
            if not allowed:
                data["Robots_Block_Rule"] = "Disallow"
                data["Indexability_Reason"] = "Blocked by robots.txt"
                data["Final_URL"] = url
                data["Final_Status_Code"] = "N/A"
                data["Final_Status_Type"] = "Robots Block"
                logging.info(f"URL blocked by robots.txt: {url}")
                return data

            headers = {"User-Agent": self.user_agent}
            try:
                logging.info(f"Making request to {url} with headers: {headers}")
                async with self.session.get(url, headers=headers, ssl=False, allow_redirects=False) as resp:
                    # Add Content-Type field before status codes
                    data["Content_Type"] = resp.headers.get("Content-Type", "")
                    init_str = str(resp.status)
                    data["Initial_Status_Code"] = init_str
                    data["Initial_Status_Type"] = self.status_label(resp.status)
                    data["Final_URL"] = str(resp.url)
                    logging.info(f"Initial response for {url}: Status {init_str}, Type {data['Initial_Status_Type']}")

                    if resp.status in (301, 302, 307, 308):
                        loc = resp.headers.get("Location")
                        if not loc:
                            data["Final_Status_Code"] = init_str
                            data["Final_Status_Type"] = data["Initial_Status_Type"]
                            data["Indexability_Reason"] = "Redirect w/o Location"
                            data = update_redirect_label(data, url)
                            logging.warning(f"Redirect without Location header for {url}")
                            return data
                        logging.info(f"Following redirect from {url} to {loc}")
                        result = await self.follow_redirect_chain(url, loc, data, headers)
                        result = update_redirect_label(result, url)
                        return result
                    else:
                        if resp.status == 200 and resp.content_type and resp.content_type.startswith("text/html"):
                            content = await resp.text(errors='replace')
                            result = self.parse_html_content(data, content, resp.headers, resp.status, True)
                            result = update_redirect_label(result, url)
                            logging.info(f"Successfully parsed HTML content from {url}")
                            return result
                        else:
                            data["Final_Status_Code"] = init_str
                            data["Final_Status_Type"] = data["Initial_Status_Type"]
                            data["Indexability_Reason"] = "Non-200 or non-HTML"
                            data = update_redirect_label(data, url)
                            logging.info(f"Non-HTML or non-200 response from {url}: {init_str}")
                            return data
            except asyncio.TimeoutError:
                logging.error(f"Timeout while fetching {url}")
                data["Initial_Status_Code"] = "Timeout"
                data["Initial_Status_Type"] = "Request Timeout"
                data["Final_URL"] = url
                data["Final_Status_Code"] = "Timeout"
                data["Final_Status_Type"] = "Request Timeout"
                data["Indexability_Reason"] = "Timeout"
                data = update_redirect_label(data, url)
                return data
            except Exception as e:
                logging.error(f"Error fetching {url}: {str(e)}")
                data["Initial_Status_Code"] = "Error"
                data["Initial_Status_Type"] = str(e)
                data["Final_URL"] = url
                data["Final_Status_Code"] = "Error"
                data["Final_Status_Type"] = str(e)
                data["Indexability_Reason"] = "Exception"
                data = update_redirect_label(data, url)
                return data

    async def recrawl_failed_urls(self) -> List[Dict]:
        """Recrawl URLs that previously failed."""
        if not self.failed_urls:
            return []
        
        results = []
        for url in self.failed_urls:
            try:
                result = await self.fetch_and_parse(url)
                results.append(result)
            except Exception as e:
                logging.error(f"Error recrawling {url}: {e}")
        
        return results 

async def dynamic_frontier_crawl(
    seed_url: str,
    checker: URLChecker,
    include_regex: Optional[str],
    exclude_regex: Optional[str],
    show_partial_callback=None
) -> List[Dict]:
    """
    Dynamic frontier crawl implementation.
    """
    visited: Set[str] = set()
    results = []
    frontier = asyncio.PriorityQueue()
    await frontier.put((0, seed_url))
    base_netloc = urlparse(seed_url).netloc.lower()
    inc, exc = compile_filters(include_regex, exclude_regex)

    logging.info(f"Starting dynamic frontier crawl from seed URL: {seed_url}")
    
    try:
        await checker.setup()
        while not frontier.empty() and len(visited) < DEFAULT_MAX_URLS:
            depth, url = await frontier.get()
            norm_url = normalize_url(url)
            
            if norm_url in visited:
                continue
                
            visited.add(norm_url)
            logging.info(f"Crawling URL: {norm_url}")
            
            result = await checker.fetch_and_parse(norm_url)
            results.append(result)
            
            if show_partial_callback:
                crawled_count = len(visited)
                discovered_count = crawled_count + frontier.qsize()
                show_partial_callback(results, crawled_count, discovered_count)
            
            # Discover new links from the current page
            discovered_links = await discover_links(norm_url, checker.session, checker.user_agent)
            logging.info(f"Discovered {len(discovered_links)} links from {norm_url}")
            
            for link in discovered_links:
                norm_link = normalize_url(link)
                parsed_link = urlparse(norm_link)
                
                # Crawl only internal URLs (matching the seed's netloc)
                if parsed_link.netloc.lower() != base_netloc:
                    continue
                    
                if not regex_filter(norm_link, inc, exc):
                    continue
                    
                if norm_link not in visited:
                    await frontier.put((depth + 1, norm_link))
                    
        logging.info(f"Dynamic frontier crawl completed. Visited {len(visited)} URLs.")
        return results
        
    except Exception as e:
        logging.error(f"Error in dynamic frontier crawl: {e}")
        return results
    finally:
        await checker.close()

async def discover_links(url: str, session: aiohttp.ClientSession, user_agent: str) -> List[str]:
    """
    Discover links from a page.
    """
    out = []
    headers = {"User-Agent": user_agent}
    try:
        async with session.get(url, headers=headers, ssl=False, allow_redirects=False) as resp:
            if resp.status == 200 and resp.content_type and resp.content_type.startswith("text/html"):
                text = await resp.text(errors='replace')
                soup = BeautifulSoup(text, "lxml")
                for a in soup.find_all("a", href=True):
                    abs_link = urljoin(url, a["href"])
                    out.append(abs_link)
    except Exception as e:
        logging.error(f"Error discovering links from {url}: {e}")
    return out

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
        results = await dynamic_frontier_crawl(
            seed_url=seed_url.strip(),
            checker=checker,
            include_regex=include_pattern,
            exclude_regex=exclude_pattern,
            show_partial_callback=show_partial_callback
        )
        
        # Recrawl failed URLs if any
        if checker.failed_urls:
            recrawl_results = await checker.recrawl_failed_urls()
            results.extend(recrawl_results)
        
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

def main():
    st.set_page_config(layout="wide")
    st.title("Lazy Crawler - Dynamic Frontier Mode")

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
    mode = st.radio("Select Mode", ["Dynamic Frontier", "List", "Sitemap"], horizontal=True)
    st.write("----")

    # Add copy to clipboard button
    def copy_to_clipboard(df):
        csv_data = df.to_csv(index=False)
        pyperclip.copy(csv_data)
        st.success("Data copied to clipboard! You can now paste it into Google Sheets.")

    if mode == "Dynamic Frontier":
        st.subheader("Dynamic Frontier Spider")
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

        if st.button("Start Dynamic Crawl"):
            if not seed_url.strip():
                st.warning("No seed URL provided.")
                return

            seeds = [seed_url.strip()] + sitemap_urls
            progress_ph = st.empty()
            progress_bar = st.progress(0.0)
            with st.expander("Intermediate Results", expanded=True):
                table_ph = st.empty()
            
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
                    table_ph.dataframe(df_temp, height=500, use_container_width=True)

            checker = URLChecker(user_agent, concurrency, DEFAULT_TIMEOUT, respect_robots)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                results = loop.run_until_complete(
                    run_dynamic_crawl(
                        seed_url=seed_url.strip(),
                        checker=checker,
                        include_pattern=include_pattern,
                        exclude_pattern=exclude_pattern,
                        show_partial_callback=show_partial_data
                    )
                )
            finally:
                loop.close()

            if not results:
                st.warning("No results from Dynamic Crawl.")
                return

            df = pd.DataFrame(results)
            st.subheader("Dynamic Frontier Crawl Results")
            st.dataframe(df, use_container_width=True)
            
            if st.button("Copy to Clipboard"):
                copy_to_clipboard(df)

            csv_data = df.to_csv(index=False).encode("utf-8")
            now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            st.download_button(
                label="Download CSV",
                data=csv_data,
                file_name=f"dynamic_{now_str}.csv",
                mime="text/csv"
            )
            show_summary(df)

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
            
            if st.button("Copy to Clipboard"):
                copy_to_clipboard(df)

            csv_data = df.to_csv(index=False).encode("utf-8")
            now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            st.download_button(
                label="Download CSV",
                data=csv_data,
                file_name=f"list_results_{now_str}.csv",
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
            
            if st.button("Copy to Clipboard"):
                copy_to_clipboard(df)

            csv_data = df.to_csv(index=False).encode("utf-8")
            now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            st.download_button(
                label="Download CSV",
                data=csv_data,
                file_name=f"sitemap_results_{now_str}.csv",
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
    display_distribution("Is_Blocked_by_Robots", "Blocked by Robots.txt?")
    display_distribution("Is_Indexable", "Indexable?")
    display_distribution("Indexability_Reason", "Indexability Reasons")

if __name__ == "__main__":
    main() 