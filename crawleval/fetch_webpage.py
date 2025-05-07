"""
Web Page Fetcher Module

This module provides functionality to fetch web pages using DrissionPage,
analyze their content, and save both the HTML and metadata for later evaluation.

Features:
- Fetches web pages with proper rendering of JavaScript content
- Extracts and analyzes metadata such as DOM structure, nesting levels, etc.
- Content deduplication using SHA-256 hashing with HTML normalization
- URL deduplication with URL normalization to avoid processing duplicates
- Saves HTML and metadata for future reference
- Batch processing of multiple URLs from a text file
- Parallel fetching with configurable number of workers
- Progress tracking with tqdm

Command-line usage:
  python -m crawleval.fetch_webpage --batch urls.txt [options]

Options:
  --dir DIR               Base directory for storing data
  --list-hashes           Display the content hash index
  --list-urls             Display the URL index
  --save-results FILE     Save batch processing results to a JSON file
  --workers N             Number of parallel workers for batch processing (default: 4)

Deduplication systems:
1. Content deduplication:
   - Computes a SHA-256 hash of the HTML content (with normalization)
   - Checks if the hash exists in the hash index
   - If found, returns the existing file ID instead of creating a duplicate
   - If not found, saves the content and registers its hash

2. URL deduplication:
   - Normalizes URLs to handle common variations (www prefix, trailing slashes, etc.)
   - Filters out common tracking parameters
   - Maintains a URL index that maps normalized URLs to file IDs
   - Checks if a URL has been processed before fetching content
   - In batch processing, automatically filters duplicate URLs from the input list

Parallel Processing:
The batch mode uses ThreadPoolExecutor to process multiple URLs concurrently,
which significantly speeds up data collection. Performance improvements depend on:
- Network bandwidth
- CPU resources
- Chrome browser instances
The optimal number of workers can be adjusted with the --workers option.

HTML normalization removes elements that often change between page loads but
don't represent meaningful content differences (scripts, styles, comments, etc.).

Batch processing format:
The URL file should contain one URL per line. Empty lines and lines starting
with # are ignored (useful for commenting out URLs).
"""

import argparse
import hashlib
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup, Comment, Declaration, Doctype
from DrissionPage import ChromiumOptions, ChromiumPage
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.theme import Theme

from crawleval.models import (
    BatchProcessingResults,
    PageData,
    PageMetadata,
    UrlProcessResult,
)

# Define comment types for HTML normalization
comment_types = (Comment, Doctype, Declaration)

# Create a Rich console with custom theme
custom_theme = Theme(
    {
        "info": "cyan",
        "warning": "yellow",
        "error": "bold red",
        "success": "green",
        "url": "blue underline",
    }
)
console = Console(theme=custom_theme)


# Task tracking for rich UI
class TaskState(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass
class TaskInfo:
    """Information about a task being processed"""

    url: str
    state: TaskState = TaskState.QUEUED
    progress: float = 0.0
    message: str = ""
    file_id: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    logs: List[str] = field(default_factory=list)

    def add_log(self, message: str, log_type: str = "info"):
        """Add a log message with timestamp"""
        timestamp = time.strftime("%H:%M:%S")
        log_entry = f"[dim][{timestamp}][/dim] [{log_type}]{message}[/{log_type}]"
        self.logs.append(log_entry)

    def get_recent_logs(self, count: int = 3) -> List[str]:
        """Get the most recent logs"""
        return self.logs[-count:] if self.logs else []


class TaskManager:
    """Manages task state for Rich UI display"""

    def __init__(self):
        self.tasks: Dict[str, TaskInfo] = {}
        self.lock = threading.Lock()

    def add_task(self, url: str) -> str:
        """Add a new task and return its ID (URL)"""
        with self.lock:
            self.tasks[url] = TaskInfo(url=url)
        return url

    def update_task(self, url: str, **kwargs):
        """Update a task's properties"""
        with self.lock:
            if url in self.tasks:
                task = self.tasks[url]
                for key, value in kwargs.items():
                    if hasattr(task, key):
                        setattr(task, key, value)

    def get_task(self, url: str) -> Optional[TaskInfo]:
        """Get a task by URL"""
        with self.lock:
            return self.tasks.get(url)

    def get_tasks_by_state(self, state: TaskState) -> List[TaskInfo]:
        """Get all tasks in a particular state"""
        with self.lock:
            return [task for task in self.tasks.values() if task.state == state]

    def get_all_tasks(self) -> List[TaskInfo]:
        """Get all tasks"""
        with self.lock:
            return list(self.tasks.values())

    def add_log(self, url: str, message: str, log_type: str = "info"):
        """Add a log message to a task"""
        with self.lock:
            if url in self.tasks:
                self.tasks[url].add_log(message, log_type)


class WebPageFetcher:
    def __init__(self, base_dir):
        """Initialize the fetcher with directory paths"""
        self.base_dir = Path(base_dir)
        self.html_dir = self.base_dir / "html"
        self.metadata_dir = self.base_dir / "metadata"
        self.screenshots_dir = self.base_dir / "screenshots"
        self.ground_truth_dir = self.base_dir / "ground_truth"
        self.ground_truth_patterns_dir = self.ground_truth_dir / "patterns"
        self.ground_truth_data_dir = self.ground_truth_dir / "data"
        self.hash_index_path = self.base_dir / "hash_index.json"
        self.url_index_path = self.base_dir / "url_index.json"

        # Create directories if they don't exist
        self.html_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.screenshots_dir.mkdir(parents=True, exist_ok=True)
        self.ground_truth_dir.mkdir(parents=True, exist_ok=True)
        self.ground_truth_patterns_dir.mkdir(parents=True, exist_ok=True)
        self.ground_truth_data_dir.mkdir(parents=True, exist_ok=True)

        # Initialize hash index
        self.hash_index = self._load_hash_index()

        # Initialize URL index
        self.url_index = self._load_url_index()

        # Locks for thread safety
        self.hash_index_lock = threading.Lock()
        self.url_index_lock = threading.Lock()
        self.file_id_lock = threading.Lock()

        # Task manager for UI
        self.task_manager = TaskManager()

    def _load_hash_index(self):
        """Load existing hash index or create a new one"""
        if self.hash_index_path.exists():
            try:
                with open(self.hash_index_path, "r") as f:
                    import json

                    return json.load(f)
            except Exception as e:
                print(f"Error loading hash index: {e}. Creating a new one.")
                return {}
        return {}

    def _save_hash_index(self):
        """Save hash index to disk"""
        try:
            with open(self.hash_index_path, "w") as f:
                import json

                json.dump(self.hash_index, f, indent=2)
        except Exception as e:
            print(f"Error saving hash index: {e}")

    def _load_url_index(self):
        """Load existing URL index or create a new one"""
        if self.url_index_path.exists():
            try:
                with open(self.url_index_path, "r") as f:
                    import json

                    return json.load(f)
            except Exception as e:
                print(f"Error loading URL index: {e}. Creating a new one.")
                return {}
        return {}

    def _save_url_index(self):
        """Save URL index to disk"""
        try:
            with open(self.url_index_path, "w") as f:
                import json

                json.dump(self.url_index, f, indent=2)
        except Exception as e:
            print(f"Error saving URL index: {e}")

    def _check_url_duplicate(self, url):
        """Check if URL already exists in the dataset

        Returns:
            str or None: The file ID of the duplicate if found, None otherwise
        """
        # Normalize URL to handle slight variations
        normalized_url = self._normalize_url(url)
        with self.url_index_lock:
            return self.url_index.get(normalized_url)

    def _register_url(self, url, file_id):
        """Register a URL in the index"""
        normalized_url = self._normalize_url(url)
        with self.url_index_lock:
            self.url_index[normalized_url] = file_id
            self._save_url_index()

    def _register_hash(self, html_content, file_id):
        """Register a new hash in the index"""
        content_hash = self._compute_html_hash(html_content)
        with self.hash_index_lock:
            self.hash_index[content_hash] = file_id
            self._save_hash_index()

    @staticmethod
    def _normalize_url(url):
        """Normalize URL to handle variations

        This helps identify the same URL despite minor variations:
        - Remove trailing slashes
        - Handle www. prefix
        - Standardize protocol (http/https)
        - Handle common URL parameters

        Returns:
            str: Normalized URL
        """
        try:
            from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

            # Parse URL
            parsed = urlparse(url)

            # Create normalized components
            scheme = parsed.scheme.lower()
            netloc = parsed.netloc.lower()

            # Remove 'www.' if present
            if netloc.startswith("www."):
                netloc = netloc[4:]

            # Ensure path doesn't end with trailing slash
            path = parsed.path
            if path.endswith("/") and len(path) > 1:
                path = path[:-1]

            # Handle empty path
            if not path:
                path = "/"

            # Filter out unnecessary query parameters (analytics, tracking, etc.)
            query_params = parse_qs(parsed.query)
            filtered_params = {}

            # List of parameters to exclude (tracking, session IDs, etc.)
            exclude_params = [
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_term",
                "utm_content",
                "fbclid",
                "gclid",
                "msclkid",
                "ref",
                "source",
                "session",
                "sid",
            ]

            for key, value in query_params.items():
                if key.lower() not in exclude_params:
                    filtered_params[key] = value

            # Reconstruct query string
            query = urlencode(filtered_params, doseq=True) if filtered_params else ""

            # Rebuild URL without fragment (hash)
            normalized = urlunparse((scheme, netloc, path, parsed.params, query, ""))

            return normalized

        except Exception as e:
            print(f"Error normalizing URL: {e}")
            # Fall back to original URL if normalization fails
            return url

    @staticmethod
    def _normalize_html(html_content):
        """Normalize HTML content to improve deduplication

        This removes or normalizes elements that might change between
        fetches but don't represent meaningful content differences:
        - Whitespace normalization
        - Remove dynamic timestamps, CSRF tokens, etc.
        - Standardize common variations

        Returns:
            str: Normalized HTML content
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")

            # Remove script tags (often contain dynamic content)
            for script in soup.find_all("script"):
                script.decompose()

            # Remove style tags
            for style in soup.find_all("style"):
                style.decompose()

            # Remove comments
            for comment in soup.find_all(
                string=lambda text: isinstance(text, comment_types)
            ):
                comment.extract()

            # Remove common analytics and tracking elements
            for element in soup.select(
                '[id*="analytics"], [class*="analytics"], [id*="tracking"], [class*="tracking"]'
            ):
                element.decompose()

            # Normalize the HTML structure
            normalized_html = str(soup)

            # Remove all whitespace between tags
            import re

            normalized_html = re.sub(r">\s+<", "><", normalized_html)

            return normalized_html
        except Exception as e:
            print(f"Error normalizing HTML: {e}")
            # Fall back to original content if normalization fails
            return html_content

    def _compute_html_hash(self, html_content):
        """Compute a SHA-256 hash of the HTML content

        Args:
            html_content (str): HTML content to hash

        Returns:
            str: The hexadecimal digest of the hash
        """
        content_to_hash = self._normalize_html(html_content)
        return hashlib.sha256(content_to_hash.encode("utf-8")).hexdigest()

    def _check_duplicate(self, html_content):
        """Check if HTML content already exists in the dataset

        Returns:
            str or None: The file ID of the duplicate if found, None otherwise
        """
        content_hash = self._compute_html_hash(html_content)
        with self.hash_index_lock:
            return self.hash_index.get(content_hash)

    def get_next_id(self):
        """Get the next available ID for files (in format '0001', '0002', etc.)"""
        with self.file_id_lock:
            html_files = list(self.html_dir.glob("*.html"))
            ids = [int(f.stem) for f in html_files if f.stem.isdigit()]
            return f"{max(ids + [0]) + 1:04d}" if ids else "0001"

    @staticmethod
    def setup_page():
        """Set up and return a DrissionPage ChromiumPage instance"""
        # Create options object with sensible defaults
        options = ChromiumOptions()

        # Set headless mode
        options.headless()

        # Set user agent
        options.set_user_agent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )

        # Add browser arguments for stability
        options.set_argument("--no-sandbox")
        options.set_argument("--disable-dev-shm-usage")
        options.set_argument("--disable-gpu")
        options.set_argument("--window-size=1920,1080")
        options.set_argument("--disable-extensions")

        try:
            # Create page with options
            page = ChromiumPage(options)
            return page
        except Exception as e:
            print(f"Failed to create browser: {e}")
            print("Make sure Chrome and DrissionPage are installed properly.")
            return None

    def fetch_html(self, url: str, task_url: str) -> PageData | None:
        """Fetch HTML content from a URL using DrissionPage

        Args:
            url: The URL to fetch
            task_url: The original URL for task tracking
        """
        page = None
        try:
            # Create a fresh browser instance
            page = self.setup_page()
            if not page:
                self.task_manager.add_log(task_url, "Failed to create browser", "error")
                return None

            self.task_manager.update_task(task_url, progress=0.1)
            self.task_manager.add_log(task_url, f"Navigating to {url}...", "info")
            page.get(url)
            self.task_manager.update_task(task_url, progress=0.3)

            # Wait for page to load using dynamic waiting strategy
            self.task_manager.add_log(
                task_url, "Waiting for page to fully render...", "info"
            )
            self.task_manager.update_task(task_url, progress=0.4)
            try:
                # Wait for document ready state to be complete
                page.wait.doc_loaded(timeout=10)
                self.task_manager.update_task(task_url, progress=0.6)
            except Exception as e:
                self.task_manager.add_log(
                    task_url, f"Warning: Wait condition timed out: {e}", "warning"
                )
                self.task_manager.add_log(
                    task_url,
                    "Continuing with potentially incomplete page...",
                    "warning",
                )

            # Get page source after JavaScript execution
            html_content = page.html
            self.task_manager.update_task(task_url, progress=0.7)

            # Take a screenshot of the page
            self.task_manager.add_log(task_url, "Taking screenshot...", "info")
            screenshot_data = page.get_screenshot(as_bytes=True)
            self.task_manager.update_task(task_url, progress=0.8)

            # Additional page metadata
            page_title = page.title
            current_url = page.url  # In case of redirects
            self.task_manager.update_task(task_url, progress=0.9)

            page_data = PageData(
                html=html_content,
                title=page_title,
                url=current_url,
                screenshot=screenshot_data,
            )

            self.task_manager.update_task(task_url, progress=1.0)
            return page_data
        except Exception as e:
            self.task_manager.add_log(
                task_url, f"Error fetching URL {url}: {e}", "error"
            )
            return None
        finally:
            # Always close the browser when done
            if page:
                try:
                    page.quit()
                except Exception:
                    self.task_manager.add_log(
                        task_url, "Warning: Failed to close browser properly", "warning"
                    )

    def analyze_html(self, page_data: PageData) -> PageMetadata:
        """Analyze HTML to extract metadata"""
        html_content = page_data.html
        soup = BeautifulSoup(html_content, "html.parser")

        # Extract basic metadata (use DrissionPage title if available)
        title = (
            page_data.title
            if page_data.title
            else (soup.title.text if soup.title else "Unknown Page")
        )

        # Count DOM nodes
        dom_nodes = len(soup.find_all())

        # Count text nodes, excluding script and style content
        text_nodes = len(
            [
                node
                for node in soup.find_all(string=True)
                if node.parent.name not in ["script", "style"]
                and not isinstance(node, comment_types)
            ]
        )

        # Count link nodes
        link_nodes = len(soup.find_all("a"))

        # Count image nodes
        image_nodes = len(soup.find_all("img"))

        # Calculate max nesting level
        def get_nesting_level(tag, level=0):
            if not tag or not hasattr(tag, "contents"):
                return level
            if not tag.contents:
                return level
            return max(
                [
                    get_nesting_level(child, level + 1)
                    for child in tag.contents
                    if hasattr(child, "contents")
                ]
                or [level]
            )

        max_nesting_level = get_nesting_level(soup)

        # Generate metadata
        url = page_data.url
        metadata = PageMetadata(
            id=self.get_next_id(),
            name=title[:50],  # Truncate long titles
            description=f"Content from {urlparse(url).netloc}",
            url=url,
            size=len(html_content),
            dom_nodes=dom_nodes,
            text_nodes=text_nodes,
            link_nodes=link_nodes,
            image_nodes=image_nodes,
            max_nesting_level=max_nesting_level,
        )

        return metadata

    def save_files(self, page_data: PageData, metadata: PageMetadata, task_url: str):
        """Save HTML content, screenshot, and metadata files"""
        html_content = page_data.html
        screenshot_data = page_data.screenshot
        file_id = metadata.id

        # Save HTML file
        html_path = self.html_dir / f"{file_id}.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        # Save screenshot file (if available)
        screenshot_path = None
        if screenshot_data:
            screenshot_path = self.screenshots_dir / f"{file_id}.png"
            with open(screenshot_path, "wb") as f:
                f.write(screenshot_data)

        # Save metadata file
        metadata_path = self.metadata_dir / f"{file_id}.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            f.write(metadata.model_dump_json(indent=2))

        self.task_manager.add_log(task_url, "Files created successfully:", "success")
        self.task_manager.add_log(task_url, f"  HTML: {html_path}", "info")
        if screenshot_path:
            self.task_manager.add_log(
                task_url, f"  Screenshot: {screenshot_path}", "info"
            )
        self.task_manager.add_log(task_url, f"  Metadata: {metadata_path}", "info")

        return file_id

    def list_hashes(self):
        """List all content hashes and their corresponding file IDs"""
        from rich.table import Table

        if not self.hash_index:
            console.print(
                "Hash index is empty. No pages have been processed yet.",
                style="warning",
            )
            return

        table = Table(title=f"Content Hash Index ({len(self.hash_index)} entries)")
        table.add_column("Content Hash (SHA-256)", style="cyan", no_wrap=True)
        table.add_column("File ID", style="green")
        table.add_column("URL", style="blue")

        for hash_value, file_id in sorted(self.hash_index.items(), key=lambda x: x[1]):
            # Try to get URL from metadata
            metadata_path = self.metadata_dir / f"{file_id}.json"
            url = "Unknown"
            if metadata_path.exists():
                try:
                    with open(metadata_path, "r") as f:
                        import json

                        metadata = json.load(f)
                        url = metadata.get("url", "Unknown")
                        # Truncate long URLs
                        if len(url) > 40:
                            url = url[:37] + "..."
                except Exception:
                    pass

            table.add_row(hash_value, file_id, url)

        console.print(table)

    def list_urls(self):
        """List all URLs and their corresponding file IDs"""
        from rich.table import Table

        if not self.url_index:
            console.print(
                "URL index is empty. No URLs have been processed yet.", style="warning"
            )
            return

        table = Table(title=f"URL Index ({len(self.url_index)} entries)")
        table.add_column("URL", style="blue", no_wrap=True)
        table.add_column("File ID", style="green")
        table.add_column("Status", style="cyan")

        for normalized_url, file_id in sorted(
            self.url_index.items(), key=lambda x: x[1]
        ):
            # Check if file exists to determine status
            html_file = self.html_dir / f"{file_id}.html"
            status = "✅ Available" if html_file.exists() else "❌ Missing"

            # Truncate long URLs
            display_url = normalized_url
            if len(display_url) > 70:
                display_url = display_url[:67] + "..."

            table.add_row(display_url, file_id, status)

        console.print(table)

    def process_single_url(self, url: str) -> UrlProcessResult:
        """Process a single URL and return the result"""
        task_url = url
        self.task_manager.add_task(task_url)
        self.task_manager.update_task(
            task_url, state=TaskState.RUNNING, started_at=time.time()
        )

        result = UrlProcessResult(url=url, status="unknown")
        retry_count = 0
        max_retries = 2  # Allow up to 2 retries per URL

        while retry_count <= max_retries:
            try:
                # First check URL duplication (we already filtered, but double-check)
                url_duplicate_id = self._check_url_duplicate(url)
                if url_duplicate_id:
                    result.status = "duplicate_url"
                    result.file_id = url_duplicate_id
                    result.is_new = False
                    self.task_manager.update_task(
                        task_url,
                        state=TaskState.COMPLETE,
                        file_id=url_duplicate_id,
                        progress=1.0,
                        message="Duplicate URL detected",
                        completed_at=time.time(),
                    )
                    return result

                # Fetch the content with a fresh browser for each URL
                page_data = self.fetch_html(url, task_url)
                if not page_data:
                    if retry_count < max_retries:
                        retry_count += 1
                        self.task_manager.add_log(
                            task_url,
                            f"Retrying {url} (attempt {retry_count}/{max_retries})",
                            "warning",
                        )
                        continue
                    result.status = "failed"
                    self.task_manager.update_task(
                        task_url,
                        state=TaskState.FAILED,
                        progress=1.0,
                        message="Failed to fetch content",
                        completed_at=time.time(),
                    )
                    return result

                # Check content duplication
                content_duplicate_id = self._check_duplicate(page_data.html)
                if content_duplicate_id:
                    # Use a lock to avoid race conditions when updating shared data
                    self._register_url(url, content_duplicate_id)
                    result.status = "duplicate_content"
                    result.file_id = content_duplicate_id
                    result.is_new = False
                    self.task_manager.update_task(
                        task_url,
                        state=TaskState.COMPLETE,
                        file_id=content_duplicate_id,
                        progress=1.0,
                        message="Duplicate content detected",
                        completed_at=time.time(),
                    )
                    return result

                # Process as new content
                metadata = self.analyze_html(page_data)
                file_id = self.save_files(page_data, metadata, task_url)

                # Use locks to prevent race conditions when updating shared data
                self._register_hash(page_data.html, file_id)
                self._register_url(url, file_id)

                result.status = "success"
                result.file_id = file_id
                result.is_new = True
                self.task_manager.update_task(
                    task_url,
                    state=TaskState.COMPLETE,
                    file_id=file_id,
                    progress=1.0,
                    message="Successfully processed",
                    completed_at=time.time(),
                )
                return result

            except Exception as e:
                if retry_count < max_retries:
                    retry_count += 1
                    self.task_manager.add_log(
                        task_url, f"Error processing {url}: {e}", "error"
                    )
                    self.task_manager.add_log(
                        task_url,
                        f"Retrying (attempt {retry_count}/{max_retries})",
                        "warning",
                    )
                else:
                    result.status = "error"
                    result.error = str(e)
                    self.task_manager.update_task(
                        task_url,
                        state=TaskState.FAILED,
                        progress=1.0,
                        message=f"Error: {str(e)}",
                        completed_at=time.time(),
                    )
                    return result

        # If we reached this point after all retries, return the failure result
        if result.status == "unknown":
            result.status = "error"
        return result

    # Process each URL in parallel
    def batch_process_urls(
        self, url_file_path: str, max_workers=4
    ) -> BatchProcessingResults:
        """Process multiple URLs from a text file in parallel

        Args:
            url_file_path (str): Path to text file with URLs (one per line)
            max_workers (int): Maximum number of parallel workers

        Returns:
            BatchProcessingResults: Summary of processing results
        """
        if not os.path.exists(url_file_path):
            console.print(f"Error: URL file not found: {url_file_path}", style="error")
            return BatchProcessingResults()

        results = BatchProcessingResults()

        try:
            # Load URLs from file
            with open(url_file_path, "r", encoding="utf-8") as f:
                urls = [
                    line.strip()
                    for line in f
                    if line.strip() and not line.strip().startswith("#")
                ]

            # Initial filtering of duplicate URLs
            unique_urls = []
            url_map = {}  # Normalized URL -> Original URL
            duplicates = []

            for url in urls:
                normalized = self._normalize_url(url)

                # Check if already in our batch
                if normalized in url_map:
                    duplicates.append(url)
                    continue

                # Check if already in the URL index
                if self._check_url_duplicate(url):
                    duplicates.append(url)
                    continue

                url_map[normalized] = url
                unique_urls.append(url)

            if duplicates:
                console.print(
                    f"Skipping {len(duplicates)} duplicate URLs", style="warning"
                )
                if len(duplicates) <= 5:  # Show if just a few
                    for dup in duplicates:
                        console.print(f"  - [url]{dup}[/url]", style="info")

            urls = unique_urls

            results.total = len(urls)
            if not urls:
                console.print(
                    "No URLs to process after filtering duplicates.", style="warning"
                )
                return results

            console.print(
                f"Starting batch processing of {len(urls)} URLs with {max_workers} workers...",
                style="info",
            )

            # Process URLs in parallel with rich live display
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks to the executor
                futures = {
                    executor.submit(self.process_single_url, url): url for url in urls
                }

                # Create a live display for real-time updates
                with Live(self._generate_live_display(), refresh_per_second=4) as live:
                    for future in as_completed(futures):
                        url = futures[future]
                        try:
                            result = future.result()
                            results.entries.append(result)

                            # Update statistics
                            if result.status == "success" and result.is_new:
                                results.successful += 1
                                results.new_entries += 1
                            elif result.status == "duplicate_url":
                                results.successful += 1
                                results.duplicate_urls += 1
                            elif result.status == "duplicate_content":
                                results.successful += 1
                                results.duplicate_content += 1
                            elif result.status in ["failed", "error"]:
                                results.failed += 1

                            # Update the live display
                            live.update(self._generate_live_display())

                        except Exception as e:
                            results.failed += 1
                            results.entries.append(
                                UrlProcessResult(url=url, status="error", error=str(e))
                            )
                            # Update the live display
                            live.update(self._generate_live_display())

            # Print summary with rich tables
            from rich.table import Table

            console.print("\n[bold cyan]Batch processing completed[/bold cyan]")

            # Create summary table
            table = Table(title="Processing Results Summary")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green", justify="right")

            table.add_row("Total URLs", str(results.total))
            table.add_row("Successful", str(results.successful))
            table.add_row("Failed", str(results.failed))
            table.add_row("New entries", str(results.new_entries))
            table.add_row("Duplicate URLs", str(results.duplicate_urls))
            table.add_row("Duplicate content", str(results.duplicate_content))

            console.print(table)

        except Exception as e:
            console.print(f"Error during batch processing: {e}", style="error")

        return results

    def _generate_live_display(self):
        """Generate a rich layout for the live display"""
        # Create the main layout
        layout = Layout()

        # Get tasks in different states
        running_tasks = self.task_manager.get_tasks_by_state(TaskState.RUNNING)
        completed_tasks = self.task_manager.get_tasks_by_state(TaskState.COMPLETE)
        failed_tasks = self.task_manager.get_tasks_by_state(TaskState.FAILED)
        queued_tasks = self.task_manager.get_tasks_by_state(TaskState.QUEUED)

        # Add overall stats
        all_tasks = self.task_manager.get_all_tasks()
        total = len(all_tasks)
        running = len(running_tasks)
        completed = len(completed_tasks)
        failed = len(failed_tasks)
        queued = len(queued_tasks)

        stats_panel = Panel(
            f"[bold]Total:[/bold] {total} | "
            f"[bold blue]Running:[/bold blue] {running} | "
            f"[bold green]Completed:[/bold green] {completed} | "
            f"[bold red]Failed:[/bold red] {failed} | "
            f"[bold yellow]Queued:[/bold yellow] {queued}",
            title="Processing Statistics",
            border_style="cyan",
            padding=(0, 1),
        )

        # Create progress bars for running tasks
        if running_tasks:
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(bar_width=None),
                TaskProgressColumn(),
                TimeRemainingColumn(),
            )

            # Add a task for each running URL
            task_ids = {}
            for task in running_tasks:
                task_desc = f"{task.url}"
                task_id = progress.add_task(
                    task_desc, total=1.0, completed=task.progress
                )
                task_ids[task.url] = task_id

            # Update progress for existing tasks
            for url, task_id in task_ids.items():
                task_info = self.task_manager.get_task(url)
                if task_info:
                    progress.update(task_id, completed=task_info.progress)

            running_panel = Panel(
                progress,
                title=f"Running Tasks ({len(running_tasks)})",
                border_style="blue",
                padding=(0, 1),
            )
        else:
            running_panel = Panel(
                "[dim]No tasks currently running[/dim]",
                title="Running Tasks (0)",
                border_style="blue",
                padding=(0, 1),
            )

        # Create a table for the most recently completed tasks (show last 5)
        recent_completed = sorted(
            completed_tasks + failed_tasks,
            key=lambda t: t.completed_at or 0,
            reverse=True,
        )[:5]

        if recent_completed:
            completed_table = Table(
                show_header=True, header_style="bold", box=None, padding=0
            )
            completed_table.add_column("URL", style="cyan", no_wrap=True)
            completed_table.add_column(
                "Status", style="green", justify="center", width=12
            )
            completed_table.add_column(
                "File ID", style="cyan", justify="center", width=8
            )
            completed_table.add_column(
                "Duration", style="cyan", justify="right", width=8
            )

            for task in recent_completed:
                duration = "Unknown"
                if task.started_at and task.completed_at:
                    duration = f"{task.completed_at - task.started_at:.1f}s"

                # Truncate long URLs
                display_url = task.url
                if len(display_url) > 50:
                    display_url = display_url[:47] + "..."

                status_style = "green" if task.state == TaskState.COMPLETE else "red"
                status = (
                    "✅ Complete" if task.state == TaskState.COMPLETE else "❌ Failed"
                )

                completed_table.add_row(
                    display_url,
                    f"[{status_style}]{status}[/{status_style}]",
                    task.file_id or "N/A",
                    duration,
                )

            completed_panel = Panel(
                completed_table,
                title=f"Recent Completed Tasks ({len(completed_tasks)} complete, {len(failed_tasks)} failed)",
                border_style="green",
                padding=(0, 1),
            )
        else:
            completed_panel = Panel(
                "[dim]No tasks completed yet[/dim]",
                title="Recent Completed Tasks (0)",
                border_style="green",
                padding=(0, 1),
            )

        # Task details panel (most recent logs for running tasks)
        if running_tasks:
            # Get the first running task for detailed view
            current_task = running_tasks[0]
            logs = current_task.get_recent_logs(5)  # Show up to 5 recent logs

            if logs:
                log_text = "\n".join(logs)
                logs_panel = Panel(
                    log_text,
                    title=f"Task Logs: {current_task.url}",
                    border_style="yellow",
                    padding=(0, 1),
                )
            else:
                logs_panel = Panel(
                    "[dim]No logs yet[/dim]",
                    title=f"Task Logs: {current_task.url}",
                    border_style="yellow",
                    padding=(0, 1),
                )
        else:
            logs_panel = Panel(
                "[dim]No active tasks[/dim]",
                title="Task Logs",
                border_style="yellow",
                padding=(0, 1),
            )

        # Arrange panels in the layout
        layout.split(Layout(name="stats", size=3), Layout(name="main"))

        layout["stats"].update(stats_panel)

        layout["main"].split_column(
            Layout(name="running", size=3),
            Layout(name="completed", size=7),
            Layout(name="logs", size=7),
        )

        layout["running"].update(running_panel)
        layout["completed"].update(completed_panel)
        layout["logs"].update(logs_panel)

        return layout


def main():
    parser = argparse.ArgumentParser(
        description="Fetch website content in batch mode using DrissionPage and generate metadata"
    )
    parser.add_argument(
        "--batch",
        metavar="URL_FILE",
        required=True,
        help="Process multiple URLs from a text file (one URL per line)",
    )
    parser.add_argument(
        "--dir",
        default=os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "..", "eval_data_extract"
        ),
        help="Base directory (default: <project_root>/eval_data_extract)",
    )
    parser.add_argument(
        "--list-hashes",
        action="store_true",
        help="List all content hashes and their file IDs",
    )
    parser.add_argument(
        "--list-urls",
        action="store_true",
        help="List all processed URLs and their file IDs",
    )
    parser.add_argument(
        "--save-results",
        metavar="RESULTS_FILE",
        help="Save batch processing results to a JSON file",
    )
    parser.add_argument(
        "--workers",
        metavar="N",
        type=int,
        default=4,
        help="Number of parallel workers for batch processing (default: 4)",
    )

    args = parser.parse_args()
    fetcher = WebPageFetcher(args.dir)

    # Display Rich banner
    from rich.panel import Panel

    console.print(
        Panel.fit(
            "[bold cyan]Web Page Fetcher[/bold cyan]\n"
            "Fetch website content in batch mode and generate metadata",
            title="🌐 CrawlEval",
            border_style="green",
        )
    )

    # Handle list-hashes command
    if args.list_hashes:
        fetcher.list_hashes()
        return

    # Handle list-urls command
    if args.list_urls:
        fetcher.list_urls()
        return

    # Process in batch mode
    console.print(
        f"[info]Starting batch processing from file: [/info][url]{args.batch}[/url]"
    )
    console.print(f"[info]Using [bold]{args.workers}[/bold] workers[/info]")
    console.print(f"[info]Saving to directory: [/info][cyan]{args.dir}[/cyan]")

    results = fetcher.batch_process_urls(
        args.batch,
        max_workers=args.workers,
    )

    # Save results if requested
    if args.save_results and results:
        try:
            import json

            with open(args.save_results, "w", encoding="utf-8") as f:
                json.dump(results.model_dump(), f, indent=2)
            console.print(
                f"Results saved to [cyan]{args.save_results}[/cyan]", style="success"
            )
        except Exception as e:
            console.print(f"Error saving results: {e}", style="error")


if __name__ == "__main__":
    main()
