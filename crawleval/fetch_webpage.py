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
import re
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
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, TaskID
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
    progress: int = 0
    message: str = ""
    file_id: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    logs: List[str] = field(default_factory=list)
    log_file: Optional[str] = None
    progress_task_id: Optional[TaskID] = None

    def add_log(self, message: str, log_type: str = "info"):
        """Add a log message with timestamp"""
        timestamp = time.strftime("%H:%M:%S")
        log_entry = f"[dim][{timestamp}][/dim] [{log_type}]{message}[/{log_type}]"
        self.logs.append(log_entry)

        # Also write to log file if available
        if self.log_file and os.path.exists(os.path.dirname(self.log_file)):
            try:
                with open(self.log_file, "a", encoding="utf-8") as f:
                    # Strip rich formatting for file logs
                    clean_message = re.sub(r"\[.*?\]", "", log_entry)
                    f.write(f"{clean_message}\n")
            except Exception as e:
                # Don't let logging errors affect the main process
                print(f"Error writing to log file: {e}")

    def get_recent_logs(self, count: int = 3) -> List[str]:
        """Get the most recent logs"""
        return self.logs[-count:] if self.logs else []


class TaskManager:
    """Manages task state for Rich UI display"""

    def __init__(self, logs_dir=None):
        self.tasks: Dict[str, TaskInfo] = {}
        self.lock = threading.Lock()
        self.logs_dir = logs_dir

        # Create logs directory if specified
        if logs_dir:
            os.makedirs(logs_dir, exist_ok=True)

    def add_task(self, url: str) -> str:
        """Add a new task and return its ID (URL)"""
        with self.lock:
            task = TaskInfo(url=url)

            # Set up log file if logs directory is available
            if self.logs_dir:
                # Create a safe filename from the URL
                safe_filename = self._get_safe_filename(url)
                log_file = os.path.join(self.logs_dir, f"{safe_filename}.log")
                task.log_file = log_file

                # Initialize the log file
                try:
                    with open(log_file, "w", encoding="utf-8") as f:
                        f.write(f"=== Task log for URL: {url} ===\n")
                        f.write(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                except Exception as e:
                    print(f"Error creating log file: {e}")

            self.tasks[url] = task
        return url

    @staticmethod
    def _get_safe_filename(url: str) -> str:
        """Convert URL to a safe filename"""
        # Remove protocol and special characters
        safe_name = re.sub(r"^https?://", "", url)
        safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", safe_name)
        # Trim if too long
        if len(safe_name) > 100:
            safe_name = safe_name[:97] + "..."
        return safe_name

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
        self.logs_dir = self.base_dir / "task_logs"

        # Create directories if they don't exist
        self.html_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.screenshots_dir.mkdir(parents=True, exist_ok=True)
        self.ground_truth_dir.mkdir(parents=True, exist_ok=True)
        self.ground_truth_patterns_dir.mkdir(parents=True, exist_ok=True)
        self.ground_truth_data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        # Initialize hash index
        self.hash_index = self._load_hash_index()

        # Initialize URL index
        self.url_index = self._load_url_index()

        # Locks for thread safety
        self.hash_index_lock = threading.Lock()
        self.url_index_lock = threading.Lock()
        self.file_id_lock = threading.Lock()

        # Task manager for UI
        self.task_manager = TaskManager(logs_dir=self.logs_dir)

        # ID for the current file being processed
        self.current_id = None

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
            # Check if we already have a current ID
            if not self.current_id:
                html_files = list(self.html_dir.glob("*.html"))
                metadata_files = list(self.metadata_dir.glob("*.json"))
                screenshot_files = list(self.screenshots_dir.glob("*.png"))

                # Combine all file IDs from all directories
                all_files = html_files + metadata_files + screenshot_files
                ids = [int(f.stem) for f in all_files if f.stem.isdigit()]
                self.current_id = max(ids)

            # Increment the ID for the next file
            self.current_id += 1

            return f"{self.current_id:04d}"

    def _save_files_internal(
        self, page_data: PageData, metadata: PageMetadata, url: str
    ):
        """Internal method to save files using pre-generated file ID"""
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

        self.task_manager.add_log(url, "Files created successfully:", "success")
        self.task_manager.add_log(url, f"  HTML: {html_path}", "info")
        if screenshot_path:
            self.task_manager.add_log(url, f"  Screenshot: {screenshot_path}", "info")
        self.task_manager.add_log(url, f"  Metadata: {metadata_path}", "info")

        return file_id

    @staticmethod
    def analyze_html(page_data: PageData, file_id: str) -> PageMetadata:
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
            id=file_id,
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

    def list_hashes(self):
        """List all content hashes and their corresponding file IDs"""

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

    def _process_single_url(self, url: str) -> UrlProcessResult:
        """Process a single URL and return the result"""
        self.task_manager.update_task(
            url, state=TaskState.RUNNING, started_at=time.time()
        )
        self.task_manager.add_log(url, f"Starting to process URL: {url}", "info")

        result = UrlProcessResult(url=url, status="unknown")
        retry_count = 0
        max_retries = 2  # Allow up to 2 retries per URL

        while retry_count <= max_retries:
            try:
                # First check URL duplication (we already filtered, but double-check)
                url_duplicate_id = self._check_url_duplicate(url)
                if url_duplicate_id:
                    self.task_manager.add_log(
                        url, f"Found duplicate URL with ID: {url_duplicate_id}", "info"
                    )
                    result.status = "duplicate_url"
                    result.file_id = url_duplicate_id
                    result.is_new = False
                    self.task_manager.update_task(
                        url,
                        state=TaskState.COMPLETE,
                        file_id=url_duplicate_id,
                        progress=1.0,
                        message="Duplicate URL detected",
                        completed_at=time.time(),
                    )
                    self.task_manager.add_log(
                        url, "Task completed as duplicate URL", "success"
                    )
                    return result

                # Fetch the content with a fresh browser for each URL
                self.task_manager.add_log(url, "Fetching HTML content...", "info")
                page_data = self.fetch_html(url)
                if not page_data:
                    if retry_count < max_retries:
                        retry_count += 1
                        self.task_manager.add_log(
                            url,
                            f"Retrying {url} (attempt {retry_count}/{max_retries})",
                            "warning",
                        )
                        continue
                    result.status = "failed"
                    self.task_manager.update_task(
                        url,
                        state=TaskState.FAILED,
                        progress=1.0,
                        message="Failed to fetch content",
                        completed_at=time.time(),
                    )
                    self.task_manager.add_log(
                        url, "Task failed: Could not fetch content", "error"
                    )
                    return result

                # Check content duplication
                self.task_manager.add_log(
                    url, "Checking for duplicate content...", "info"
                )
                content_duplicate_id = self._check_duplicate(page_data.html)
                if content_duplicate_id:
                    self.task_manager.add_log(
                        url,
                        f"Found duplicate content with ID: {content_duplicate_id}",
                        "info",
                    )
                    # Use a lock to avoid race conditions when updating shared data
                    self._register_url(url, content_duplicate_id)
                    result.status = "duplicate_content"
                    result.file_id = content_duplicate_id
                    result.is_new = False
                    self.task_manager.update_task(
                        url,
                        state=TaskState.COMPLETE,
                        file_id=content_duplicate_id,
                        progress=1.0,
                        message="Duplicate content detected",
                        completed_at=time.time(),
                    )
                    self.task_manager.add_log(
                        url, "Task completed as duplicate content", "success"
                    )
                    return result

                # Process as new content
                self.task_manager.add_log(
                    url, "Analyzing and saving content...", "info"
                )
                metadata, file_id = self.analyze_and_save_content(page_data, url)
                self.task_manager.add_log(
                    url, f"Content saved with ID: {file_id}", "success"
                )

                # Use locks to prevent race conditions when updating shared data
                self._register_hash(page_data.html, file_id)
                self._register_url(url, file_id)
                self.task_manager.add_log(
                    url, "URL and hash registered in indexes", "info"
                )

                result.status = "success"
                result.file_id = file_id
                result.is_new = True
                self.task_manager.update_task(
                    url,
                    state=TaskState.COMPLETE,
                    file_id=file_id,
                    progress=1.0,
                    message="Successfully processed",
                    completed_at=time.time(),
                )
                self.task_manager.add_log(url, "Task completed successfully", "success")
                return result

            except Exception as e:
                self.task_manager.add_log(url, f"Error processing {url}: {e}", "error")
                if retry_count < max_retries:
                    retry_count += 1
                    self.task_manager.add_log(
                        url,
                        f"Retrying (attempt {retry_count}/{max_retries})",
                        "warning",
                    )
                else:
                    result.status = "error"
                    result.error = str(e)
                    self.task_manager.update_task(
                        url,
                        state=TaskState.FAILED,
                        progress=1.0,
                        message=f"Error: {str(e)}",
                        completed_at=time.time(),
                    )
                    self.task_manager.add_log(
                        url, "Task failed after maximum retries", "error"
                    )
                    return result

        # If we reached this point after all retries, return the failure result
        if result.status == "unknown":
            result.status = "error"
        return result

    def fetch_html(self, url: str) -> PageData | None:
        """Fetch HTML content from a URL using DrissionPage

        Args:
            url: The URL to fetch
        """
        page = None
        try:
            # Create a fresh browser instance
            page = self.setup_page()
            if not page:
                self.task_manager.add_log(url, "Failed to create browser", "error")
                return None

            self.task_manager.update_task(url, progress=10)
            self.task_manager.add_log(url, f"Navigating to {url}...", "info")
            page.get(url)
            self.task_manager.update_task(url, progress=30)

            # Wait for page to load using dynamic waiting strategy
            self.task_manager.add_log(
                url, "Waiting for page to fully render...", "info"
            )
            self.task_manager.update_task(url, progress=40)
            try:
                # Wait for document ready state to be complete
                page.wait.doc_loaded(timeout=10)
                self.task_manager.update_task(url, progress=60)
            except Exception as e:
                self.task_manager.add_log(
                    url, f"Warning: Wait condition timed out: {e}", "warning"
                )
                self.task_manager.add_log(
                    url,
                    "Continuing with potentially incomplete page...",
                    "warning",
                )

            # Get page source after JavaScript execution
            html_content = page.html
            self.task_manager.update_task(url, progress=70)

            # Take a screenshot of the page
            self.task_manager.add_log(url, "Taking screenshot...", "info")
            screenshot_data = page.get_screenshot(as_bytes=True)
            self.task_manager.update_task(url, progress=0.8)

            # Additional page metadata
            page_title = page.title
            current_url = page.url  # In case of redirects
            self.task_manager.update_task(url, progress=90)

            page_data = PageData(
                html=html_content,
                title=page_title,
                url=current_url,
                screenshot=screenshot_data,
            )

            self.task_manager.update_task(url, progress=100)
            return page_data
        except Exception as e:
            self.task_manager.add_log(url, f"Error fetching URL {url}: {e}", "error")
            return None
        finally:
            # Always close the browser when done
            if page:
                try:
                    page.quit()
                except Exception:
                    self.task_manager.add_log(
                        url, "Warning: Failed to close browser properly", "warning"
                    )

    def analyze_and_save_content(
        self, page_data: PageData, url: str
    ) -> tuple[PageMetadata, str]:
        """Analyze HTML and save files atomically with a single file ID

        This method ensures that analysis and file saving use the same ID
        to prevent race conditions in parallel processing.

        Returns:
            tuple: (metadata, file_id)
        """
        # Generate the file ID first, under the lock
        file_id = self.get_next_id()

        # Analyze HTML
        metadata = self.analyze_html(page_data, file_id)

        # Ensure the metadata uses our pre-generated ID
        metadata.id = file_id

        # Save files with this ID
        self._save_files_internal(page_data, metadata, url)

        return metadata, file_id

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
                running_progress = Progress()
                # Set up the live display
                with Live(
                    self._generate_live_display(running_progress), refresh_per_second=4
                ) as live:
                    # Add all tasks to the task manager
                    for url in urls:
                        self.task_manager.add_task(url)

                    # Keep track of running futures
                    futures = {}
                    completed_urls = set()

                    # Submit all tasks to the executor
                    for url in urls:
                        futures[executor.submit(self._process_single_url, url)] = url

                    # Create a flag and function for the background updater
                    update_display = True

                    def ui_updater():
                        while update_display:
                            live.update(self._generate_live_display(running_progress))
                            time.sleep(0.25)  # Update 4 times per second

                    # Start background thread for UI updates
                    updater_thread = threading.Thread(target=ui_updater)
                    updater_thread.daemon = True
                    updater_thread.start()

                    try:
                        # Process results as they complete
                        for future in as_completed(futures):
                            url = futures[future]
                            completed_urls.add(url)

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

                            except Exception as e:
                                results.failed += 1
                                results.entries.append(
                                    UrlProcessResult(
                                        url=url, status="error", error=str(e)
                                    )
                                )
                    finally:
                        # Stop the background updater thread
                        update_display = False
                        if updater_thread.is_alive():
                            updater_thread.join(timeout=1.0)

                        # Final update of the display
                        live.update(self._generate_live_display(running_progress))

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

    def _generate_live_display(self, running_progress: Progress):
        """Generate a rich layout for the live display with a combined panel"""
        # Create the main layout

        # Get tasks in different states
        running_tasks = self.task_manager.get_tasks_by_state(TaskState.RUNNING)
        completed_tasks = self.task_manager.get_tasks_by_state(TaskState.COMPLETE)
        failed_tasks = self.task_manager.get_tasks_by_state(TaskState.FAILED)
        queued_tasks = self.task_manager.get_tasks_by_state(TaskState.QUEUED)

        # Calculate stats
        all_tasks = self.task_manager.get_all_tasks()
        total = len(all_tasks)
        running = len(running_tasks)
        completed = len(completed_tasks)
        failed = len(failed_tasks)
        queued = len(queued_tasks)

        stats_line = (
            f"[bold]Total:[/bold] {total} | "
            f"[bold blue]Running:[/bold blue] {running} | "
            f"[bold green]Completed:[/bold green] {completed} | "
            f"[bold red]Failed:[/bold red] {failed} | "
            f"[bold yellow]Queued:[/bold yellow] {queued}"
        )

        if running_tasks:
            for task in running_tasks:
                if task.progress_task_id is None:
                    task_id = running_progress.add_task(
                        f"[bold blue]{task.url}",
                        completed=task.progress,
                    )
                    self.task_manager.update_task(task.url, progress_task_id=task_id)
                else:
                    running_progress.update(
                        task.progress_task_id, completed=task.progress
                    )

        if completed_tasks:
            for task in completed_tasks:
                if task.progress_task_id is not None:
                    running_progress.update(task.progress_task_id, visible=False)

        if failed_tasks:
            for task in failed_tasks:
                if task.progress_task_id is not None:
                    running_progress.update(task.progress_task_id, visible=False)

        return Group(
            stats_line,
            running_progress,
        )

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
    parser.add_argument(
        "--debug-logs",
        action="store_true",
        help="Enable detailed debug logs for each task",
    )

    args = parser.parse_args()
    fetcher = WebPageFetcher(args.dir)

    # Display Rich banner

    console.print(
        Panel.fit(
            "[bold cyan]Web Page Fetcher[/bold cyan]\n"
            "Fetch website content in batch mode and generate metadata",
            title="🌐 CrawlEval",
            border_style="green",
        )
    )

    # Enable or disable verbose debug logging based on flag
    if args.debug_logs:
        console.print(
            "[info]Debug logs enabled. Saving detailed logs to task_logs/[/info]"
        )
    else:
        # When not debugging, don't save logs to files
        fetcher.task_manager.logs_dir = None

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
