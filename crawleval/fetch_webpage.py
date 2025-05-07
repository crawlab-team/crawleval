"""
Web Page Fetcher Module

This module provides functionality to fetch web pages using DrissionPage,
analyze their content, and save both the HTML and metadata for later evaluation.

Features:
- Fetches web pages with proper rendering of JavaScript content
- Extracts and analyzes metadata such as DOM structure, nesting levels, etc.
- Content deduplication using SHA-256 hashing and optional HTML normalization
- URL deduplication with URL normalization to avoid processing duplicates
- Saves HTML and metadata for future reference
- Batch processing of multiple URLs from a text file
- Parallel fetching with configurable number of workers
- Progress tracking with tqdm

Command-line usage:
  python -m crawleval.fetch_webpage <url> [options]
  python -m crawleval.fetch_webpage --batch urls.txt [options]

Options:
  --dir DIR               Base directory for storing data
  --force                 Override duplicate detection and save anyway
  --no-normalize          Disable HTML normalization for strict deduplication
  --list-hashes           Display the content hash index
  --list-urls             Display the URL index
  --batch URL_FILE        Process multiple URLs from a text file (one URL per line)
  --save-results FILE     Save batch processing results to a JSON file
  --workers N             Number of parallel workers for batch processing (default: 4)

Deduplication systems:
1. Content deduplication:
   - Computes a SHA-256 hash of the HTML content (optionally normalized)
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup, Comment, Declaration, Doctype
from DrissionPage import ChromiumOptions, ChromiumPage
from tqdm import tqdm

from crawleval.models import PageData, PageMetadata

# Define comment types for HTML normalization
comment_types = (Comment, Doctype, Declaration)


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

    def _compute_html_hash(self, html_content, normalize=True):
        """Compute a SHA-256 hash of the HTML content

        Args:
            html_content (str): HTML content to hash
            normalize (bool): Whether to normalize the HTML before hashing

        Returns:
            str: The hexadecimal digest of the hash
        """
        content_to_hash = (
            self._normalize_html(html_content) if normalize else html_content
        )
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

    def fetch_html(self, url: str) -> PageData | None:
        """Fetch HTML content from a URL using DrissionPage

        Args:
            url: The URL to fetch
        """
        # Create a fresh browser instance
        page = self.setup_page()
        if not page:
            return None

        try:
            print(f"Navigating to {url}...")
            page.get(url)

            # Wait for page to load using dynamic waiting strategy
            print("Waiting for page to fully render...")
            try:
                # Wait for document ready state to be complete
                page.wait.doc_loaded(timeout=10)
            except Exception as e:
                print(f"Warning: Wait condition timed out: {e}")
                print("Continuing with potentially incomplete page...")

            # Get page source after JavaScript execution
            html_content = page.html

            # Take a screenshot of the page
            screenshot_data = page.get_screenshot(as_bytes=True)

            # Additional page metadata
            page_title = page.title
            current_url = page.url  # In case of redirects

            return PageData(
                html=html_content,
                title=page_title,
                url=current_url,
                screenshot=screenshot_data,
            )
        except Exception as e:
            print(f"Error fetching URL {url}: {e}")
            return None
        finally:
            # Always close the browser when done
            if page:
                try:
                    page.quit()
                except Exception:
                    pass

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

    def save_files(self, page_data: PageData, metadata: PageMetadata):
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

        print("Files created successfully:")
        print(f"  HTML: {html_path}")
        if screenshot_path:
            print(f"  Screenshot: {screenshot_path}")
        print(f"  Metadata: {metadata_path}")

        return file_id

    def process_url(self, url):
        """Process a URL: fetch, analyze, and save"""
        print(f"Fetching content from: {url}")

        # Check for URL duplicate first
        url_duplicate_id = self._check_url_duplicate(url)
        if url_duplicate_id:
            print(
                f"URL already processed! Matches existing entry with ID: {url_duplicate_id}"
            )
            return url_duplicate_id

        page_data = self.fetch_html(url)

        if not page_data:
            print("Failed to fetch content. Aborting.")
            return None

        # Check for duplicate content
        duplicate_id = self._check_duplicate(page_data.html)
        if duplicate_id:
            print(
                f"Duplicate content found! Matches existing entry with ID: {duplicate_id}"
            )
            # Register the URL to the duplicate content's file ID
            self._register_url(url, duplicate_id)
            return duplicate_id

        print("Analyzing content...")
        metadata = self.analyze_html(page_data)

        print("Saving files...")
        file_id = self.save_files(page_data, metadata)

        # Register the hash for future duplicate checks
        self._register_hash(page_data.html, file_id)

        # Register the URL for future duplicate checks
        self._register_url(url, file_id)

        return file_id

    def list_hashes(self):
        """List all content hashes and their corresponding file IDs"""
        if not self.hash_index:
            print("Hash index is empty. No pages have been processed yet.")
            return

        print(f"Content hash index ({len(self.hash_index)} entries):")
        print("=" * 80)
        print(f"{'Content Hash (SHA-256)':<64} | {'File ID':<10} | {'URL':<20}")
        print("-" * 80)

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

            print(f"{hash_value:<64} | {file_id:<10} | {url:<40}")
        print("=" * 80)

    def list_urls(self):
        """List all URLs and their corresponding file IDs"""
        if not self.url_index:
            print("URL index is empty. No URLs have been processed yet.")
            return

        print(f"URL index ({len(self.url_index)} entries):")
        print("=" * 80)
        print(f"{'URL':<60} | {'File ID':<10}")
        print("-" * 80)

        for normalized_url, file_id in sorted(
            self.url_index.items(), key=lambda x: x[1]
        ):
            # Truncate long URLs
            display_url = normalized_url
            if len(display_url) > 60:
                display_url = display_url[:57] + "..."

            print(f"{display_url:<60} | {file_id:<10}")
        print("=" * 80)

    def batch_process_urls(
        self, url_file_path: str, normalize=True, max_workers=4
    ):
        """Process multiple URLs from a text file in parallel

        Args:
            url_file_path (str): Path to text file with URLs (one per line)
            normalize (bool): Whether to normalize HTML for deduplication
            max_workers (int): Maximum number of parallel workers

        Returns:
            dict: Summary of processing results
        """
        if not os.path.exists(url_file_path):
            print(f"Error: URL file not found: {url_file_path}")
            return None

        # Setup for normalization override if needed
        original_compute = None
        if not normalize:
            original_compute = self._compute_html_hash
            self._compute_html_hash = lambda html: original_compute(
                html, normalize=False
            )

        results = {
            "total": 0,
            "successful": 0,
            "failed": 0,
            "duplicate_urls": 0,
            "duplicate_content": 0,
            "new_entries": 0,
            "entries": [],
        }

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
                print(f"Skipping {len(duplicates)} duplicate URLs")
                if len(duplicates) <= 5:  # Show if just a few
                    for dup in duplicates:
                        print(f"  - {dup}")

            urls = unique_urls

            results["total"] = len(urls)
            if not urls:
                print("No URLs to process after filtering duplicates.")
                return results

            print(
                f"Starting batch processing of {len(urls)} URLs with {max_workers} workers..."
            )

            # Process each URL in parallel
            def process_single_url(url):
                """Process a single URL and return the result"""
                result = {"url": url, "status": "unknown"}
                retry_count = 0
                max_retries = 2  # Allow up to 2 retries per URL

                while retry_count <= max_retries:
                    try:
                        # First check URL duplication (we already filtered, but double-check)
                        url_duplicate_id = self._check_url_duplicate(url)
                        if url_duplicate_id:
                            result["status"] = "duplicate_url"
                            result["file_id"] = url_duplicate_id
                            result["is_new"] = False
                            return result

                        # Fetch the content with a fresh browser for each URL
                        page_data = self.fetch_html(url)
                        if not page_data:
                            if retry_count < max_retries:
                                retry_count += 1
                                print(
                                    f"Retrying {url} (attempt {retry_count}/{max_retries})"
                                )
                                continue
                            result["status"] = "failed"
                            return result

                        # Check content duplication
                        content_duplicate_id = self._check_duplicate(page_data.html)
                        if content_duplicate_id:
                            # Use a lock to avoid race conditions when updating shared data
                            self._register_url(url, content_duplicate_id)
                            result["status"] = "duplicate_content"
                            result["file_id"] = content_duplicate_id
                            result["is_new"] = False
                            return result

                        # Process as new content
                        metadata = self.analyze_html(page_data)
                        file_id = self.save_files(page_data, metadata)

                        # Use locks to prevent race conditions when updating shared data
                        self._register_hash(page_data.html, file_id)
                        self._register_url(url, file_id)

                        result["status"] = "success"
                        result["file_id"] = file_id
                        result["is_new"] = True
                        return result

                    except Exception as e:
                        if retry_count < max_retries:
                            retry_count += 1
                            print(f"Error processing {url}: {e}")
                            print(f"Retrying (attempt {retry_count}/{max_retries})")
                        else:
                            result["status"] = "error"
                            result["error"] = str(e)
                            return result

                # If we reached this point after all retries, return the failure result
                result["status"] = (
                    "error" if "error" not in result else result["status"]
                )
                return result

            # Process URLs in parallel with progress bar
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(process_single_url, url): url for url in urls
                }

                # Use tqdm for progress tracking
                with tqdm(total=len(futures), desc="Processing URLs") as pbar:
                    for future in as_completed(futures):
                        url = futures[future]
                        try:
                            result = future.result()
                            results["entries"].append(result)

                            # Update statistics
                            if result["status"] == "success" and result.get(
                                "is_new", False
                            ):
                                results["successful"] += 1
                                results["new_entries"] += 1
                            elif result["status"] == "duplicate_url":
                                results["successful"] += 1
                                results["duplicate_urls"] += 1
                            elif result["status"] == "duplicate_content":
                                results["successful"] += 1
                                results["duplicate_content"] += 1
                            elif result["status"] in ["failed", "error"]:
                                results["failed"] += 1
                                if result["status"] == "error" and "error" in result:
                                    print(
                                        f"\nError processing {url}: {result['error']}"
                                    )

                        except Exception as e:
                            print(f"\nUnexpected error with {url}: {e}")
                            results["failed"] += 1
                            results["entries"].append(
                                {"url": url, "status": "error", "error": str(e)}
                            )

                        # Update progress bar
                        pbar.update(1)

            # Print summary
            print("\nBatch processing completed.")
            print(f"Total URLs: {results['total']}")
            print(f"Successful: {results['successful']}")
            print(f"Failed: {results['failed']}")
            print(f"New entries: {results['new_entries']}")
            print(f"Duplicate URLs: {results['duplicate_urls']}")
            print(f"Duplicate content: {results['duplicate_content']}")

        except Exception as e:
            print(f"Error during batch processing: {e}")
        finally:
            # Restore original hash method if it was modified
            if original_compute is not None:
                self._compute_html_hash = original_compute

        return results


def main():
    parser = argparse.ArgumentParser(
        description="Fetch website content using DrissionPage and generate metadata"
    )
    parser.add_argument("url", help="URL to fetch", nargs="?")
    parser.add_argument(
        "--dir",
        default=os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "..", "eval_data_extract"
        ),
        help="Base directory (default: <project_root>/eval_data_extract)",
    )
    parser.add_argument(
        "--no-normalize",
        action="store_true",
        help="Disable HTML normalization for deduplication (more strict comparison)",
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
        "--batch",
        metavar="URL_FILE",
        help="Process multiple URLs from a text file (one URL per line)",
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

    # Handle list-hashes command
    if args.list_hashes:
        fetcher.list_hashes()
        return

    # Handle list-urls command
    if args.list_urls:
        fetcher.list_urls()
        return

    # Handle batch processing
    if args.batch:
        results = fetcher.batch_process_urls(
            args.batch,
            normalize=not args.no_normalize,
            max_workers=args.workers,
        )

        # Save results if requested
        if args.save_results and results:
            try:
                import json

                with open(args.save_results, "w", encoding="utf-8") as f:
                    json.dump(results, f, indent=2)
                print(f"Results saved to {args.save_results}")
            except Exception as e:
                print(f"Error saving results: {e}")

        return

    # URL is required for single-URL fetching
    if not args.url:
        parser.error(
            "URL is required unless --list-hashes, --list-urls, or --batch is specified"
        )

    # Override the _compute_html_hash method to disable normalization if requested
    if args.no_normalize:
        original_compute = fetcher._compute_html_hash
        fetcher._compute_html_hash = lambda html: original_compute(html)

    # Normal processing with duplicate check
    file_id = fetcher.process_url(args.url)
    if file_id:
        print(f"\nCompleted! Dataset entry ID: {file_id}")


if __name__ == "__main__":
    main()
