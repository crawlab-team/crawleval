# CrawlEval

Resources and tools for evaluating the performance and behavior of web crawling systems.

## Overview

CrawlEval provides a comprehensive suite of tools and datasets for evaluating web crawling systems, with a particular focus on HTML pattern extraction and content analysis. The project includes:

1. A curated dataset of web pages with ground truth patterns
2. Tools for fetching and analyzing web content
3. Evaluation metrics and benchmarking capabilities

## Dataset

The dataset is designed to test and benchmark web crawling systems' ability to extract structured data from HTML. It includes:

- Raw HTML files with various structures and complexities
- Ground truth PagePattern JSON files
- Metadata about each example (query, complexity, etc.)

See the [dataset documentation](crawleval/README.md) for detailed information about the dataset structure and usage.

## Tools

### Web Page Fetcher (`fetch_webpage.py`)

A powerful tool for collecting and analyzing web pages for evaluation purposes.

Key features:
- Fetches web pages with proper JavaScript rendering using Selenium
- Extracts and analyzes metadata (DOM structure, nesting levels, etc.)
- Content deduplication using SHA-256 hashing
- URL deduplication with normalization
- Parallel processing of multiple URLs
- Progress tracking and detailed logging

Usage:
```bash
python -m crawleval.fetch_webpage --batch urls.txt [options]
```

Options:
- `--dir DIR`: Base directory for storing data
- `--list-hashes`: Display the content hash index
- `--list-urls`: Display the URL index
- `--save-results FILE`: Save batch processing results to a JSON file
- `--workers N`: Number of parallel workers (default: 4)

## Contributing

We welcome contributions to improve the dataset and tools. Please see the [dataset documentation](crawleval/README.md) for guidelines on adding new examples.
