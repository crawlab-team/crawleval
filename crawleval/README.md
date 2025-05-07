# Extract Agent Evaluation Dataset

This directory contains an evaluation dataset for testing and benchmarking the Extract Agent's HTML pattern extraction capabilities.

## Dataset Structure

The dataset is organized as follows:

- `html/`: Contains raw HTML files with different structures and complexities
- `ground_truth/`: Contains ground truth data and patterns
  - `patterns/`: PagePattern JSON files defining the expected extraction patterns
  - `data/`: Sample data extracted using the patterns
- `metadata/`: Contains metadata about each example including:
  - ID and name
  - Description and source URL
  - Creation timestamp
  - Page statistics (size, DOM nodes, text nodes, etc.)
  - Maximum nesting level
- `screenshots/`: Visual snapshots of the pages for reference
- `task_logs/`: Detailed logs of the data collection process
- `hash_index.json`: Content deduplication index
- `url_index.json`: URL deduplication index

## Usage

To evaluate the Extract Agent against this dataset:

1. Load an HTML file from the `html/` directory
2. Pass it to the Extract Agent with the query specified in the corresponding metadata file
3. Compare the extracted pattern with the ground truth pattern in `ground_truth/patterns/`
4. Verify the extracted data against the sample data in `ground_truth/data/`

## Evaluation Metrics

When evaluating the Extract Agent, consider the following metrics:

1. **Field Coverage**: Are all important fields identified?
2. **Selector Accuracy**: Are the selectors valid and robust?
3. **List Detection**: Are lists correctly identified?
4. **Pagination Detection**: Is pagination correctly identified?
5. **Extraction Accuracy**: Can the pattern successfully extract data?
6. **Pattern Complexity**: How well does the pattern handle complex DOM structures?

## Adding New Examples

To add a new example to the dataset:

1. Add the HTML file to the `html/` directory
2. Create a corresponding ground truth pattern in `ground_truth/patterns/`
3. Add sample extracted data in `ground_truth/data/`
4. Add metadata in the `metadata/` directory with:
   - Unique ID
   - Name and description
   - Source URL
   - Page statistics
   - Creation timestamp

## File Naming Convention

Files follow the naming convention:

- HTML: `{id}.html`
- Pattern: `{id}.json`
- Metadata: `{id}.json`
- Screenshot: `{id}.png`

## Example Types

The dataset includes examples of various types:

1. **Simple Pages**: Basic HTML with a few elements
2. **Product Listings**: Pages with lists of products
3. **Article Pages**: Pages with article content
4. **Nested Lists**: Pages with hierarchical list structures
5. **Paginated Content**: Pages with pagination controls
6. **Dynamic Content**: Pages with JavaScript-rendered content
7. **Complex Forms**: Pages with interactive form elements 