# Extract Agent Evaluation Dataset

This directory contains an evaluation dataset for testing and benchmarking the Extract Agent's HTML pattern extraction
capabilities.

## Dataset Structure

The dataset is organized as follows:

- `html/`: Contains raw HTML files with different structures and complexities
- `patterns/`: Contains ground truth PagePattern JSON files
- `metadata/`: Contains metadata about each example (query, complexity, etc.)

## Usage

To evaluate the Extract Agent against this dataset:

1. Load an HTML file from the `html/` directory
2. Pass it to the Extract Agent with the query specified in the corresponding metadata file
3. Compare the extracted pattern with the ground truth pattern in the `patterns/` directory

## Evaluation Metrics

When evaluating the Extract Agent, consider the following metrics:

1. **Field Coverage**: Are all important fields identified?
2. **Selector Accuracy**: Are the selectors valid and robust?
3. **List Detection**: Are lists correctly identified?
4. **Pagination Detection**: Is pagination correctly identified?
5. **Extraction Accuracy**: Can the pattern successfully extract data?

## Adding New Examples

To add a new example to the dataset:

1. Add the HTML file to the `html/` directory
2. Create a corresponding ground truth pattern in the `patterns/` directory
3. Add metadata in the `metadata/` directory

## File Naming Convention

Files follow the naming convention:

- HTML: `example_name.html`
- Pattern: `example_name.json`
- Metadata: `example_name.json`

## Example Types

The dataset includes examples of various types:

1. **Simple Pages**: Basic HTML with a few elements
2. **Product Listings**: Pages with lists of products
3. **Article Pages**: Pages with article content
4. **Nested Lists**: Pages with hierarchical list structures
5. **Paginated Content**: Pages with pagination controls 