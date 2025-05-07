# Extract Agent Evaluation Dataset Instructions

## Overview

This dataset is designed to evaluate the performance of the Extract Agent in identifying patterns in HTML content for
web scraping. It includes:

- Various HTML files of different complexity levels
- Ground truth pattern files showing the expected extraction patterns
- Metadata for each example with testing parameters
- Evaluation script to automate testing

## Getting Started

1. **Setup environment variables**:
   ```bash
   export OPENAI_API_KEY=your_openai_api_key
   export LLM_MODEL=gpt-4  # or other model of your choice
   ```

2. **Run the evaluation script**:
   ```bash
   cd /path/to/crawlab-pro
   go run core/ai/agent/eval_dataset/evaluate.go
   ```

3. **Review results**:
    - Results will be saved in a JSON file in the `eval_dataset` directory
    - The file will be named with a timestamp: `results_YYYYMMDD_HHMMSS.json`

## Adding New Test Cases

To expand the evaluation dataset with new examples:

1. **Add HTML file**:
    - Place your HTML file in the `html/` directory
    - Name it appropriately (e.g., `product_page.html`)

2. **Create metadata file**:
    - Add a new file in `metadata/` with the same base name (e.g., `product_page.json`)
    - Follow the existing metadata structure with appropriate queries

3. **Create ground truth pattern**:
    - Add a pattern file in `patterns/` (e.g., `product_page.json`)
    - Define the expected extraction pattern using the PagePattern format

4. **Update dataset config**:
    - Add your new example to `dataset_config.json`

## Evaluation Metrics

The evaluation script calculates several metrics:

- **Field Coverage**: How many of the expected fields were identified
- **List Detection**: How many of the expected lists were identified
- **Pagination Detection**: Whether pagination was correctly identified
- **Overall Score**: Average of all metrics

## Performance Considerations

- The evaluation can be resource-intensive, especially on large HTML files
- Consider adjusting timeout settings for complex examples
- Use a powerful model like GPT-4 for best results

## Troubleshooting

- **API errors**: Check your API key and rate limits
- **Timeouts**: Increase the timeout in the evaluation script
- **Validation failures**: Check that your HTML is well-formed

## Contributing

When contributing new examples to the dataset:

1. Try to include diverse HTML structures
2. Vary complexity levels from basic to advanced
3. Include realistic examples from actual websites
4. Document any special characteristics in the metadata
5. Ensure ground truth patterns are accurate 