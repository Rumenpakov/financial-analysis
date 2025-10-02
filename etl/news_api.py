import os
import sys
import json
import argparse
from datetime import datetime, timedelta

import requests
import boto3

# --- Configuration ---

# The base URL for the NewsAPI 'everything' endpoint.
NEWS_API_BASE_URL = "https://newsapi.org/v2/everything"

# Keywords to search for. This targets relevant financial and economic news.
# The `OR` ensures we get articles matching any of these terms.
SEARCH_KEYWORDS = (
    "stock market OR earnings OR federal reserve OR inflation OR interest rates "
    "OR quarterly results OR market forecast OR stock price"
)


# --- Main Script Logic ---

def fetch_and_store_news(api_key: str, s3_bucket: str):
    """
    Fetches news from NewsAPI, transforms it, and stores it in S3.

    :param api_key: The API key for NewsAPI.org.
    :param s3_bucket: The name of the S3 bucket to upload the raw data to.
    """
    print("Starting news fetching process...")

    # 1. CONFIGURE API REQUEST
    # Fetch news from the last 24 hours.
    five_days_ago = (datetime.utcnow() - timedelta(days=5)).strftime('%Y-%m-%dT%H:%M:%S')

    params = {
        "q": SEARCH_KEYWORDS,
        "from": five_days_ago,
        "sortBy": "relevancy",
        "language": "en",
        "apiKey": api_key
    }

    # 2. FETCH DATA FROM API
    try:
        response = requests.get(NEWS_API_BASE_URL, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        raw_api_data = response.json()
        articles_found = raw_api_data.get("articles", [])
        print(f"Successfully fetched {len(articles_found)} articles from NewsAPI.")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from NewsAPI: {e}")
        sys.exit(1)  # Exit with an error code

    if not articles_found:
        print("No new articles found. Exiting.")
        return

    # 3. TRANSFORM DATA
    # Convert the raw API response into the standardized format our ETL job expects.
    # This is a critical step to decouple our pipeline from the specific source API format.
    transformed_articles = []
    for article in articles_found:
        # Note: NewsAPI's free tier sometimes returns truncated content.
        # We use a combination of description and content to get the most text.
        body = article.get('content', '') or article.get('description', '')

        transformed_article = {
            "articleId": article.get('url'),  # Use the URL as a unique ID
            "headline": article.get('title'),
            "articleBody": body,
            "publishedAt": article.get('publishedAt'),
            "source": {
                "name": article.get('source', {}).get('name')
            }
        }
        transformed_articles.append(transformed_article)

    print(f"Transformed {len(transformed_articles)} articles into the standard format.")

    # 4. STORE DATA IN S3
    try:
        s3_client = boto3.client("s3")

        # Generate a unique filename to prevent overwriting data.
        # e.g., raw_news_2025-10-27T10-30-00.json
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        s3_key = f"raw_news_{timestamp}.json"

        # Convert our list of articles to a JSON string for upload.
        # Using `indent` makes the raw file human-readable in S3.
        s3_object_body = json.dumps(transformed_articles, indent=2)

        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=s3_object_body
        )
        print(f"Successfully uploaded raw data to s3://{s3_bucket}/{s3_key}")

    except Exception as e:
        print(f"Error uploading data to S3: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch financial news and upload to S3.")
    parser.add_argument("--s3-bucket", required=True, help="The S3 bucket for raw data")
    args = parser.parse_args()

    # Best Practice: Get the API key from an environment variable, not hardcoded.
    news_api_key = os.getenv("NEWS_API_KEY")
    # e8d63f2ec7314db1b46c21d770781151
    if not news_api_key:
        print("Error: NEWS_API_KEY environment variable not set.")
        sys.exit(1)

    fetch_and_store_news(api_key=news_api_key, s3_bucket=args.s3_bucket)