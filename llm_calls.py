# llm_calls.py

import os
import time
import logging
from openai import OpenAI

MODEL = "o3-mini"
MAX_RETRIES = 3
REQUEST_TIMEOUT = 240

logger = logging.getLogger(__name__)

def call_gpt_api(messages, api_key=None, model=MODEL):
    """
    Call OpenAI API with retry logic and basic error handling.
    If api_key is not provided, attempts to get it from environment variables.
    """
    if api_key is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.error("No API key provided. Please set OPENAI_API_KEY environment variable.")
            return None

    # Estimate tokens (very rough)
    total_token_estimate = int(sum(len(m["content"].split()) for m in messages) * 1.3)
    logger.info("API Request Details:")
    logger.info(f"- Model: {model}")
    logger.info(f"- Timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"- Message count: {len(messages)}")
    logger.info(f"- Approx token count: {total_token_estimate}")

    client = OpenAI(api_key=api_key)
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Making API call (attempt {attempt+1}/{MAX_RETRIES}) to {model}...")
            start_time = time.time()
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                timeout=REQUEST_TIMEOUT
            )
            elapsed_time = time.time() - start_time
            logger.info(f"API call successful in {elapsed_time:.2f}s with model='{model}'")
            return response.choices[0].message.content.strip()

        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"Error on attempt {attempt+1}: {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES - 1:
                logger.warning("Retrying in 2 seconds...")
                time.sleep(2)
            else:
                return None
