import json
import pycurl
import io
import os
from pathlib import Path
from datetime import datetime

class LLMClient:
    def __init__(self, config_path="config/llm_config.json"):
        self.config = self._load_config(config_path)

    def _load_config(self, config_path):
        with open(config_path, "r") as f:
            return json.load(f)

    def log_token_usage(self, tokens):
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "llm_token_usage.log")
        with open(log_file, "a") as f:
            f.write(f"{datetime.now().isoformat()} - tokens_used: {tokens}\n")

    def generate_response(self, prompt):
        response_buffer = io.BytesIO()
        url = self.config["api_url"]
        api_key = self.config["api_key"]
        model = self.config["model"]
        temperature = self.config.get("temperature", 0.2)
        top_p = self.config.get("top_p", 1.0)
        max_tokens = self.config.get("max_tokens", 100)

        c = pycurl.Curl()
        c.setopt(c.URL, url)
        headers = [
            "Content-Type: application/json",
            f"Authorization: Bearer {api_key}"
        ]
        c.setopt(c.HTTPHEADER, headers)

        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "top_p": top_p,
            "max_tokens": max_tokens
        }
        c.setopt(c.POSTFIELDS, json.dumps(payload))
        c.setopt(c.WRITEDATA, response_buffer)

        try:
            c.perform()
            response_body = response_buffer.getvalue().decode()
            c.close()
            response_json = json.loads(response_body)
            # Log tokens if available
            tokens = None
            if "usage" in response_json and "total_tokens" in response_json["usage"]:
                tokens = response_json["usage"]["total_tokens"]
            elif "tokens" in response_json:
                tokens = response_json["tokens"]
            if tokens is not None:
                self.log_token_usage(tokens)
            return response_json
        except Exception as e:
            return {"error": f"Request failed: {e}"} 