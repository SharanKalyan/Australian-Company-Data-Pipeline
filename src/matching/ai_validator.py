import requests
import json
import re


class AIValidator:
    """
    AI Validator for semantic entity matching using a locally hosted LLM (Ollama).

    This class sends borderline fuzzy matches (75–84 similarity)
    to an LLM for semantic validation and returns structured results.
    """

    def __init__(self, model="phi3:mini"):
        self.model = model
        self.url = "http://localhost:11434/api/generate"

    def extract_json(self, raw_text):
        """
        Extract valid JSON object from LLM response text.
        Handles cases where model adds extra explanation or markdown.
        """

        # Remove markdown code blocks if present
        raw_text = raw_text.strip()
        raw_text = re.sub(r"```json", "", raw_text, flags=re.IGNORECASE)
        raw_text = re.sub(r"```", "", raw_text)

        # Extract first JSON object found
        match = re.search(r"\{.*\}", raw_text, re.DOTALL)

        if not match:
            raise ValueError("No JSON object found in response")

        json_str = match.group()

        return json.loads(json_str)

    def validate(self, name_a, name_b):
        """
        Validate whether two company names refer to the same legal entity.

        Returns:
        {
            "prompt": str,
            "raw_response": str,
            "parsed": {
                "same_entity": bool,
                "confidence": float,
                "reason": str
            }
        }
        """

        prompt = f"""
You are an expert in Australian company entity resolution.

Determine whether the following two company names
refer to the SAME legal business entity in Australia.

Company A: "{name_a}"
Company B: "{name_b}"

Guidelines:
- Consider abbreviations (e.g., Pty Ltd, Limited).
- Consider minor formatting differences.
- Do NOT assume similarity based only on shared common words.
- Be conservative if uncertain.

Respond ONLY in valid JSON format exactly like this:

{{
  "same_entity": true or false,
  "confidence": number between 0 and 1,
  "reason": "short explanation"
}}
"""

        try:
            response = requests.post(
                self.url,
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False
                },
                timeout=60
            )

            response.raise_for_status()

            raw_output = response.json().get("response", "").strip()

        except Exception as e:
            return {
                "prompt": prompt,
                "raw_response": str(e),
                "parsed": {
                    "same_entity": False,
                    "confidence": 0.0,
                    "reason": "LLM request failed"
                }
            }

        # ------------------------------
        # Robust JSON Extraction
        # ------------------------------
        try:
            parsed_json = self.extract_json(raw_output)

            same_entity = bool(parsed_json.get("same_entity", False))
            confidence = float(parsed_json.get("confidence", 0.0))
            reason = str(parsed_json.get("reason", ""))

            parsed_clean = {
                "same_entity": same_entity,
                "confidence": max(0.0, min(confidence, 1.0)),
                "reason": reason
            }

        except Exception as e:
            parsed_clean = {
                "same_entity": False,
                "confidence": 0.0,
                "reason": f"LLM response parsing failed: {str(e)}"
            }

        return {
            "prompt": prompt,
            "raw_response": raw_output,
            "parsed": parsed_clean
        }