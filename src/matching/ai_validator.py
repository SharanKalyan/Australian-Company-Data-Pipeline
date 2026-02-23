
from __future__ import annotations
import json
import re

import requests

# ── Compiled once at import ──────────────────────────────────────────────────
_MD_FENCE_RE   = re.compile(r"```(?:json)?", re.IGNORECASE)
_JSON_BLOCK_RE = re.compile(r"\{[^{}]*\}", re.DOTALL)
# ────────────────────────────────────────────────────────────────────────────


class AIValidator:
    """
    Thin wrapper around Ollama's /api/generate endpoint for entity validation.

    Parameters
    ----------
    model   : Ollama model tag, default "phi3:mini"
    timeout : HTTP timeout in seconds, default 20
    """

    def __init__(self, model: str = "phi3:mini", timeout: int = 20):
        self.model   = model
        self.url     = "http://localhost:11434/api/generate"
        self.timeout = timeout
        # One persistent session → one TCP connection reused for all calls
        self.session = requests.Session()

    # ── Prompt builder ────────────────────────────────────────────────────────

    def _build_prompt(self, name_a: str, name_b: str) -> str:
        # Minimal tokens = faster prefill on Phi3:mini
        return (
            f'Same Australian business entity?\n'
            f'A: "{name_a}"\n'
            f'B: "{name_b}"\n'
            f'Ignore: Pty Ltd / Ltd / Limited / Holdings / Group / state codes / punctuation.\n'
            f'Be conservative.\n'
            f'Reply ONLY with JSON: '
            f'{{"same_entity":true/false,"confidence":0.0-1.0,"reason":"brief"}}'
        )

    # ── JSON extraction ───────────────────────────────────────────────────────

    def _extract_json(self, raw: str) -> dict:
        # Strip any markdown fences the model might emit
        cleaned = _MD_FENCE_RE.sub("", raw).strip()

        # Fast path — entire response is already valid JSON
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass

        # Fallback — grab the first {...} block
        m = _JSON_BLOCK_RE.search(cleaned)
        if m:
            return json.loads(m.group())

        raise ValueError(f"No valid JSON in LLM response: {cleaned!r}")

    # ── Public API ────────────────────────────────────────────────────────────

    def validate(self, name_a: str, name_b: str) -> dict:
        """
        Validate whether two names refer to the same entity.

        Returns
        -------
        {
            "prompt":       str,
            "raw_response": str,
            "parsed": {
                "same_entity": bool,
                "confidence":  float,   # 0.0 – 1.0
                "reason":      str,
            }
        }
        """
        prompt = self._build_prompt(name_a, name_b)

        # ── LLM call ─────────────────────────────────────────────────────────
        try:
            resp = self.session.post(
                self.url,
                json={
                    "model":  self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "num_predict": 60,   # ~40 tokens needed; 60 is safe ceiling
                        "temperature": 0.0,  # deterministic = consistent + faster
                    },
                },
                timeout=self.timeout,
            )
            resp.raise_for_status()
            raw_output = resp.json().get("response", "").strip()

        except Exception as exc:
            return {
                "prompt":       prompt,
                "raw_response": str(exc),
                "parsed": {
                    "same_entity": False,
                    "confidence":  0.0,
                    "reason":      "LLM request failed",
                },
            }

        # ── Parse ─────────────────────────────────────────────────────────────
        try:
            parsed = self._extract_json(raw_output)
            parsed_clean = {
                "same_entity": bool(parsed.get("same_entity", False)),
                "confidence":  round(
                    max(0.0, min(float(parsed.get("confidence", 0.0)), 1.0)), 4
                ),
                "reason": str(parsed.get("reason", "")),
            }
        except Exception as exc:
            parsed_clean = {
                "same_entity": False,
                "confidence":  0.0,
                "reason":      f"Parsing failed: {exc}",
            }

        return {
            "prompt":       prompt,
            "raw_response": raw_output,
            "parsed":       parsed_clean,
        }