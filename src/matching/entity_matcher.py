"""
entity_matcher.py
-----------------
Matches entities from staging.commoncrawl_clean → staging.abr_clean and:
  - Writes every confirmed match to core.company_master IMMEDIATELY.
  - Writes every AI log to core.ai_match_log IMMEDIATELY after each AI call.

Scoring rules
  >= 85        → auto-approved  (fuzzy only, no AI)
  78 – 84      → AI validated   (approved only if AI returns same_entity=True)
  < 78         → rejected

Key optimisations
  - rapidfuzz with score_cutoff skips sub-threshold candidates in C.
  - Dual blocking index (3-char prefix + first token) catches word-order variants.
  - Composite scorer: token_set_ratio + partial_ratio + token_sort_ratio.
  - Parallel AI calls via ThreadPoolExecutor (default 6 workers, safe on 8GB RAM).
  - Immediate single-row DB writes — no batching delay.
  - All regex compiled once at module level.
"""

from __future__ import annotations

import json
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from rapidfuzz import fuzz, process as rf_process
from sqlalchemy import text

from src.db.connection import PostgresConnector
from src.matching.ai_validator import AIValidator


# ── Module-level compiled patterns ──────────────────────────────────────────

_LEGAL_SUFFIX_RE = re.compile(
    r"\b("
    r"pty\.?\s*ltd\.?|p\/l|"
    r"ltd\.?|limited|"
    r"inc\.?|incorporated|"
    r"corp\.?|corporation|"
    r"holdings?|group|trust|"
    r"services?|solutions?|"
    r"australia|aust\.?|"
    r"nsw|vic|qld|wa|sa|tas|act|nt"
    r")\b",
    re.IGNORECASE,
)
_PUNCT_RE      = re.compile(r"[^\w\s]")
_WHITESPACE_RE = re.compile(r"\s+")

# ── Scoring thresholds ───────────────────────────────────────────────────────

SCORE_HIGH_CONF    = 90   # > 90   → auto-approve, HIGH confidence
SCORE_MED_CONF     = 82   # 82–90  → auto-approve, MEDIUM confidence
SCORE_AI_MIN       = 78   # 78–81  → send to AI for validation
                           # < 78   → reject outright

# ── DB SQL ───────────────────────────────────────────────────────────────────

_INSERT_MASTER = text("""
    INSERT INTO core.company_master
        (abn, website_url, company_name, industry,
         entity_type, entity_status, state, postcode,
         match_method, match_confidence)
    SELECT
        :abn, :website_url, :company_name, :industry,
        :entity_type, :entity_status, :state, :postcode,
        :match_method, :match_confidence
    WHERE NOT EXISTS (
        SELECT 1 FROM core.company_master
        WHERE abn = :abn AND website_url = :website_url
    )
""")

_INSERT_AI_LOG = text("""
    INSERT INTO core.ai_match_log
        (company_a, company_b, fuzzy_score,
         prompt, llm_response,
         decision)
    VALUES
        (:company_a, :company_b, :fuzzy_score,
         :prompt, CAST(:llm_response AS jsonb),
         :decision)
""")


# ── Helpers ──────────────────────────────────────────────────────────────────

def strong_normalize(name: str) -> str:
    """
    Lower-case, strip legal suffixes / state codes / punctuation, collapse spaces.
    Uses module-level compiled regexes — safe to call from multiple threads.
    """
    if not name:
        return ""
    name = name.lower()
    name = _LEGAL_SUFFIX_RE.sub("", name)
    name = _PUNCT_RE.sub(" ", name)
    name = _WHITESPACE_RE.sub(" ", name).strip()
    return name


def composite_score(norm_a: str, norm_b: str) -> float:
    """
    Best of three rapidfuzz scorers (all run in C, very fast):
      token_set_ratio  – handles word-order differences & subset names
      partial_ratio    – handles one name being contained in the other
      token_sort_ratio – handles pure word-order shuffles
    """
    return max(
        fuzz.token_set_ratio(norm_a, norm_b),
        fuzz.partial_ratio(norm_a, norm_b),
        fuzz.token_sort_ratio(norm_a, norm_b),
    )


# ── Main class ───────────────────────────────────────────────────────────────

class EntityMatcher:
    """
    Parameters
    ----------
    db_password  : PostgreSQL password for PostgresConnector.
    ai_workers   : Parallel threads for AI validation (default 6, safe on 8 GB RAM).
                   Do not exceed 8 on an 8 GB machine — risk of swap thrashing.
    debug        : Print per-record decisions to stdout.
    """

    def __init__(
        self,
        db_password: str,
        ai_workers:  int  = 6,     # ← safe sweet spot for 8 GB RAM
        debug:       bool = False,
    ):
        self.db           = PostgresConnector(password=db_password)
        self.ai_workers   = ai_workers
        self.debug        = debug
        self.ai_validator = AIValidator()

    # ── Data loading ──────────────────────────────────────────────────────────

    def load_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        cc_df = pd.read_sql("""
            SELECT website_url, company_name, normalized_name
            FROM staging.commoncrawl_clean
            WHERE normalized_name IS NOT NULL
        """, self.db.engine)

        abr_df = pd.read_sql("""
            SELECT abn, entity_name, normalized_name,
                   entity_type, entity_status, state, postcode
            FROM staging.abr_clean
            WHERE normalized_name IS NOT NULL
        """, self.db.engine)

        print(f"Common Crawl records : {len(cc_df):,}")
        print(f"ABR records          : {len(abr_df):,}")
        return cc_df, abr_df

    # ── Blocking index ────────────────────────────────────────────────────────

    def build_indexes(self, abr_df: pd.DataFrame) -> tuple[dict, dict, dict, dict]:
        """
        Build two blocking indexes for fast candidate retrieval:
          by_prefix3  – keyed on first 3 chars of strong_norm
          by_token1   – keyed on first whitespace token of strong_norm
        Both map key → list[namedtuple row] and key → list[str norm].
        """
        abr_df = abr_df.copy()
        abr_df["strong_norm"] = abr_df["normalized_name"].apply(strong_normalize)

        by_prefix3 = defaultdict(list)
        by_token1  = defaultdict(list)

        for row in abr_df.itertuples(index=False):
            sn = row.strong_norm
            if not sn:
                continue
            by_prefix3[sn[:3]].append(row)
            first_tok = sn.split()[0] if sn.split() else sn
            by_token1[first_tok].append(row)

        def _names(d):
            return {k: [r.strong_norm for r in v] for k, v in d.items()}

        return by_prefix3, _names(by_prefix3), by_token1, _names(by_token1)

    # ── Immediate DB writes ───────────────────────────────────────────────────

    def _write_match(self, record: dict) -> None:
        """Write a single confirmed match to core.company_master immediately."""
        with self.db.engine.begin() as conn:
            conn.execute(_INSERT_MASTER, record)
        if self.debug:
            print(f"  [DB MATCH] {record['company_name']} | "
                  f"abn={record['abn']} | "
                  f"method={record['match_method']} | "
                  f"conf={record['match_confidence']:.1f}")

    def _write_ai_log(self, log: dict) -> None:
        """Write a single AI validation event to core.ai_match_log immediately."""
        with self.db.engine.begin() as conn:
            conn.execute(_INSERT_AI_LOG, log)
        if self.debug:
            print(f"  [DB AI LOG] {log['company_a']!r} / {log['company_b']!r} | "
                  f"fuzzy={log['fuzzy_score']} | "
                  f"approved={log['decision']}")

    # ── Candidate lookup ──────────────────────────────────────────────────────

    def _get_candidates(
        self,
        cc_norm:       str,
        by_prefix3:    dict,
        names_prefix3: dict,
        by_token1:     dict,
        names_token1:  dict,
    ) -> tuple[list, list]:
        """
        Merge candidates from both blocking indexes, deduplicating by abn.
        Returns (candidate_rows, candidate_norms).
        """
        key3   = cc_norm[:3]
        tokens = cc_norm.split()
        keytok = tokens[0] if tokens else cc_norm

        seen_abns = set()
        rows, norms = [], []

        for bucket_rows, bucket_norms in (
            (by_prefix3.get(key3, []),  names_prefix3.get(key3, [])),
            (by_token1.get(keytok, []), names_token1.get(keytok, [])),
        ):
            for row, norm in zip(bucket_rows, bucket_norms):
                if row.abn not in seen_abns:
                    seen_abns.add(row.abn)
                    rows.append(row)
                    norms.append(norm)

        return rows, norms

    # ── Single AI validation job (runs in thread pool) ────────────────────────

    def _run_ai_job(
        self,
        cc_name:     str,
        abr_name:    str,
        fuzzy_score: float,
    ) -> tuple[bool, float]:
        """
        Call AIValidator, write the log immediately, return (approved, confidence).
        Runs inside a ThreadPoolExecutor worker thread.
        """
        ai_result = self.ai_validator.validate(cc_name, abr_name)
        parsed    = ai_result["parsed"]
        approved  = bool(parsed["same_entity"])

        self._write_ai_log({
            "company_a":    cc_name,
            "company_b":    abr_name,
            "fuzzy_score":  fuzzy_score,
            "prompt":       ai_result["prompt"],
            "llm_response": json.dumps({
                "raw":    ai_result["raw_response"],
                "parsed": parsed,
            }),
            "decision":     approved,
        })

        return approved, float(parsed["confidence"])

    # ── Core matching loop ────────────────────────────────────────────────────

    def fuzzy_match(self, cc_df: pd.DataFrame, abr_df: pd.DataFrame) -> None:

        print("\nBuilding blocking indexes …")
        by_prefix3, names_prefix3, by_token1, names_token1 = self.build_indexes(abr_df)
        print("Indexes ready. Starting matching …\n")

        start          = time.perf_counter()
        total_records  = len(cc_df)          # known upfront for ETA
        total_proc     = 0
        total_written  = 0
        total_ai       = 0
        total_rejected = 0
        total_high     = 0
        total_med      = 0

        # ── Helper closures ───────────────────────────────────────────────────

        def _build_record(cc_row, abr_row, method: str, confidence: float) -> dict:
            # Truncate to column limits and coerce None-safe strings
            # state: VARCHAR(3), postcode: VARCHAR(4), match_method: VARCHAR(20)
            raw_state    = str(abr_row.state    or "").strip()
            raw_postcode = str(abr_row.postcode or "").strip()
            raw_method   = str(method or "").strip()

            # Only keep postcode if it looks like a valid AU postcode (4 digits)
            postcode = raw_postcode if re.fullmatch(r"\d{4}", raw_postcode) else None
            state    = raw_state[:3]   if raw_state    else None
            method_  = raw_method[:20] if raw_method   else method

            return {
                "abn":              abr_row.abn,
                "website_url":      cc_row.website_url,
                "company_name":     abr_row.entity_name,
                "industry":         None,
                "entity_type":      abr_row.entity_type,
                "entity_status":    abr_row.entity_status,
                "state":            state,
                "postcode":         postcode,
                "match_method":     method_,
                "match_confidence": round(float(confidence), 2),
            }

        def _drain_futures(pending: dict, total_written: int) -> int:
            """Collect any completed AI futures and write approved matches."""
            done = [f for f in list(pending) if f.done()]
            for fut in done:
                cc_row, abr_row, _ = pending.pop(fut)
                try:
                    approved, confidence = fut.result()
                except Exception as exc:
                    print(f"  [AI ERROR] {exc}")
                    continue
                if approved:
                    self._write_match(_build_record(
                        cc_row, abr_row, "ai_validated", confidence * 100
                    ))
                    total_written += 1
            return total_written

        # ── Main loop ─────────────────────────────────────────────────────────

        pending_futures: dict = {}   # future → (cc_row, abr_row, fuzzy_score)

        with ThreadPoolExecutor(max_workers=self.ai_workers) as executor:

            for cc_row in cc_df.itertuples(index=False):
                total_proc += 1

                cc_norm = strong_normalize(cc_row.normalized_name)
                if not cc_norm:
                    continue

                # Candidate retrieval via dual index
                candidates, candidate_norms = self._get_candidates(
                    cc_norm,
                    by_prefix3, names_prefix3,
                    by_token1,  names_token1,
                )
                if not candidates:
                    continue

                # Fast pre-filter — score_cutoff skips sub-77 pairs in C
                best = rf_process.extractOne(
                    cc_norm,
                    candidate_norms,
                    scorer=fuzz.token_set_ratio,
                    score_cutoff=SCORE_AI_MIN - 1,   # skip anything below 77
                )
                if not best:
                    total_rejected += 1
                    continue

                _, _, idx = best
                abr_row  = candidates[idx]
                abr_norm = abr_row.strong_norm

                # Composite rescore for final decision
                score = composite_score(cc_norm, abr_norm)

                # ── Decision tree ─────────────────────────────────────────────

                if score > SCORE_HIGH_CONF:
                    # > 90 — HIGH confidence auto-approve
                    if self.debug:
                        print(f"[HIGH] {cc_row.company_name!r} → "
                              f"{abr_row.entity_name!r} | score={score:.0f}")
                    self._write_match(_build_record(
                        cc_row, abr_row, "fuzzy_high_conf", score
                    ))
                    total_written += 1
                    total_high    += 1

                elif score >= SCORE_MED_CONF:
                    # 82–90 — MEDIUM confidence auto-approve
                    if self.debug:
                        print(f"[MED]  {cc_row.company_name!r} → "
                              f"{abr_row.entity_name!r} | score={score:.0f}")
                    self._write_match(_build_record(
                        cc_row, abr_row, "fuzzy_med_conf", score
                    ))
                    total_written += 1
                    total_med     += 1

                elif score >= SCORE_AI_MIN:
                    # 78–81 — borderline, send to AI
                    total_ai += 1
                    if self.debug:
                        print(f"[AI]   {cc_row.company_name!r} → "
                              f"{abr_row.entity_name!r} | fuzzy={score:.0f}")
                    fut = executor.submit(
                        self._run_ai_job,
                        cc_row.company_name,
                        abr_row.entity_name,
                        float(score),
                    )
                    pending_futures[fut] = (cc_row, abr_row, score)

                else:
                    # < 78 — reject
                    total_rejected += 1

                # Drain completed AI futures so memory stays bounded
                if len(pending_futures) >= self.ai_workers * 2:
                    total_written = _drain_futures(pending_futures, total_written)

                # Progress report every 1,000 rows
                if total_proc % 1000 == 0:
                    elapsed      = time.perf_counter() - start
                    remaining    = total_records - total_proc
                    rate         = total_proc / elapsed if elapsed > 0 else 0
                    eta_secs     = (remaining / rate) if rate > 0 else 0
                    eta_mins     = eta_secs / 60
                    pct          = (total_proc / total_records * 100) if total_records > 0 else 0
                    print(
                        f"  [{pct:5.1f}%] "
                        f"Processed={total_proc:,}/{total_records:,}  "
                        f"Remaining={remaining:,}  "
                        f"Written={total_written:,}  "
                        f"AI calls={total_ai:,}  "
                        f"Rejected={total_rejected:,}  "
                        f"Elapsed={elapsed:.1f}s  "
                        f"ETA={eta_mins:.1f}m"
                    )

            # ── Drain all remaining AI futures ────────────────────────────────
            print("\nWaiting for remaining AI calls to finish …")
            for fut in as_completed(pending_futures):
                cc_row, abr_row, _ = pending_futures[fut]
                try:
                    approved, confidence = fut.result()
                except Exception as exc:
                    print(f"  [AI ERROR] {exc}")
                    continue
                if approved:
                    self._write_match(_build_record(
                        cc_row, abr_row, "ai_validated", confidence * 100
                    ))
                    total_written += 1

        # ── Final summary ─────────────────────────────────────────────────────
        elapsed = time.perf_counter() - start
        print(
            f"\n{'─' * 55}\n"
            f"  Matching complete\n"
            f"  Total processed  : {total_proc:,}\n"
            f"  Written to DB    : {total_written:,}\n"
            f"    ↳ High conf (>90) : {total_high:,}\n"
            f"    ↳ Med conf (82-90): {total_med:,}\n"
            f"    ↳ AI approved     : {total_written - total_high - total_med:,}\n"
            f"  AI calls made    : {total_ai:,}\n"
            f"  Rejected (< 78)  : {total_rejected:,}\n"
            f"  Thresholds       : AI=78-81 | Med=82-90 | High=>90\n"
            f"  Total time       : {elapsed:.2f}s\n"
            f"{'─' * 55}"
        )

    # ── Entry point ───────────────────────────────────────────────────────────

    def run(self) -> None:
        cc_df, abr_df = self.load_data()
        self.fuzzy_match(cc_df, abr_df)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    matcher = EntityMatcher(
        db_password="firmable",
        ai_workers=6,    # safe sweet spot for 8 GB RAM
        debug=True,
    )
    matcher.run()