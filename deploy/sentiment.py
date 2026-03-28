"""
FinBERT sentiment scoring wrapper.

RULE: The model is loaded at MODULE LEVEL — once on import, not once
per call. Loading inside the function would re-load ~440MB of weights
on every record. Module-level = 6-second pipeline. Inside function = 6
minutes.

Model: ProsusAI/finbert
  Fine-tuned on Financial PhraseBank. Three labels: positive, negative, neutral.
  First import downloads weights to ~/.cache/huggingface/ (~440MB).
  Subsequent runs use the local cache.
"""

import logging
from typing import Any

from transformers import pipeline as hf_pipeline

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level model initialization — runs once when this module is imported.
# Do NOT move this into score_text() or score_batch().
# ---------------------------------------------------------------------------
_finbert = hf_pipeline(
    "text-classification",
    model="ProsusAI/finbert",
    tokenizer="ProsusAI/finbert",
    top_k=1,          # return only the highest-scoring label as list-of-one dict
    truncation=True,  # hard-cut inputs > 512 tokens silently
    max_length=512,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def score_text(text: str) -> dict[str, Any]:
    """Score a single text string with FinBERT.

    Returns a dict with keys:
      - sentiment_label: "positive" | "negative" | "neutral"
      - sentiment_score: float 0.0–1.0

    Returns neutral/0.0 for empty input or on any exception — the pipeline
    must never die because one article had unusual Unicode or was too short.
    """
    if not text or not text.strip():
        return {"sentiment_label": "neutral", "sentiment_score": 0.0}

    try:
        # Pre-slice to 512 chars as a heuristic before tokenization.
        # Chars ≠ tokens, but this prevents obviously oversized inputs.
        # truncation=True at the pipeline level provides the hard token cut.
        # top_k=1 on a single string returns [[{label, score}]]
        # so [0] gives the inner list, [0] again gives the dict
        result = _finbert(text[:512])[0][0]
        return {
            "sentiment_label": result["label"],
            "sentiment_score": round(float(result["score"]), 6),
        }
    except Exception as exc:
        logger.warning("FinBERT scoring failed: %s", exc)
        return {"sentiment_label": "neutral", "sentiment_score": 0.0}


def score_batch(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Score a list of pipeline records in place.

    Concatenates title + summary to give FinBERT more context than either
    field alone. The 512-token truncation handles combined text that is long.

    Mutates each record by adding sentiment_label and sentiment_score.
    Returns the same list (modified in place).
    """
    for record in records:
        text = f"{record.get('title', '')} {record.get('summary', '')}".strip()
        scores = score_text(text)
        record.update(scores)
    return records
