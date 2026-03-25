"""
NLP analysis of video transcriptions — topic clustering, keyword extraction,
per-video summaries. Uses scikit-learn (no external API required for clustering).
"""
import json
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


def extract_keywords(text: str, top_n: int = 20) -> list[str]:
    """TF-IDF keyword extraction from a single text."""
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        import numpy as np
        vec = TfidfVectorizer(
            stop_words="english",
            max_features=500,
            ngram_range=(1, 2),
        )
        tfidf = vec.fit_transform([text])
        scores = tfidf.toarray()[0]
        feature_names = vec.get_feature_names_out()
        top_indices = np.argsort(scores)[::-1][:top_n]
        return [feature_names[i] for i in top_indices if scores[i] > 0]
    except ImportError:
        # Fallback: simple word frequency
        import re
        from collections import Counter
        words = re.findall(r"\b[a-z]{4,}\b", text.lower())
        stopwords = {"this", "that", "with", "have", "from", "they", "will", "been", "were", "their", "what", "when", "your", "also"}
        filtered = [w for w in words if w not in stopwords]
        return [w for w, _ in Counter(filtered).most_common(top_n)]


def cluster_topics(transcripts: list[str], n_clusters: int = 5) -> dict:
    """K-means clustering on TF-IDF vectors. Returns cluster labels + top terms."""
    if not transcripts:
        return {"clusters": [], "labels": []}
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.cluster import KMeans
        import numpy as np

        n_clusters = min(n_clusters, len(transcripts))
        vec = TfidfVectorizer(stop_words="english", max_features=200)
        X = vec.fit_transform(transcripts)
        km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        labels = km.fit_predict(X)
        feature_names = vec.get_feature_names_out()

        clusters = []
        for c in range(n_clusters):
            center = km.cluster_centers_[c]
            top_terms = [feature_names[i] for i in np.argsort(center)[::-1][:5]]
            count = int(np.sum(labels == c))
            clusters.append({
                "id": c,
                "top_terms": top_terms,
                "label": " / ".join(top_terms[:3]),
                "count": count,
                "percentage": round(count / len(transcripts) * 100, 1),
            })
        return {"clusters": clusters, "labels": labels.tolist()}
    except ImportError:
        logger.warning("scikit-learn not installed — skipping topic clustering")
        return {"clusters": [], "labels": []}


def summarize_video(transcript: str, title: str = "", api_key: Optional[str] = None) -> str:
    """Generate a 2-sentence summary using Claude API (falls back to extractive summary)."""
    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if key and transcript.strip():
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=key)
            prompt = (
                f"Video title: {title}\n\nTranscript excerpt:\n{transcript[:3000]}\n\n"
                "Provide a concise 2-sentence summary of what this video is about."
            )
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}],
            )
            return msg.content[0].text.strip()
        except Exception as e:
            logger.warning("Claude summary failed: %s", e)

    # Extractive fallback: first 2 sentences
    import re
    sentences = re.split(r"(?<=[.!?])\s+", transcript.strip())
    return " ".join(sentences[:2]) if sentences else transcript[:200]


def generate_report(transcripts_by_id: dict[str, dict], api_key: Optional[str] = None) -> dict:
    """
    Generate a full market trends report.
    transcripts_by_id: {video_id: {"title": str, "transcript": str}}
    Returns report dict.
    """
    all_transcripts = [v["transcript"] for v in transcripts_by_id.values() if v.get("transcript")]
    all_text = " ".join(all_transcripts)

    global_keywords = extract_keywords(all_text, top_n=30)
    topic_data = cluster_topics(all_transcripts)

    per_video = {}
    for vid_id, meta in transcripts_by_id.items():
        t = meta.get("transcript", "")
        if not t:
            continue
        per_video[vid_id] = {
            "title": meta.get("title", vid_id),
            "keywords": extract_keywords(t, top_n=10),
            "summary": summarize_video(t, title=meta.get("title", ""), api_key=api_key),
        }

    return {
        "total_videos_analyzed": len(all_transcripts),
        "global_keywords": global_keywords,
        "topic_clusters": topic_data.get("clusters", []),
        "per_video": per_video,
    }
