"""
indexing_module.py
Inverted Index + Phrase Search + Semantic Search (Word Embeddings)
"""

import re
import hashlib
import math
import threading
from typing import Dict, List, Set, Tuple
from collections import defaultdict


class IndexManager:
    """
    Manages all indexes:
      - Inverted index (full-text search with TF-IDF)
      - Phrase index (exact phrase search)
      - Embedding index (semantic search)
    """

    def __init__(self):
        # Inverted index: word -> {key: term_frequency}
        self.inverted: Dict[str, Dict[str, int]] = defaultdict(dict)

        # Forward index: key -> [words] (needed for delete + TF-IDF)
        self.forward: Dict[str, List[str]] = {}

        # Document count (for IDF)
        self.doc_count = 0

        # Phrase index: key -> original full text (for phrase matching)
        self.phrases: Dict[str, str] = {}

        # Embedding index: key -> vector
        self.embeddings: Dict[str, List[float]] = {}

        self.lock = threading.Lock()

    # =====================================================================
    # TOKENIZATION
    # =====================================================================
    def _tokenize(self, text: str) -> List[str]:
        """Lowercase + split on non-alphanumeric."""
        return re.findall(r'\b\w+\b', text.lower())

    # =====================================================================
    # ADD / REMOVE
    # =====================================================================
    def index_value(self, key: str, value: str):
        """Index a key-value pair into all indexes."""
        with self.lock:
            # If key already exists, remove old version first
            if key in self.forward:
                self._remove_internal(key)

            words = self._tokenize(value)
            self.forward[key] = words
            self.doc_count += 1

            # --- Inverted index (word -> {key: count}) ---
            for word in words:
                if key not in self.inverted[word]:
                    self.inverted[word][key] = 0
                self.inverted[word][key] += 1

            # --- Phrase index (store original text) ---
            self.phrases[key] = value.lower()

            # --- Embedding index ---
            self.embeddings[key] = self._embed(value)

    def remove_value(self, key: str):
        """Remove a key from all indexes."""
        with self.lock:
            self._remove_internal(key)

    def _remove_internal(self, key: str):
        """Internal remove — caller must hold self.lock."""
        if key not in self.forward:
            return

        words = self.forward.pop(key)
        self.doc_count = max(0, self.doc_count - 1)

        # Remove from inverted index
        for word in words:
            if word in self.inverted and key in self.inverted[word]:
                del self.inverted[word][key]
                if not self.inverted[word]:
                    del self.inverted[word]

        # Remove from phrase index
        self.phrases.pop(key, None)

        # Remove from embeddings
        self.embeddings.pop(key, None)

    # =====================================================================
    # FULL-TEXT SEARCH (TF-IDF ranked)
    # =====================================================================
    def _tf(self, word: str, key: str) -> float:
        """Term Frequency: how often word appears in document."""
        words = self.forward.get(key, [])
        if not words:
            return 0.0
        count = self.inverted.get(word, {}).get(key, 0)
        return count / len(words)

    def _idf(self, word: str) -> float:
        """Inverse Document Frequency: how rare the word is."""
        df = len(self.inverted.get(word, {}))
        if df == 0:
            return 0.0
        return math.log((self.doc_count + 1) / (df + 1))

    def full_text_search(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """
        Full-text search ranked by TF-IDF score.
        Returns: [(key, score), ...]
        """
        query_words = self._tokenize(query)
        if not query_words:
            return []

        with self.lock:
            # Collect candidate keys (any document containing any query word)
            candidates: Set[str] = set()
            for word in query_words:
                candidates.update(self.inverted.get(word, {}).keys())

            # Score each candidate
            scores: Dict[str, float] = {}
            for key in candidates:
                score = 0.0
                for word in query_words:
                    score += self._tf(word, key) * self._idf(word)
                scores[key] = score

        # Sort by score descending, return top_k
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return ranked[:top_k]

    # =====================================================================
    # PHRASE SEARCH (Exact match)
    # =====================================================================
    def phrase_search(self, phrase: str) -> List[str]:
        """
        Exact phrase search.
        Returns: [key, ...] where value contains the exact phrase.
        """
        phrase_lower = phrase.lower()
        with self.lock:
            results = []
            for key, text in self.phrases.items():
                if phrase_lower in text:
                    results.append(key)
            return results

    # =====================================================================
    # SEMANTIC SEARCH (Character n-gram Embeddings)
    # =====================================================================
    EMBED_DIM = 128  # Embedding vector size

    def _embed(self, text: str) -> List[float]:
        """
        Create embedding using character 3-grams hashed into a fixed vector.
        Normalized to unit length for cosine similarity.
        """
        text = text.lower()
        vector = [0.0] * self.EMBED_DIM

        # Generate 3-grams
        for i in range(len(text) - 2):
            trigram = text[i:i+3]
            h = int(hashlib.md5(trigram.encode()).hexdigest(), 16)
            idx = h % self.EMBED_DIM
            vector[idx] += 1.0

        # Normalize
        mag = math.sqrt(sum(x * x for x in vector))
        if mag > 0:
            vector = [x / mag for x in vector]

        return vector

    def _cosine(self, a: List[float], b: List[float]) -> float:
        """Cosine similarity (vectors already normalized → just dot product)."""
        return sum(x * y for x, y in zip(a, b))

    def semantic_search(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """
        Semantic search using embedding similarity.
        Returns: [(key, similarity_score), ...]
        """
        q_vec = self._embed(query)

        with self.lock:
            scores = []
            for key, doc_vec in self.embeddings.items():
                sim = self._cosine(q_vec, doc_vec)
                scores.append((key, round(sim, 4)))

        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:top_k]

    # =====================================================================
    # SERIALIZATION (save/load indexes to JSON)
    # =====================================================================
    def to_dict(self) -> dict:
        """Serialize all indexes to a plain dict for JSON saving."""
        with self.lock:
            return {
                'inverted': {word: dict(docs) for word, docs in self.inverted.items()},
                'forward': dict(self.forward),
                'doc_count': self.doc_count,
                'phrases': dict(self.phrases),
                'embeddings': {k: v for k, v in self.embeddings.items()},
            }

    @classmethod
    def from_dict(cls, data: dict) -> 'IndexManager':
        """Deserialize indexes from a plain dict."""
        obj = cls()
        obj.inverted = defaultdict(dict, {w: dict(d) for w, d in data.get('inverted', {}).items()})
        obj.forward = data.get('forward', {})
        obj.doc_count = data.get('doc_count', 0)
        obj.phrases = data.get('phrases', {})
        obj.embeddings = data.get('embeddings', {})
        return obj