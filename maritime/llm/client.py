"""Client minimal pour l'API HTTP d'Ollama (https://github.com/ollama/ollama/blob/main/docs/api.md).

Ollama sert localement des modèles à poids ouverts (Qwen, Llama, Mistral...) : aucune donnée ne
quitte la machine et aucune clé n'est nécessaire.
"""
from __future__ import annotations

import logging
import time

import requests

log = logging.getLogger(__name__)


class LLMError(RuntimeError):
    """LLM désactivé, injoignable ou réponse inexploitable."""


class OllamaClient:
    def __init__(self, url: str, model: str, timeout_s: float = 180):
        self.url = (url or "").rstrip("/")
        self.model = model
        self.timeout_s = timeout_s
        self._available: tuple[float, bool] | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.url)

    def available(self, cache_s: float = 30) -> bool:
        """Le serveur répond et le modèle est téléchargé (résultat mis en cache)."""
        if not self.enabled:
            return False
        now = time.time()
        if self._available and now - self._available[0] < cache_s:
            return self._available[1]
        ok = False
        try:
            r = requests.get(f"{self.url}/api/tags", timeout=5)
            r.raise_for_status()
            names = {m.get("name") for m in r.json().get("models", [])}
            ok = self.model in names or f"{self.model}:latest" in names
        except (requests.RequestException, ValueError):
            ok = False
        self._available = (now, ok)
        return ok

    def chat(self, messages: list[dict], tools: list[dict] | None = None,
             temperature: float = 0.2) -> dict:
        """Un tour de conversation. Renvoie le message de l'assistant (content, tool_calls...)."""
        if not self.enabled:
            raise LLMError("LLM désactivé (LLM_URL vide)")
        body = {"model": self.model, "messages": messages, "stream": False,
                "options": {"temperature": temperature}}
        if tools:
            body["tools"] = tools
        try:
            r = requests.post(f"{self.url}/api/chat", json=body, timeout=self.timeout_s)
        except requests.RequestException as exc:
            raise LLMError(f"Ollama injoignable : {exc}") from exc
        if r.status_code != 200:
            raise LLMError(f"Ollama a répondu {r.status_code} : {r.text[:200]}")
        try:
            return r.json()["message"]
        except (ValueError, KeyError) as exc:
            raise LLMError("Réponse Ollama illisible") from exc
