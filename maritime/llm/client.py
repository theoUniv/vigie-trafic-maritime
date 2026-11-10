"""Client minimal pour l'API HTTP d'Ollama (https://github.com/ollama/ollama/blob/main/docs/api.md).

Ollama sert localement des modèles à poids ouverts (Qwen, Llama, Mistral...) : aucune donnée ne
quitte la machine et aucune clé n'est nécessaire.
"""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator

import requests

log = logging.getLogger(__name__)

KEEP_ALIVE = "24h"     # le modèle reste en mémoire : pas de rechargement entre deux requêtes
NUM_CTX = 8192         # contexte suffisant pour les définitions d'outils et leurs résultats


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

    def _body(self, messages, tools, temperature, stream) -> dict:
        if not self.enabled:
            raise LLMError("LLM désactivé (LLM_URL vide)")
        body = {"model": self.model, "messages": messages, "stream": stream,
                "keep_alive": KEEP_ALIVE,
                "options": {"temperature": temperature, "num_ctx": NUM_CTX}}
        if tools:
            body["tools"] = tools
        return body

    def _post(self, body: dict, stream: bool) -> requests.Response:
        try:
            r = requests.post(f"{self.url}/api/chat", json=body, timeout=self.timeout_s, stream=stream)
        except requests.RequestException as exc:
            raise LLMError(f"Ollama injoignable : {exc}") from exc
        if r.status_code != 200:
            raise LLMError(f"Ollama a répondu {r.status_code} : {r.text[:200]}")
        return r

    def chat(self, messages: list[dict], tools: list[dict] | None = None,
             temperature: float = 0.2) -> dict:
        """Un tour de conversation. Renvoie le message de l'assistant (content, tool_calls...)."""
        r = self._post(self._body(messages, tools, temperature, False), stream=False)
        try:
            return r.json()["message"]
        except (ValueError, KeyError) as exc:
            raise LLMError("Réponse Ollama illisible") from exc

    def chat_stream(self, messages: list[dict], tools: list[dict] | None = None,
                    temperature: float = 0.2) -> Iterator[dict]:
        """Même chose en flux : un fragment de message par morceau généré (content et/ou tool_calls)."""
        r = self._post(self._body(messages, tools, temperature, True), stream=True)
        try:
            for line in r.iter_lines():
                if not line:
                    continue
                try:
                    chunk = json.loads(line)
                except ValueError as exc:
                    raise LLMError("Flux Ollama illisible") from exc
                if chunk.get("error"):
                    raise LLMError(chunk["error"])
                yield chunk.get("message") or {}
                if chunk.get("done"):
                    break
        except requests.RequestException as exc:
            raise LLMError(f"Flux Ollama interrompu : {exc}") from exc
        finally:
            r.close()

    def warmup(self) -> bool:
        """Charge le modèle en mémoire à l'avance (évite la latence de la première question)."""
        # Mêmes options (num_ctx) que les vraies requêtes, sinon Ollama rechargerait le modèle.
        if not self.enabled:
            return False
        try:
            self._post(self._body([], None, 0.2, False), stream=False)
            return True
        except LLMError:
            return False
