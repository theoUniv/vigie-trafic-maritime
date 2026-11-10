"""Assistant conversationnel : le LLM répond aux questions en appelant des outils sur la base.

Le modèle ne connaît pas les données : il doit appeler les outils (recherche de navires, zones,
alertes, statistiques) dont les résultats proviennent de PostgreSQL. Les outils sont injectés,
ce qui permet de tester la boucle sans base ni LLM.
"""
from __future__ import annotations

import inspect
import json
import logging
from datetime import datetime, timezone
from collections.abc import Iterator
from typing import Callable

from ..ais import CATEGORIES
from ..processing.anomalies import KIND_LABELS
from .client import LLMError

log = logging.getLogger(__name__)

MAX_TOOL_RESULT_CHARS = 3500   # résultats courts : moins de texte à lire pour le modèle sur CPU
MAX_HISTORY = 4

TOOLS = [
    {"type": "function", "function": {
        "name": "search_vessels",
        "description": "Liste des navires précis (nom, MMSI, destination, vitesse) filtrés par nom, type, "
                       "pavillon, état ou destination. Ex. : « quels cargos vont à Helsinki ? », "
                       "« quels navires sont en retard ? » (status=delayed).",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string", "description": "Nom, MMSI, IMO, indicatif ou destination (ex. 'Helsinki', 'FI HEL')"},
            "category": {"type": "string", "enum": list(CATEGORIES), "description": "Type de navire"},
            "country": {"type": "string", "description": "Code pays ISO à 2 lettres du pavillon (ex. FI, EE, SE)"},
            "status": {"type": "string", "enum": ["moving", "stopped", "delayed"],
                       "description": "moving = en route, stopped = à l'arrêt, delayed = en retard"},
            "limit": {"type": "integer", "description": "Nombre maximal de résultats (défaut 15)"},
        }}}},
    {"type": "function", "function": {
        "name": "vessel_details",
        "description": "Fiche complète d'un navire : identité, position, vitesse, destination, "
                       "ETA déclarée et recalculée, dernières anomalies.",
        "parameters": {"type": "object", "required": ["mmsi"], "properties": {
            "mmsi": {"type": "integer", "description": "Identifiant MMSI à 9 chiffres"},
        }}}},
    {"type": "function", "function": {
        "name": "congestion_zones",
        "description": "Zones actuelles de congestion (cargos au mouillage), de ralentissement ou de trafic dense.",
        "parameters": {"type": "object", "properties": {
            "min_level": {"type": "string", "enum": ["dense", "ralentissement", "congestion"],
                          "description": "Niveau minimal (défaut ralentissement)"},
        }}}},
    {"type": "function", "function": {
        "name": "recent_alerts",
        "description": "Événements d'alerte récents (saut de position, arrêt en mer, perte AIS, ralentissement...). "
                       "Ne sert PAS à compter les navires en retard : utiliser traffic_stats.",
        "parameters": {"type": "object", "properties": {
            "hours": {"type": "number", "description": "Période en heures (défaut 2)"},
            "kind": {"type": "string", "enum": list(KIND_LABELS), "description": "Type d'alerte"},
            "limit": {"type": "integer", "description": "Nombre maximal (défaut 20)"},
        }}}},
    {"type": "function", "function": {
        "name": "traffic_stats",
        "description": "Chiffres globaux actuels. À utiliser pour toute question « combien » : navires actifs, "
                       "en route, au mouillage, à quai, EN RETARD, par type, zones saturées, alertes de la dernière heure. "
                       "Aussi pour « résume la situation ».",
        "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {
        "name": "latest_briefing",
        "description": "Dernier bulletin de situation rédigé automatiquement.",
        "parameters": {"type": "object", "properties": {}}}},
]

SYSTEM_PROMPT = """Tu es l'assistant de Vigie, un outil de surveillance du trafic maritime en temps réel
(données AIS open data, principalement en mer Baltique). Heure actuelle : {now} UTC.

Règles :
- Réponds toujours en français, de façon concise : quelques phrases, ou une liste de 5 éléments
  maximum avec UNE ligne par élément. Pas de gras ni de titres markdown.
- Choix de l'outil : « combien ... ? » ou « résume » → traffic_stats ; « quels navires / quels cargos ... ? »
  (une liste de navires) → search_vessels avec les bons filtres (category, status, query) ; zones → congestion_zones.
- Pour toute question sur les navires, zones, alertes ou chiffres, appelle d'abord les outils.
  N'invente jamais de navire, de chiffre ni de position : appuie-toi uniquement sur leurs résultats.
- Quand tu cites un navire, donne son nom suivi de son MMSI entre parenthèses, ex. « VIKING GRACE (230629000) ».
- Les vitesses sont en nœuds (nd). Si les outils ne renvoient rien, dis-le simplement.
- Si la question ne concerne pas le trafic maritime, réponds poliment que tu es limité à ce sujet."""

ToolFn = Callable[..., object]


def _parse_args(raw) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def _to_json(value) -> str:
    text = json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
    if len(text) > MAX_TOOL_RESULT_CHARS:
        text = text[:MAX_TOOL_RESULT_CHARS] + "…(tronqué)"
    return text


class Assistant:
    def __init__(self, client, tools: dict[str, ToolFn], max_rounds: int = 4):
        self.client = client
        self.tools = tools
        self.max_rounds = max_rounds

    def run_tool(self, name: str, args: dict) -> str:
        fn = self.tools.get(name)
        if fn is None:
            return _to_json({"erreur": f"outil inconnu : {name}"})
        # Les petits modèles inventent souvent des arguments (ex. « kind » pour traffic_stats) :
        # on ignore ceux que l'outil n'accepte pas plutôt que d'échouer.
        params = inspect.signature(fn).parameters
        if not any(p.kind is p.VAR_KEYWORD for p in params.values()):
            args = {k: v for k, v in args.items() if k in params}
        try:
            return _to_json(fn(**args))
        except TypeError as exc:      # arguments inventés par le modèle
            return _to_json({"erreur": f"arguments invalides : {exc}"})
        except Exception as exc:      # noqa: BLE001 — l'erreur est renvoyée au modèle
            log.exception("Outil %s en échec", name)
            return _to_json({"erreur": str(exc)})

    def _messages(self, question: str, history: list[dict] | None) -> list[dict]:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        messages = [{"role": "system", "content": SYSTEM_PROMPT.format(now=now)}]
        for h in (history or [])[-MAX_HISTORY:]:
            if h.get("role") in ("user", "assistant") and isinstance(h.get("content"), str):
                messages.append({"role": h["role"], "content": h["content"][:2000]})
        messages.append({"role": "user", "content": question})
        return messages

    def _round(self, messages: list[dict], tools) -> Iterator[dict]:
        """Un appel au modèle en flux. Émet les jetons de texte puis un résumé du tour."""
        content, calls = "", []
        for part in self.client.chat_stream(messages, tools=tools):
            if part.get("content"):
                content += part["content"]
                yield {"type": "token", "text": part["content"]}
            calls.extend(part.get("tool_calls") or [])
        yield {"type": "_round", "content": content, "calls": calls}

    def ask_stream(self, question: str, history: list[dict] | None = None) -> Iterator[dict]:
        """Événements : token (texte au fil de l'eau), reset (texte à effacer), tool, done."""
        messages = self._messages(question, history)
        calls_log: list[dict] = []
        for attempt in range(self.max_rounds + 1):
            final = attempt == self.max_rounds
            if final:   # trop d'appels d'outils : on force une réponse avec ce qui a été collecté
                messages.append({"role": "user", "content": "Réponds maintenant avec les informations obtenues."})
            for ev in self._round(messages, None if final else TOOLS):
                if ev["type"] != "_round":
                    yield ev
                    continue
                content, calls = ev["content"], ev["calls"]
            if not calls or final:
                answer = content.strip()
                if not answer:
                    raise LLMError("Le modèle n'a pas produit de réponse")
                yield {"type": "done", "answer": answer, "tool_calls": calls_log}
                return
            if content:
                yield {"type": "reset"}   # texte émis avant un appel d'outil : ce n'était pas la réponse
            messages.append({"role": "assistant", "content": content, "tool_calls": calls})
            for call in calls:
                fn = call.get("function", {})
                name, args = fn.get("name", ""), _parse_args(fn.get("arguments"))
                calls_log.append({"name": name, "arguments": args})
                yield {"type": "tool", "name": name, "arguments": args}
                messages.append({"role": "tool", "tool_name": name, "content": self.run_tool(name, args)})

    def ask(self, question: str, history: list[dict] | None = None) -> dict:
        """Version non diffusée : renvoie directement la réponse finale."""
        for ev in self.ask_stream(question, history):
            if ev["type"] == "done":
                return {"answer": ev["answer"], "tool_calls": ev["tool_calls"]}
        raise LLMError("Le modèle n'a pas produit de réponse")
