"""Assistant conversationnel : le LLM répond aux questions en appelant des outils sur la base.

Le modèle ne connaît pas les données : il doit appeler les outils (recherche de navires, zones,
alertes, statistiques) dont les résultats proviennent de PostgreSQL. Les outils sont injectés,
ce qui permet de tester la boucle sans base ni LLM.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Callable

from ..ais import CATEGORIES
from ..processing.anomalies import KIND_LABELS
from .client import LLMError

log = logging.getLogger(__name__)

MAX_TOOL_RESULT_CHARS = 6000
MAX_HISTORY = 6

TOOLS = [
    {"type": "function", "function": {
        "name": "search_vessels",
        "description": "Cherche des navires actifs (dernière position) avec filtres. Utiliser pour "
                       "lister, compter ou trouver des navires par nom, type, pavillon, état ou destination.",
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
        "description": "Alertes récentes détectées par le pipeline (anomalies, retards, congestion).",
        "parameters": {"type": "object", "properties": {
            "hours": {"type": "number", "description": "Période en heures (défaut 2)"},
            "kind": {"type": "string", "enum": list(KIND_LABELS), "description": "Type d'alerte"},
            "limit": {"type": "integer", "description": "Nombre maximal (défaut 20)"},
        }}}},
    {"type": "function", "function": {
        "name": "traffic_stats",
        "description": "Indicateurs globaux : navires actifs, en route, au mouillage, en retard, "
                       "répartition par type, anomalies sur 24 h.",
        "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {
        "name": "latest_briefing",
        "description": "Dernier bulletin de situation rédigé automatiquement.",
        "parameters": {"type": "object", "properties": {}}}},
]

SYSTEM_PROMPT = """Tu es l'assistant de Vigie, un outil de surveillance du trafic maritime en temps réel
(données AIS open data, principalement en mer Baltique). Heure actuelle : {now} UTC.

Règles :
- Réponds toujours en français, de façon concise (quelques phrases ou une courte liste).
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
        try:
            return _to_json(fn(**args))
        except TypeError as exc:      # arguments inventés par le modèle
            return _to_json({"erreur": f"arguments invalides : {exc}"})
        except Exception as exc:      # noqa: BLE001 — l'erreur est renvoyée au modèle
            log.exception("Outil %s en échec", name)
            return _to_json({"erreur": str(exc)})

    def ask(self, question: str, history: list[dict] | None = None) -> dict:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        messages = [{"role": "system", "content": SYSTEM_PROMPT.format(now=now)}]
        for h in (history or [])[-MAX_HISTORY:]:
            if h.get("role") in ("user", "assistant") and isinstance(h.get("content"), str):
                messages.append({"role": h["role"], "content": h["content"][:2000]})
        messages.append({"role": "user", "content": question})

        calls_log: list[dict] = []
        for _ in range(self.max_rounds):
            msg = self.client.chat(messages, tools=TOOLS)
            calls = msg.get("tool_calls") or []
            if not calls:
                return {"answer": (msg.get("content") or "").strip(), "tool_calls": calls_log}
            messages.append({"role": "assistant", "content": msg.get("content") or "", "tool_calls": calls})
            for call in calls:
                fn = call.get("function", {})
                name, args = fn.get("name", ""), _parse_args(fn.get("arguments"))
                calls_log.append({"name": name, "arguments": args})
                messages.append({"role": "tool", "tool_name": name, "content": self.run_tool(name, args)})

        # Trop d'appels d'outils : on force une réponse avec ce qui a été collecté
        msg = self.client.chat(messages + [{"role": "user", "content":
                                            "Réponds maintenant avec les informations obtenues."}])
        answer = (msg.get("content") or "").strip()
        if not answer:
            raise LLMError("Le modèle n'a pas produit de réponse")
        return {"answer": answer, "tool_calls": calls_log}
