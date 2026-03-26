#!/usr/bin/env python3
"""
Microservicio FAQ con IA ligera (recuperación semántica local).

Arranque:
  uvicorn servicios.api_faq_ia:app --host 0.0.0.0 --port 8091
"""
from __future__ import annotations

import json
import os
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI
from pydantic import BaseModel, Field

from config import PROJECT_DESCRIPTION, PROJECT_DISPLAY_NAME, PROJECT_SLUG

KB_PATH = Path(__file__).resolve().parent / "faq_knowledge_base.json"


class FAQAskRequest(BaseModel):
    question: str = Field(..., min_length=3, description="Pregunta en lenguaje natural")
    top_k: int = Field(default=3, ge=1, le=10, description="Máximo de sugerencias")


class FAQAskResponse(BaseModel):
    answer: str
    confidence: float
    matched_question: str
    suggestions: List[str]
    sources: List[str]
    engine: str


def _tokenizar(texto: str) -> List[str]:
    t = re.sub(r"[^a-zA-Z0-9áéíóúÁÉÍÓÚñÑüÜ ]+", " ", (texto or "").lower())
    return [x for x in t.split() if len(x) > 1]


def _cargar_kb() -> List[Dict[str, Any]]:
    if not KB_PATH.exists():
        return []
    try:
        raw = json.loads(KB_PATH.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict) and x.get("question") and x.get("answer")]
    except Exception:
        return []
    return []


def _score(pregunta: str, item: Dict[str, Any]) -> float:
    q = pregunta.strip().lower()
    base = f"{item.get('question', '')} {item.get('keywords', '')}".lower()
    q_tokens = set(_tokenizar(q))
    b_tokens = set(_tokenizar(base))
    if not q_tokens or not b_tokens:
        return 0.0
    jaccard = len(q_tokens & b_tokens) / max(len(q_tokens | b_tokens), 1)
    seq = SequenceMatcher(None, q, base).ratio()
    bonus = 0.15 if q in base else 0.0
    return round((0.6 * jaccard) + (0.4 * seq) + bonus, 4)


def _resolver_faq(pregunta: str, top_k: int = 3) -> FAQAskResponse:
    kb = _cargar_kb()
    if not kb:
        return FAQAskResponse(
            answer="No hay base FAQ configurada todavía. Añade entradas en `servicios/faq_knowledge_base.json`.",
            confidence=0.0,
            matched_question="",
            suggestions=[],
            sources=["servicios/faq_knowledge_base.json"],
            engine="semantic-fallback-v1",
        )

    scored = []
    for item in kb:
        scored.append((item, _score(pregunta, item)))
    scored.sort(key=lambda x: x[1], reverse=True)
    best, conf = scored[0]
    sugerencias = [x[0].get("question", "") for x in scored[1 : 1 + max(1, top_k)] if x[0].get("question")]

    if conf < 0.22:
        return FAQAskResponse(
            answer=(
                "No encuentro una coincidencia alta en el FAQ. "
                "Reformula la pregunta o consulta las sugerencias disponibles."
            ),
            confidence=float(conf),
            matched_question=best.get("question", ""),
            suggestions=sugerencias,
            sources=[str(KB_PATH)],
            engine="semantic-fallback-v1",
        )

    return FAQAskResponse(
        answer=str(best.get("answer", "")),
        confidence=float(conf),
        matched_question=str(best.get("question", "")),
        suggestions=sugerencias,
        sources=list(best.get("sources", [])) or [str(KB_PATH)],
        engine="semantic-fallback-v1",
    )


app = FastAPI(
    title=f"{PROJECT_DISPLAY_NAME} — FAQ IA API",
    description=(
        f"{PROJECT_DESCRIPTION}\n\n"
        "Microservicio FAQ con recuperación semántica local para resolver preguntas frecuentes "
        "sobre operación del stack, consultas e informes."
    ),
    version="1.0.0",
    openapi_tags=[
        {"name": "sistema", "description": "Salud y metadatos del servicio FAQ"},
        {"name": "faq", "description": "Preguntas frecuentes con motor semántico"},
    ],
)


@app.get("/health", tags=["sistema"], summary="Liveness del microservicio FAQ IA")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": f"{PROJECT_SLUG}_faq_ia"}


@app.get("/api/v1/faq/questions", tags=["faq"], summary="Listado de preguntas FAQ disponibles")
def faq_questions() -> Dict[str, Any]:
    kb = _cargar_kb()
    return {
        "count": len(kb),
        "questions": [x.get("question", "") for x in kb],
        "kb_path": str(KB_PATH),
    }


@app.post("/api/v1/faq/ask", response_model=FAQAskResponse, tags=["faq"], summary="Pregunta al FAQ IA")
def faq_ask(payload: FAQAskRequest) -> FAQAskResponse:
    return _resolver_faq(payload.question, top_k=payload.top_k)


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("SIMLOG_PORT_FAQ_IA", "8091"))
    uvicorn.run("servicios.api_faq_ia:app", host="0.0.0.0", port=port, reload=False)
