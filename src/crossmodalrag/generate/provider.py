from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Protocol, runtime_checkable

from crossmodalrag.config import (
    get_llm_base_url,
    get_llm_model,
    get_llm_provider_name,
    get_llm_timeout,
)


class LLMUnavailable(RuntimeError):
    """Raised when the configured LLM backend can't be reached or errors out.

    Callers (e.g. ``mem ask``) catch this to degrade gracefully to the
    deterministic, non-LLM evidence template.
    """


@runtime_checkable
class LLMProvider(Protocol):
    name: str

    def generate(self, prompt: str, system: str | None = None) -> str:
        ...


class OllamaProvider:
    """Local LLM via the Ollama HTTP API (stdlib urllib, no new dependency)."""

    def __init__(
        self,
        model: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
    ) -> None:
        self.name = model or get_llm_model()
        self.base_url = (base_url or get_llm_base_url()).rstrip("/")
        self.timeout = timeout if timeout is not None else get_llm_timeout()

    def generate(self, prompt: str, system: str | None = None) -> str:
        payload: dict[str, object] = {
            "model": self.name,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0},
        }
        if system:
            payload["system"] = system

        request = urllib.request.Request(
            f"{self.base_url}/api/generate",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read().decode("utf-8", errors="replace")
        except (urllib.error.URLError, OSError) as exc:
            raise LLMUnavailable(
                f"Could not reach Ollama at {self.base_url} (model '{self.name}'): {exc}. "
                "Is `ollama serve` running and the model pulled?"
            ) from exc

        try:
            data = json.loads(body)
        except json.JSONDecodeError as exc:
            raise LLMUnavailable(f"Unexpected non-JSON response from Ollama: {exc}") from exc

        if "error" in data:
            raise LLMUnavailable(f"Ollama error: {data['error']}")
        return str(data.get("response", "")).strip()


def get_default_llm_provider(model: str | None = None) -> LLMProvider | None:
    """Return the configured LLM provider, or ``None`` for an unknown backend.

    Construction is cheap and does not open a connection; reachability is
    determined when ``generate`` is first called (raising ``LLMUnavailable``).
    Pass ``model`` to override the configured model (e.g. a faster extraction model).
    """
    provider_name = get_llm_provider_name()
    if provider_name == "ollama":
        return OllamaProvider(model=model)
    return None
