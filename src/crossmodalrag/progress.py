from __future__ import annotations

import sys
from typing import Callable

# A progress callback reports (done, total) for a long operation. The library loops accept one as an
# optional parameter (default None = silent), so the core stays UI-agnostic and dependency-free.
ProgressFn = Callable[[int, int], None]


class ProgressReporter:
    """Minimal, dependency-free progress line on stderr (carriage-return updates).

    It is a no-op unless stderr is an interactive TTY, so piped output and the ``--json`` contracts are
    never polluted. Call it as ``reporter(done, total)`` per item; it finalizes with a newline when
    ``done >= total``.
    """

    def __init__(self, label: str, *, stream=None) -> None:
        self.label = label
        self._stream = stream if stream is not None else sys.stderr
        self._active = bool(getattr(self._stream, "isatty", lambda: False)())
        self._done = False

    def __call__(self, done: int, total: int) -> None:
        if not self._active or self._done:
            return
        total = max(total, 0)
        if total <= 0:
            return
        done = min(max(done, 0), total)
        self._stream.write(f"\r{self.label}: {done}/{total}")
        self._stream.flush()
        if done >= total:
            self._stream.write("\n")
            self._stream.flush()
            self._done = True


def make_progress(label: str, *, enabled: bool = True, stream=None) -> ProgressFn | None:
    """Build a progress callback for ``label``, or ``None`` when disabled or stderr is not a TTY.

    Returning ``None`` lets callers pass the result straight through to a library loop's ``progress=``
    parameter with zero overhead and zero output in non-interactive contexts.
    """
    if not enabled:
        return None
    reporter = ProgressReporter(label, stream=stream)
    if not reporter._active:
        return None
    return reporter
