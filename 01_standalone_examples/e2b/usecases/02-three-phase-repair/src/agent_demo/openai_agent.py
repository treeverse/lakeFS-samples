"""OpenAI-powered repair code generator."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from openai import OpenAI

from agent_demo.validation import PHASE_RULES, ValidationFailure, ValidationResult


@dataclass
class AttemptContext:
    attempt: int
    repair_code: str
    validation_result: ValidationResult
    e2b_stdout: str
    e2b_stderr: str
    e2b_exit_code: int


def _load_system_prompt() -> str:
    """Read the base system prompt (contract + pandas pitfalls, no phase-specific rules)."""
    prompt_path = Path(__file__).parent.parent.parent / "prompts" / "repair_agent_system_prompt.md"
    return prompt_path.read_text(encoding="utf-8")


def _strip_code_fences(text: str) -> str:
    """Remove markdown code fences if present."""
    text = text.strip()
    match = re.match(r"^```(?:python)?\n(.*?)```$", text, re.DOTALL)
    return match.group(1).strip() if match else text


def generate_repair_code(
    openai_api_key: str,
    model: str,
    current_csv: str,
    current_phase: int,
    validation_failures: list[ValidationFailure],
    previous_attempts: list[AttemptContext],
) -> str:
    """Generate Python repair code targeting the failures in current_phase."""
    system_prompt = _load_system_prompt()

    csv_lines = current_csv.splitlines()
    csv_display = "\n".join(csv_lines[:51]) + ("\n... truncated" if len(csv_lines) > 51 else "")

    failure_lines = [
        f"- Rule: {f.rule}\n  Description: {f.description}\n  Affected rows (1-based): {f.affected_rows}"
        for f in validation_failures
    ]

    user_parts = [
        f"## Current Validation Phase\n{PHASE_RULES[current_phase]}",
        "## Current Data\n```csv\n" + csv_display + "\n```",
        "## Failures to fix\n" + "\n".join(failure_lines),
    ]

    if previous_attempts:
        parts = []
        for ctx in previous_attempts:
            p = (
                f"### Attempt {ctx.attempt} (phase {ctx.validation_result.failed_phase})\n"
                f"**Result:** {ctx.validation_result.summary}\n"
                f"**Exit code:** {ctx.e2b_exit_code}\n"
            )
            if ctx.e2b_stderr.strip():
                p += f"**Stderr:**\n```\n{ctx.e2b_stderr.strip()}\n```\n"
            p += f"**Code used:**\n```python\n{ctx.repair_code}\n```"
            parts.append(p)
        user_parts.append("## Previous attempts\n" + "\n\n".join(parts))

    user_message = "\n\n".join(user_parts)
    user_message += "\n\nWrite repair code that fixes all failures listed above."

    client = OpenAI(api_key=openai_api_key)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
    )

    return _strip_code_fences(response.choices[0].message.content or "")
