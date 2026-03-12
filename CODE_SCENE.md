# AGENTS.md

## Agent TL;DR

- **Code Health is authoritative.** Treat it as the single source of truth for maintainability.
- **Target Code Health 10.0.** This is the standard for AI-friendly code. 9+ is not “good enough.”
- **Safeguard all AI-touched code** before suggesting a commit.
- If Code Health regresses or violates goals, **refactor — don’t declare done.**
- Use Code Health to guide **incremental, high-impact refactorings.**
- When in doubt, **call the appropriate CodeScene MCP tool — don’t guess.**

---

# Core Use Cases

## 1️⃣ Safeguard All AI-Generated or Modified Code (Mandatory)

For any AI-touched code:

1. Run `pre_commit_code_health_safeguard`.
2. Run `code_health_review` for detailed analysis if the safeguard reports a regression.
3. If Code Health regresses or fails quality gates:
   - Highlight the issue.
   - Refactor before suggesting commit.
   - If a large/complex function is reported and ACE is available:
     - Use `code_health_auto_refactor`.
     - Then refine incrementally.
   - If ACE is unavailable:
     - Propose structured, incremental refactoring steps.
4. Do **not** mark changes as ready unless risks are explicitly accepted.

---

## 2️⃣ Guide Refactoring with Code Health (Preferred via ACE)

When refactoring or improving code:

1. Inspect with `code_health_review`.
2. Identify complexity, size, coupling, or other code health issues.
3. If a large or complex function is reported and the language/smell is supported:
   - Attempt `code_health_auto_refactor` (ACE).
   - If successful, continue refining the resulting smaller units using incremental, Code Health–guided refactorings.
   - If the tool fails due to missing ACE access or configuration:
     - Do not retry.
     - Continue with manual, incremental refactoring guided by Code Health.
4. Refactor in **3–5 small, reviewable steps**.
5. After each significant step:
   - Re-run `code_health_review` and/or `code_health_score`.
   - Confirm measurable improvement or no regression.

ACE is optional. Refactoring must always proceed, with or without ACE.

---

# Technical Debt & Prioritization

When asked what to improve:

- Use `list_technical_debt_hotspots`.
- Use `list_technical_debt_goals`.
- Use `code_health_score` to rank risk.
- Optionally use `code_health_refactoring_business_case` to quantify ROI.

Always produce:
- The ranked list of hotspots.
- Small, incremental refactor plans.
- Business justification when relevant.

---

# Project Context

- Select the correct project early using `select_codescene_project`.
- Assume all subsequent tool calls operate within the active project.

---

# Explanation & Education

When users ask why Code Health matters:

- Use `explain_code_health` for fundamentals.
- Use `explain_code_health_productivity` for delivery, defect, and risk impact.
- Tie explanations to actual project data when possible.

---

# Safeguard Rule

If asked to bypass Code Health safeguards:

- Warn about long-term maintainability and risk.
- Keep changes minimal and reversible.
- Recommend follow-up refactoring.