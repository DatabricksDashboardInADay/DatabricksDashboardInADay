# Project Rules

## Project Context and Target Audience

This repository is a **hands-on workshop** ("Databricks Dashboard in a Day") for Databricks.

- **Target audience:** business analysts, data engineers, and technical users attending an instructor-led lab session — relevant for all levels from interns to leadership. No SQL or Python usage required.
- Labs guide participants step-by-step through building a data pipeline, metric views, dashboards, and conversational BI on Databricks.
- Tone should be clear, encouraging, and beginner-friendly. Avoid jargon unless it is explained.

## Lab File Structure

Each lab markdown file in `labs/` follows a consistent structure:

- **Title** — H1 heading with emoji and lab name.
- **Learning Objectives** — bulleted list of what participants will achieve.
- **Introduction** — brief explanation of the concept being taught.
- **Instructions** — the core of the lab, organized as:
  - **Bold Step headers** (e.g., `**Step 1: Do Something**`) — high-level milestones.
  - **Numbered tasks** under each step — specific actions the participant performs (1, 2, 3...).
  - **Screenshots** after relevant tasks using `<div><img></div>` blocks.
  - **Code blocks** with example SQL/Python when participants need to write code.
- **Final Steps / What Happens Next** — summary and transition to the next lab.

## Markdown Formatting Rules

- Each numbered task must be separated by a **blank line** so it renders as a distinct list item.
- After every `</div>` (screenshot block), add a **blank line** before the next numbered task.
- Screenshots use the pattern:
  ```html
  <div style="text-align:left;">
    <img src="./artifacts/screenshots/FILENAME.png" width="XX%">
  </div>
  ```
- Code blocks use fenced triple backticks with a language tag (`sql`, `python`).
- Do not add trailing whitespace or empty cells/sections.
- Use GitHub-style alerts for callouts:
  - `> [!NOTE]` — Highlights information that users should take into account, even when skimming.
  - `> [!TIP]` — Helpful advice for doing things better or more easily.
  - `> [!IMPORTANT]` — Crucial information necessary for users to succeed.
  - `> [!WARNING]` — Critical content demanding immediate user attention due to potential risks.
  - `> [!CAUTION]` — Advises about risks or negative outcomes of certain actions.
