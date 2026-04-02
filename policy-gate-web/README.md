# Policy Gate Web

Web interface for the Policy Gate code policy reviewer, powered by the
**GitHub Copilot SDK**.

Users can either:
- **Paste a GitHub repository URL** — the app clones it and runs the review
- **Upload a ZIP archive** — the app extracts it and runs the review

The review is performed by the Copilot SDK agent using the same 7 policies
defined in the org-level `.github` repository.

## Prerequisites

- **Node.js** ≥ 22.5 (required by `@github/copilot-sdk` for `node:sqlite`)
- **GitHub Copilot CLI** installed and authenticated (`gh copilot` / `copilot-cli`)
- **Git** (for cloning repositories)

## Setup

```bash
cd policy-gate-web
npm install
```

## Running

```bash
npm start
# → 🚀 Policy Gate Web running at http://localhost:3000
```

For development with auto-reload:
```bash
npm run dev
```

## How It Works

1. User submits a repo URL or uploads a ZIP
2. Server extracts/clones the code into a temp directory
3. All `.py` files are collected and sent as context to the Copilot SDK agent
4. The agent reviews every file against all 7 policies
5. A formatted markdown report is returned and rendered in the browser

## Architecture

```
Browser (index.html)
  ↓ POST /api/review/repo   or   POST /api/review/upload
Server (server.mjs)
  ↓ Clone/extract → collect .py files → build prompt
Copilot SDK (@github/copilot-sdk)
  ↓ createSession → sendAndWait with policy review prompt
  ↓ Returns structured markdown report
Server → Browser (rendered report)
```
