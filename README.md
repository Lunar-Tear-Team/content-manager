# Content Manager

A standalone companion tool for [lunar-tear](https://github.com/Lunar-Tear-Team/lunar-tear) that lets you control which gacha banners, events, login bonuses, and side stories are active in the game client — without restarting the server.

## The Problem

The End-of-Service database contains **3+ years** of live-service content: 80+ gacha banners, 140+ side stories, dozens of events and login bonuses. Loading all of it at once causes severe UI lag, memory bloat, and menu freezing because the mobile client was never designed to render that volume simultaneously.

## How It Works

The Content Manager acts as a **time machine** for the game's master data:

1. **You pick which months to enable** via a web dashboard (presets or manual toggles).
2. The tool **binary-patches** `database.bin.e` in-place, setting `EndDatetime` fields to expired (2020) or active (2030) for content outside or inside your selection.
3. It **pings lunar-tear** via a webhook so the game server reloads its gacha filter and updates the client's cache version — the mobile client seamlessly redownloads the pruned database on next title screen.

```
┌──────────────┐       POST /api/schedule        ┌──────────────┐
│   Browser    │ ──────────────────────────────►  │   Content    │
│  :8081       │ ◄──────────────────────────────  │   Manager    │
└──────────────┘       JSON response              │   (Go+Py)    │
                                                  └──────┬───────┘
                                                         │
                              1. Write content_schedule.json
                              2. Run patch_masterdata.py
                              3. Overwrite database.bin.e
                                                         │
                                                         ▼
                                                  ┌──────────────┐
                       POST /api/admin/reload     │  lunar-tear   │
                  ◄───────────────────────────────│  (gRPC:8003)  │
                       Reload schedule + gacha    │  webhook:8082 │
                                                  └──────────────┘
```

## Prerequisites

- **Go 1.21+** — to build the manager server
- **Python 3.9+** — to run the binary patcher
- **Python packages**: `pip install pycryptodome msgpack lz4`
- A running [lunar-tear](https://github.com/Lunar-Tear-Team/lunar-tear) server (the game server)

## Quick Start

```bash
# 1. Clone this repo alongside lunar-tear
#    Expected layout:
#    ├── lunar-tear/
#    │   └── server/
#    │       └── assets/
#    │           ├── bundle_index.json
#    │           └── release/
#    │               ├── 20240404193219.bin.e   ← pristine master data
#    │               └── database.bin.e         ← patched (served to client)
#    └── content-manager/                       ← this repo

# 2. Build
go build -o content-manager main.go

# 3. Run
./content-manager
```

Open **http://localhost:8081** in your browser.

## Command-Line Flags

| Flag | Default | Description |
|---|---|---|
| `--data-dir` | `../lunar-tear/server` | Path to the lunar-tear `server/` directory containing `assets/` |
| `--port` | `8081` | Port for the admin web UI |
| `--webhook` | `http://localhost:8082/api/admin/reload` | URL to ping lunar-tear when the schedule changes |

### Examples

```bash
# Default (lunar-tear is a sibling directory)
./content-manager

# Custom data directory
./content-manager --data-dir /opt/lunar-tear/server

# Different ports
./content-manager --port 9090 --webhook http://192.168.1.40:8082/api/admin/reload
```

## Files

| File | Purpose |
|---|---|
| `main.go` | Go web server — serves the dashboard, handles `/api/schedule` and `/api/bundles`, invokes the Python patcher, pings the lunar-tear webhook |
| `index.html` | Single-page admin UI with monthly bundle toggles, presets, and live stats |
| `patch_masterdata.py` | Python script that decrypts `database.bin.e` (AES-128-CBC), mutates `EndDatetime` fields in the MessagePack/LZ4 binary, and re-encrypts it |

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Serves the admin dashboard |
| `GET` | `/api/status` | Returns current schedule and stats |
| `GET` | `/api/bundles` | Returns all available monthly bundles with content counts |
| `GET` | `/api/schedule` | Returns the active schedule |
| `POST` | `/api/schedule` | Applies a new schedule, patches `database.bin.e`, and pings lunar-tear |

## Integration with lunar-tear

The content manager requires a small hook in lunar-tear to work. The game server must expose:

- **`POST /api/admin/reload`** — A webhook endpoint that rereads `content_schedule.json` from disk and rebuilds its in-memory gacha filter. This is what forces the mobile client to redownload the patched database without restarting the server.
- **`os.Stat` on `database.bin.e`** — The `GetLatestMasterDataVersion` gRPC endpoint appends the file's modification timestamp to the version string, which naturally invalidates the client's cache when the file changes.

## Known Limitations

- **Zero-banner crash**: If you deselect everything, the client crashes trying to render an empty 3D gacha room. The patcher automatically injects fallback banners (IDs 45 & 46) to prevent this.
- **Pre-2022 side stories**: The "Recollections of Dusk" system launched in August 2022, so no side stories will appear for months before that — this is historically accurate.
- **Memorial Quests**: Late-game events were migrated by Applibot to the "Memorial Quests" tab in the final EOS client. They still unlock correctly but won't appear in the standard Subquests menu.

## License

MIT
