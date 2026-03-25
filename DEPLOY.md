# TennisTrade — Railway Deployment Guide

## Architecture

Two Railway services in the same project, sharing one PostgreSQL:

```
Railway Project: tennistrade
├── PostgreSQL (already running — your data is here)
├── Service 1: radar-worker (Python, runs 24/7, no port)
└── Service 2: dashboard (Node.js, port 3000, public URL)
```

## Prerequisites

You already have:
- Railway project with PostgreSQL (DATABASE_URL set)
- API-Tennis key: 8fab7dbb...
- All data loaded (323K matches, Elo, stats)

## Step 1: Create two GitHub repos

### Repo 1: tennistrade-radar
Create a new GitHub repo. Push these files:
```
tennistrade-radar/
├── Dockerfile
├── db.py
├── step5_trading.py
├── step6_radar.py
├── step7_paper.py
├── step8_stats.py
└── step9_wire.py
```

Commands:
```bash
cd tennistrade-radar
git init
git add .
git commit -m "Initial radar worker"
git remote add origin https://github.com/YOUR_USERNAME/tennistrade-radar.git
git push -u origin main
```

### Repo 2: tennistrade-dashboard
Create another GitHub repo. Push these files:
```
tennistrade-dashboard/
├── Dockerfile
├── package.json
├── server.js
├── config/db.js
├── modules/
│   ├── trades/{routes.js, service.js}
│   ├── players/{routes.js, service.js}
│   ├── predictions/{routes.js, service.js}
│   └── radar/routes.js
├── views/
│   ├── partials/{header.ejs, footer.ejs}
│   ├── home.ejs, trades.ejs, players.ejs
│   ├── player-card.ejs, predictions.ejs, radar.ejs
└── public/
    ├── css/style.css
    └── js/app.js
```

Commands:
```bash
cd tennistrade-dashboard
git init
git add .
git commit -m "Initial dashboard"
git remote add origin https://github.com/YOUR_USERNAME/tennistrade-dashboard.git
git push -u origin main
```

## Step 2: Deploy radar worker on Railway

1. Go to your Railway project (the one with PostgreSQL)
2. Click "New" → "GitHub Repo" → select tennistrade-radar
3. Railway will detect the Dockerfile and build
4. Go to the new service → Settings:
   - Remove the port (this has no web interface)
5. Go to Variables tab, add:
   - `DATABASE_URL` → click "Reference" → select your PostgreSQL's DATABASE_URL
   - `API_TENNIS_KEY` → 8fab7dbb589d73374385bfc6924d5aa2899024d0c44ab789d0b11b5fd1bb1a3b
6. Deploy. Check logs — you should see:
   ```
   TennisTrade — Starting Full System
   Monitoring X live matches
   ```

## Step 3: Deploy dashboard on Railway

1. Same Railway project → "New" → "GitHub Repo" → select tennistrade-dashboard
2. Railway will detect the Dockerfile and build
3. Go to Variables tab, add:
   - `DATABASE_URL` → click "Reference" → select your PostgreSQL's DATABASE_URL
   - `PORT` → 3000
4. Go to Settings → Networking → Generate Domain
   - You'll get something like: tennistrade-dashboard-production-xxxx.up.railway.app
5. Deploy. Open the URL — you should see the dashboard.

## Step 4: Verify

1. Open your dashboard URL — check Overview page shows database stats
2. Go to /players — search for Djokovic, Sinner, etc.
3. Go to /predictions — should show today's matches (if odds data exists for today)
4. Go to /radar — shows radar status
5. Check Railway logs for the radar service — should show "Monitoring X live matches"
6. Go to /trades — will be empty until the radar generates signals

## Environment Variables Summary

### Radar worker:
| Variable | Value |
|----------|-------|
| DATABASE_URL | (reference PostgreSQL) |
| API_TENNIS_KEY | 8fab7dbb...b1a3b |

### Dashboard:
| Variable | Value |
|----------|-------|
| DATABASE_URL | (reference PostgreSQL) |
| PORT | 3000 |

## Troubleshooting

**Radar shows "NO KEY"**: API_TENNIS_KEY env var not set
**Dashboard shows "—" for everything**: DATABASE_URL not connected
**"relation paper_trades does not exist"**: Run the radar once — it creates the tables
**No predictions showing**: Odds data only goes to 2026-03-15 (from tennis-data.co.uk). Today's data needs API-Tennis pre-match odds loader (future feature).
