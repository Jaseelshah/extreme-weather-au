# Automation Setup Guide (macOS — Mac Mini)

## Overview
The pipeline needs to run **automatically once per month** with minimal manual steps.
On macOS, we use **launchd** (the native macOS scheduler — like cron but Apple's way).

---

## Option A: launchd (Recommended for macOS)

### Step 1: Create the launch agent

Save this file as:
`~/Library/LaunchAgents/com.extremeweather.pipeline.plist`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.extremeweather.pipeline</string>

    <key>ProgramArguments</key>
    <array>
        <!-- Update this path to your Python executable -->
        <string>/usr/local/bin/python3</string>
        <string>/path/to/extreme-weather-au/automation/run_pipeline.py</string>
    </array>

    <key>WorkingDirectory</key>
    <string>/path/to/extreme-weather-au</string>

    <!-- Run on the 1st of every month at 6:00 AM -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Day</key>
        <integer>1</integer>
        <key>Hour</key>
        <integer>6</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>

    <!-- Also run when first loaded (catch up if missed) -->
    <key>RunAtLoad</key>
    <false/>

    <!-- Redirect stdout/stderr to log files -->
    <key>StandardOutPath</key>
    <string>/path/to/extreme-weather-au/logs/launchd_stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/path/to/extreme-weather-au/logs/launchd_stderr.log</string>
</dict>
</plist>
```

### Step 2: Find your Python path

Run this in Terminal to find where Python is installed:
```bash
which python3
```

Update the `<string>/usr/local/bin/python3</string>` line in the plist above
with whatever path is returned. Common locations:
- `/usr/local/bin/python3` (Homebrew)
- `/usr/bin/python3` (System)
- `/opt/homebrew/bin/python3` (Apple Silicon Homebrew)

### Step 3: Load the agent

```bash
launchctl load ~/Library/LaunchAgents/com.extremeweather.pipeline.plist
```

### Step 4: Verify it's loaded

```bash
launchctl list | grep extremeweather
```

You should see a line with the label `com.extremeweather.pipeline`.

### Step 5: Test manually

To trigger a run right now (without waiting for the 1st):
```bash
launchctl start com.extremeweather.pipeline
```

Check the logs:
```bash
tail -f /path/to/extreme-weather-au/logs/launchd_stdout.log
```

### To unload/stop:
```bash
launchctl unload ~/Library/LaunchAgents/com.extremeweather.pipeline.plist
```

---

## Option B: cron (Simpler alternative)

If you prefer cron (works the same way but less "macOS native"):

```bash
crontab -e
```

Add this line:
```
0 6 1 * * cd /path/to/extreme-weather-au && /usr/local/bin/python3 automation/run_pipeline.py >> logs/cron.log 2>&1
```

This runs at 6:00 AM on the 1st of every month.

---

## Notes

### Paths
Replace `/path/to/extreme-weather-au` in the plist/cron with wherever you cloned the repo. For example:
- `~/Projects/extreme-weather-au`
- `/Users/yourname/extreme-weather-au`
- `/Volumes/ExternalDrive/extreme-weather-au`

### What happens if the Mac is asleep?
- `launchd` will run missed jobs when the Mac wakes up
- Make sure Energy Saver settings allow the Mac to wake for scheduled tasks:
  System Settings → Energy Saver → "Wake for network access"

### Monitoring
- Check pipeline run history in the database:
  ```sql
  SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 10;
  ```
- Check log files in the `logs/` directory
- Each run creates a timestamped log: `pipeline_YYYYMMDD_HHMMSS.log`
