# main.py
import os
import io
import asyncio
from datetime import datetime, timedelta
import pytz
import asyncpg
import aiohttp
import discord
from discord.ext import commands, tasks

# ---------- CONFIG ----------
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")  # Railway Postgres connection
DEFAULT_TZ = os.environ.get("DEFAULT_TZ", "Asia/Riyadh")  # default timezone

if not DISCORD_TOKEN or not DATABASE_URL:
    raise RuntimeError("Set DISCORD_TOKEN and DATABASE_URL environment variables")

tz = pytz.timezone(DEFAULT_TZ)

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

db_pool: asyncpg.pool.Pool | None = None
http_session: aiohttp.ClientSession | None = None

# ---------- DB helpers ----------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schedules (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    message TEXT,
    attachment_url TEXT,
    run_at TIMESTAMP WITH TIME ZONE NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
"""

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with db_pool.acquire() as conn:
        await conn.execute(CREATE_TABLE_SQL)

async def add_schedule(user_id: int, run_at_utc: datetime, message: str | None, attachment_url: str | None):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO schedules(user_id, message, attachment_url, run_at) VALUES($1,$2,$3,$4) RETURNING id",
            user_id, message, attachment_url, run_at_utc
        )
        return row["id"]

async def fetch_due(limit=20):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM schedules WHERE status='pending' AND run_at <= now() ORDER BY run_at ASC LIMIT $1",
            limit
        )
        return rows

async def mark_sent(schedule_id: int, status="sent"):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE schedules SET status=$1 WHERE id=$2", status, schedule_id)

async def get_all_schedules():
    async with db_pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM schedules ORDER BY run_at ASC")

async def get_schedule(schedule_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM schedules WHERE id=$1", schedule_id)

async def cancel_schedule(schedule_id: int):
    async with db_pool.acquire() as conn:
        r = await conn.execute("UPDATE schedules SET status='canceled' WHERE id=$1 AND status='pending'", schedule_id)
        return r

# ---------- Background scheduler ----------
async def download_bytes(url: str) -> tuple[io.BytesIO, str]:
    """Download file bytes and return (BytesIO, filename)"""
    assert http_session is not None
    async with http_session.get(url) as resp:
        resp.raise_for_status()
        data = await resp.read()
    # try to get filename from url
    filename = url.split("?")[0].split("/")[-1] or "file"
    bio = io.BytesIO(data)
    bio.seek(0)
    return bio, filename

async def process_due():
    rows = await fetch_due(limit=30)
    for row in rows:
        sid = row["id"]
        user_id = row["user_id"]
        message = row["message"]
        attachment_url = row["attachment_url"]
        try:
            user = await bot.fetch_user(user_id)
            if attachment_url:
                bio, fname = await download_bytes(attachment_url)
                discord_file = discord.File(fp=bio, filename=fname)
                await user.send(content=message or "", file=discord_file)
            else:
                await user.send(content=message or "")
            await mark_sent(sid, "sent")
        except Exception as e:
            # mark failed so admin can inspect (we could add retries)
            await mark_sent(sid, "failed")
            print(f"Failed to send schedule {sid} to {user_id}: {e}")

@tasks.loop(seconds=15.0)
async def scheduler_loop():
    if db_pool is None:
        return
    await process_due()

# ---------- Utilities ----------
def parse_time_12h_to_utc(time_str: str) -> datetime:
    """
    Accepts "02:30 PM" and returns a UTC-aware datetime for the next occurrence of that time in DEFAULT_TZ.
    """
    try:
        parsed_time = datetime.strptime(time_str.strip(), "%I:%M %p").time()
    except ValueError:
        raise ValueError("Time must be in 12-hour format like '02:30 PM'")

    now_local = datetime.now(tz)
    run_local = tz.localize(datetime(now_local.year, now_local.month, now_local.day,
                                     parsed_time.hour, parsed_time.minute))
    if run_local <= now_local:
        run_local += timedelta(days=1)
    run_utc = run_local.astimezone(pytz.UTC)
    return run_utc

def pretty_time_local(utc_dt: datetime) -> str:
    local = utc_dt.astimezone(tz)
    return local.strftime("%Y-%m-%d %I:%M %p")

# ---------- Slash commands ----------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    await bot.tree.sync()
    # init DB and HTTP session
    global http_session
    if db_pool is None:
        await init_db()
    if http_session is None:
        http_session = aiohttp.ClientSession()
    if not scheduler_loop.is_running():
        scheduler_loop.start()
    print("Bot ready and scheduler started.")

@bot.tree.command(name="send", description="Send an instant DM to a member (text and/or file)")
async def slash_send(interaction: discord.Interaction, user: discord.User, message: str | None = None, file: discord.Attachment | None = None):
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        if file:
            # use attachment URL directly (download & forward)
            bio, fname = await download_bytes(file.url)
            await user.send(content=message or "", file=discord.File(fp=bio, filename=fname))
        else:
            await user.send(content=message or "")
        await interaction.followup.send(f"✅ Sent DM to {user.display_name}", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Failed to send DM: {e}", ephemeral=True)

@bot.tree.command(name="scheduledm", description="Schedule a DM to a user (12-hour time, supports optional file)")
async def slash_scheduledm(interaction: discord.Interaction, user: discord.User, time: str, message: str | None = None, file: discord.Attachment | None = None):
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        run_utc = parse_time_12h_to_utc(time)
    except ValueError:
        await interaction.followup.send("⚠️ Invalid time — use 12-hour format like `02:30 PM`.", ephemeral=True)
        return

    attachment_url = file.url if file else None
    sid = await add_schedule(user.id, run_utc, message, attachment_url)
    await interaction.followup.send(f"✅ Scheduled DM **#{sid}** to {user.mention} at {pretty_time_local(run_utc)} ({DEFAULT_TZ}).", ephemeral=True)

@bot.tree.command(name="list", description="List all scheduled DMs")
async def slash_list(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    rows = await get_all_schedules()
    if not rows:
        await interaction.followup.send("📭 No scheduled DMs.", ephemeral=True)
        return
    lines = []
    for r in rows:
        lines.append(f"#{r['id']}: to <@{r['user_id']}> at {pretty_time_local(r['run_at'])} — {r['status']} {'(file)' if r['attachment_url'] else ''}")
    text = "\n".join(lines)
    await interaction.followup.send(f"📋 Scheduled:\n{text}", ephemeral=True)

@bot.tree.command(name="get", description="Get details of a scheduled DM by ID")
async def slash_get(interaction: discord.Interaction, id: int):
    await interaction.response.defer(ephemeral=True, thinking=True)
    r = await get_schedule(id)
    if not r:
        await interaction.followup.send("❌ Not found", ephemeral=True)
        return
    text = (f"#{r['id']}\nTo: <@{r['user_id']}>\nTime: {pretty_time_local(r['run_at'])}\nStatus: {r['status']}\n"
            f"Message: {r['message'] or '[no text]'}\nAttachment: {r['attachment_url'] or '[none]'}")
    await interaction.followup.send(text, ephemeral=True)

@bot.tree.command(name="cancel", description="Cancel a scheduled DM by ID")
async def slash_cancel(interaction: discord.Interaction, id: int):
    await interaction.response.defer(ephemeral=True, thinking=True)
    r = await get_schedule(id)
    if not r:
        await interaction.followup.send("❌ Not found", ephemeral=True)
        return
    if r["status"] != "pending":
        await interaction.followup.send(f"⚠️ Schedule is already {r['status']}.", ephemeral=True)
        return
    await cancel_schedule(id)
    await interaction.followup.send(f"🛑 Canceled schedule #{id}.", ephemeral=True)

# ---------- Shutdown cleanup ----------
async def cleanup():
    global http_session, db_pool
    if http_session:
        await http_session.close()
    if db_pool:
        await db_pool.close()

import signal
def handle_exit():
    print("Shutting down...")
    asyncio.create_task(cleanup())
    asyncio.get_event_loop().stop()

signal.signal(signal.SIGINT, lambda *_: handle_exit())
signal.signal(signal.SIGTERM, lambda *_: handle_exit())

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
