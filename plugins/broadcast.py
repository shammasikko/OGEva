from pyrogram import Client, filters
import datetime
import time
from database.users_chats_db import db
from info import ADMINS
from utils import broadcast_messages
import asyncio

@Client.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
async def verupikkals(bot, message):
    users = await db.get_all_users()
    b_msg = message.reply_to_message
    sts = await message.reply_text(
        text="Broadcasting your messages..."
    )
    start_time = time.time()
    total_users = await db.total_users_count()

    # Statistics
    success, blocked, deleted, failed = 0, 0, 0, 0

    async def send_message(user):
        nonlocal success, blocked, deleted, failed
        try:
            pti, sh = await broadcast_messages(int(user['id']), b_msg)
            if pti:
                success += 1
            elif sh == "Blocked":
                blocked += 1
            elif sh == "Deleted":
                deleted += 1
            elif sh == "Error":
                failed += 1
        except Exception:
            failed += 1

    # Use asyncio.gather to run tasks concurrently
    tasks = [send_message(user) for user in users]
    for i in range(0, len(tasks), 50):  # Batch size to avoid overwhelming the server
        await asyncio.gather(*tasks[i:i + 50])
        await sts.edit(
            f"Broadcast in progress:\n\n"
            f"Total Users: {total_users}\n"
            f"Completed: {i + 50 if i + 50 < len(tasks) else len(tasks)} / {total_users}\n"
            f"Success: {success}\n"
            f"Blocked: {blocked}\n"
            f"Deleted: {deleted}\n"
            f"Failed: {failed}"
        )

    # Final Stats
    time_taken = datetime.timedelta(seconds=int(time.time() - start_time))
    await sts.edit(
        f"Broadcast Completed:\nCompleted in {time_taken} seconds.\n\n"
        f"Total Users: {total_users}\n"
        f"Success: {success}\n"
        f"Blocked: {blocked}\n"
        f"Deleted: {deleted}\n"
        f"Failed: {failed}"
    )
