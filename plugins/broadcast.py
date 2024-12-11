from pyrogram import Client, filters
import datetime
import time
import asyncio
from database.users_chats_db import db
from info import ADMINS
from utils import broadcast_messages

@Client.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
async def verupikkals(bot, message):
    # Fetch all users from the database and convert to a list
    users = await db.get_all_users().to_list(length=None)
    b_msg = message.reply_to_message
    sts = await message.reply_text(text="Broadcasting your messages...")
    start_time = time.time()
    total_users = len(users)

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

    # Create tasks for broadcasting
    tasks = [send_message(user) for user in users]

    # Process in batches to avoid overwhelming the server
    for i in range(0, len(tasks), 50):  # Batch size of 50
        await asyncio.gather(*tasks[i:i + 50])
        await sts.edit(
            f"Broadcast in progress:\n\n"
            f"Total Users: {total_users}\n"
            f"Completed: {min(i + 50, total_users)} / {total_users}\n"
            f"Success: {success}\n"
            f"Blocked: {blocked}\n"
            f"Deleted: {deleted}\n"
            f"Failed: {failed}"
        )

    # Final Statistics
    time_taken = datetime.timedelta(seconds=int(time.time() - start_time))
    await sts.edit(
        f"Broadcast Completed:\n\n"
        f"Time Taken: {time_taken}\n"
        f"Total Users: {total_users}\n"
        f"Success: {success}\n"
        f"Blocked: {blocked}\n"
        f"Deleted: {deleted}\n"
        f"Failed: {failed}"
    )
    
