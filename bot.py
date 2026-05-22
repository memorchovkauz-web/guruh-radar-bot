import asyncio
import sqlite3
from datetime import datetime

from telethon import TelegramClient, events
from telethon.tl.types import ChannelAdminLogEventActionDeleteMessage

api_id = 36784553
api_hash = "c463b506e987f1f82e211cef8c50f952"

SOURCE_GROUP = -1003172289496
ARCHIVE_GROUP = -5281572843

client = TelegramClient("archive_monitor", api_id, api_hash)

conn = sqlite3.connect("archive_map.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS message_map (
    source_msg_id INTEGER PRIMARY KEY,
    archive_msg_id INTEGER
)
""")
conn.commit()


def save_map(source_msg_id, archive_msg_id):
    cursor.execute("""
        INSERT OR REPLACE INTO message_map (source_msg_id, archive_msg_id)
        VALUES (?, ?)
    """, (source_msg_id, archive_msg_id))
    conn.commit()


def get_archive_msg_id(source_msg_id):
    cursor.execute("""
        SELECT archive_msg_id FROM message_map
        WHERE source_msg_id = ?
    """, (source_msg_id,))
    row = cursor.fetchone()
    return row[0] if row else None


@client.on(events.NewMessage(chats=SOURCE_GROUP))
async def forward_message(event):
    try:
        forwarded = await client.forward_messages(
            ARCHIVE_GROUP,
            event.message
        )

        forwarded_msg = forwarded[0] if isinstance(forwarded, list) else forwarded

        save_map(event.message.id, forwarded_msg.id)

        print(f"MAPPED: {event.message.id} -> {forwarded_msg.id}")

    except Exception as e:
        print("FORWARD ERROR:", e)


@client.on(events.MessageEdited(chats=SOURCE_GROUP))
async def edited_message(event):
    try:
        original_id = event.message.id
        archive_message_id = get_archive_msg_id(original_id)

        if not archive_message_id:
            print(f"EDIT SKIPPED, mapping not found: {original_id}")
            return

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sender = await event.get_sender()
        first = sender.first_name or ""
        last = sender.last_name or ""
        username = sender.username or ""

        sender_name = f"{first} {last}".strip()

        if username:
            sender_name = f"{sender_name} (@{username})"

        new_text = (
            event.message.message
            or event.message.text
            or event.message.raw_text
            or ""
        )

        if not new_text:
            new_text = "Матнсиз media/caption ўзгартирилган"

        audit_text = (
            f"✏️ Маълумот ўзгартирилди\n"
            f"🕒 Вақт: {now}\n"
            f"👤 Таҳрирлаган: {sender_name}\n\n"
            f"🆕 Янги маълумот:\n{new_text}"
        )

        await client.send_message(
            ARCHIVE_GROUP,
            audit_text,
            reply_to=archive_message_id
        )

        print(f"Edited audit sent: {original_id}")

    except Exception as e:
        print("EDIT ERROR:", e)


last_deleted = set()


async def check_deleted_messages():
    while True:
        try:
            async for event in client.iter_admin_log(
                SOURCE_GROUP,
                delete=True,
                limit=50
            ):
                if not isinstance(event.action, ChannelAdminLogEventActionDeleteMessage):
                    continue

                deleted_msg = event.action.message

                if deleted_msg.id in last_deleted:
                    continue

                last_deleted.add(deleted_msg.id)

                archive_message_id = get_archive_msg_id(deleted_msg.id)

                if not archive_message_id:
                    print(f"DELETE SKIPPED, mapping not found: {deleted_msg.id}")
                    continue

                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                user_name = "Номаълум"

                if event.user:
                    first = event.user.first_name or ""
                    last = event.user.last_name or ""
                    username = event.user.username or ""

                    user_name = f"{first} {last}".strip()

                    if username:
                        user_name = f"{user_name} (@{username})".strip()

                await client.send_message(
                    ARCHIVE_GROUP,
                    f"🗑 Маълумот ўчирилди\n"
                    f"🕒 Вақт: {now}\n"
                    f"👤 Ўчирган: {user_name}",
                    reply_to=archive_message_id
                )

                print(f"Deleted audit sent: {deleted_msg.id}")

        except Exception as e:
            print("DELETE ERROR:", e)

        await asyncio.sleep(5)


print("✅ Archive monitor started")

with client:
    client.loop.create_task(check_deleted_messages())
    client.run_until_disconnected()
