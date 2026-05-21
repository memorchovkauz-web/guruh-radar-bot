import os

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# .env faylni yuklaydi
load_dotenv()

# TOKEN oladi
BOT_TOKEN = os.getenv("BOT_TOKEN")


# /start komandasi
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📡 Guruh Radar 😂 ishga tushdi!"
    )


def main():
    # Bot application
    app = Application.builder().token(BOT_TOKEN).build()

    # Handler
    app.add_handler(CommandHandler("start", start))

    print("✅ Bot ishga tushdi...")

    # Bot run
    app.run_polling()


if __name__ == "__main__":
    main()
