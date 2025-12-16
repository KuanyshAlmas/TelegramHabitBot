import asyncio
import logging
from os import getenv

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

import database as db
from handlers import router
from scheduler import setup_scheduler, set_bot

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Main function to run the bot."""
    # Get bot token
    token = getenv("BOT_TOKEN")
    if not token:
        logger.error("BOT_TOKEN not found in environment variables!")
        logger.info("Create a .env file with: BOT_TOKEN=your_token_here")
        return

    # Initialize database
    logger.info("Initializing database...")
    await db.init_db()

    # Create bot instance
    bot = Bot(token=token, parse_mode="Markdown")

    # Create dispatcher with FSM storage
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    dp.include_router(router)

    # Setup scheduler
    logger.info("Setting up scheduler...")
    set_bot(bot)
    scheduler = setup_scheduler()
    scheduler.start()

    # Start polling
    logger.info("Starting bot...")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        scheduler.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
