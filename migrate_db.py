import asyncio
import logging
from database import init_db, Base, engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

async def migrate_database():
    """
    Run database migrations - creates all tables that don't exist yet.
    """
    logger.info("Starting database migration...")
    try:
        # Create tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database migration completed successfully")
    except Exception as e:
        logger.error(f"Error during database migration: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(migrate_database()) 