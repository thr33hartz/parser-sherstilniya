import logging
from telegram.ext import ContextTypes
from services import price_service

logger = logging.getLogger(__name__)

async def update_sol_price_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Эта задача вызывается по расписанию (раз в час).
    Она получает цену SOL и сохраняет ее в bot_data.
    """
    logger.info("JOB: Updating SOL price...")
    try:
        price = await price_service.get_sol_price()
        if price:
            # bot_data - это общий словарь, доступный всем пользователям
            context.bot_data['sol_price'] = price
            logger.info(f"JOB: SOL price updated to ${price}")
        else:
            logger.warning("JOB: Failed to fetch SOL price, old value will be kept.")
    except Exception as e:
        logger.error(f"JOB: An error occurred in update_sol_price_job: {e}")