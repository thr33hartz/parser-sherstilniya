# jobs/price_job.py

from services.price_service import get_sol_price
from state.sol_price_state import set_sol_price
import logging

logger = logging.getLogger(__name__)

async def update_sol_price_job(context):
    try:
        price = await get_sol_price()
        if price is not None:
            set_sol_price(price)
            logger.info(f"SOL price updated: ${price:.2f}")
        else:
            logger.warning("SOL price update returned None.")
    except Exception as e:
        logger.error(f"Failed to update SOL price: {e}")