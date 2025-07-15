# state/sol_price_state.py

sol_price: float | None = None

def set_sol_price(price: float):
    global sol_price
    sol_price = price

def get_sol_price() -> float | None:
    return sol_price