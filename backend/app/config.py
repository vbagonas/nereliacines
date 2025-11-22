CART_TTL = 30  # 30 sec

def cart_key(owner_id: str) -> str:
    return f"cart:{owner_id}"