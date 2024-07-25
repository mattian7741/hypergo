from typing import Dict, Any, List
from hypergo.transaction import Transaction

__all__ = ["converge"]


def converge(input_keys: List[str], routingkey: str, payload: Any, transaction: Transaction) -> Dict[str, Any] | None:
    merged_data = {}
    transaction.set(routingkey, payload)
    for rk in input_keys:
        merged_data[rk] = transaction.get(rk, None)
        if not merged_data[rk]:
            return None
    return merged_data


if __name__ == "__main__":
    tx = Transaction()
    result = converge(["a.b.c", "x.y.z"], "a.b.c", "First Message", tx)
    result = converge(["a.b.c", "x.y.z"], "x.y.z", "Second Message", tx)
    print(result)
