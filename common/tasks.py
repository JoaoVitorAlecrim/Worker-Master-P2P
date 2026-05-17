import time
from typing import Any, Dict


def execute_task(task: Dict[str, Any]) -> Any:
    operation = task.get("operation")
    values = task.get("values", [])

    if operation == "soma":
        time.sleep(1)
        return sum(values)

    if operation == "multiplicacao":
        time.sleep(1)
        result = 1
        for value in values:
            result *= value
        return result

    if operation == "sleep":
        duration = values[0] if values else 1
        time.sleep(duration)
        return f"slept {duration}s"

    return "operacao_desconhecida"