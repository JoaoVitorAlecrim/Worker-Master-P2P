import time
import json
from typing import Any, Dict


def execute_task(task: Dict[str, Any]) -> Any:
    """Executa uma tarefa.

    Compatibilidade: se o `task` vier com o campo `user` (payload externo),
    tentamos parseá-lo como JSON contendo `operation`/`values`. Caso contrário
    retornamos uma resposta simples reconhecendo o payload `user`.
    """
    # Suporta payload vindo da rede: {'user': '...'}
    if 'user' in task and isinstance(task['user'], str):
        user_payload = task['user']
        try:
            parsed = json.loads(user_payload)
            if isinstance(parsed, dict):
                operation = parsed.get('operation')
                values = parsed.get('values', [])
            else:
                return {"STATUS": "OK", "DETAIL": f"user: {user_payload}"}
        except Exception:
            # Não é JSON; devolve reconhecimento
            return {"STATUS": "OK", "DETAIL": f"user: {user_payload}"}
    else:
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