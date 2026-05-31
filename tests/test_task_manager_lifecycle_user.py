import unittest
from common.task_manager import TaskManager


class TestTaskManagerLifecycleUser(unittest.TestCase):
    def test_full_lifecycle_user_task(self):
        tm = TaskManager()

        # Registrar worker
        worker = tm.register_worker(worker_uuid="W-1", server_uuid="Master_A")

        # Criar tarefa a partir do payload externo USER
        user_payload = '{"operation": "soma", "values": [1, 2]}'
        task = tm.create_task_from_user(user_payload)

        self.assertIsNotNone(task)
        self.assertEqual(task.user, user_payload)
        self.assertEqual(task.status.name.lower(), "pending")

        # Atribuir à worker
        assigned = tm.assign_task(task.task_id, worker.worker_uuid)
        self.assertTrue(assigned)

        # Verificar mudança de estado
        t = tm.get_task(task.task_id)
        self.assertIsNotNone(t)
        self.assertEqual(t.status.name.lower(), "in_progress")
        self.assertEqual(t.assigned_worker, worker.worker_uuid)

        # Completar tarefa
        res = tm.complete_task(task.task_id, 3)
        self.assertTrue(res)
        t2 = tm.get_task(task.task_id)
        self.assertEqual(t2.status.name.lower(), "completed")


if __name__ == "__main__":
    unittest.main()
