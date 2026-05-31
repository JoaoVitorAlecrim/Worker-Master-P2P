import unittest
from common.task_manager import TaskManager


class TestTaskManagerUserPayload(unittest.TestCase):
    def test_create_task_from_user_sets_user_and_enqueues(self):
        tm = TaskManager()
        user_payload = "alice: compute sum 1 2"

        task = tm.create_task_from_user(user_payload)

        self.assertIsNotNone(task)
        self.assertEqual(task.user, user_payload)
        # Ensure task is stored in manager
        stored = tm.get_task(task.task_id)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.user, user_payload)

        # to_dict should include the user field
        d = stored.to_dict()
        self.assertIn("user", d)
        self.assertEqual(d["user"], user_payload)


if __name__ == "__main__":
    unittest.main()
