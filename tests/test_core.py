import unittest

from threadforge.core import Scheduler


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.now = 0
        self.s = Scheduler(clock=lambda: self.now, lease_seconds=5, heartbeat_seconds=10)
        self.w = self.s.register("w")

    def test_dependency_and_inherited_priority(self):
        low = self.s.submit("echo", {}, priority=1)["id"]
        self.now += 1
        self.s.submit("echo", {}, dependencies=[low], priority=10)
        self.now += 1
        self.s.submit("echo", {}, priority=5)
        leased = self.s.lease(self.w)
        self.assertEqual(leased["id"], low)
        self.s.complete(low, self.w, leased["lease_token"], True, "ok")
        self.assertEqual(self.s.lease(self.w)["task"], "echo")

    def test_lease_timeout_fences_old_worker_result(self):
        job = self.s.submit("echo", {}, max_retries=1)["id"]
        old = self.s.lease(self.w)
        self.now = 6
        new = self.s.lease(self.w)
        self.assertEqual(new["id"], job)
        with self.assertRaises(ValueError):
            self.s.complete(job, self.w, old["lease_token"], True, "old")
        self.s.complete(job, self.w, new["lease_token"], True, "new")
        self.assertEqual(self.s.snapshot(job)["result"], "new")

    def test_failure_blocks_dependents(self):
        parent = self.s.submit("echo", {}, max_retries=0)["id"]
        child = self.s.submit("echo", {}, dependencies=[parent])["id"]
        lease = self.s.lease(self.w)
        self.s.complete(parent, self.w, lease["lease_token"], False, "error")
        self.assertIsNone(self.s.lease(self.w))
        self.assertEqual(self.s.snapshot(child)["state"], "BLOCKED")

    def test_reject_unknown_dependency(self):
        with self.assertRaises(ValueError):
            self.s.submit("echo", {}, dependencies=["missing"])


if __name__ == "__main__":
    unittest.main()
