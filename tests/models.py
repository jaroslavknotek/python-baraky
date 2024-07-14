class MaxElapsedError(Exception):
    pass


class DeterministicCycleTimer:
    elapsed_count = 0

    def __init__(self, max_elapsed=1):
        self.elapsed_count = 0
        self.max_elapsed = max_elapsed

    def reset(self):
        self.elapsed_count += 1

    def allow_elapse(self):
        self._allow_elapsed = True

    def elapsed(self):
        if self.max_elapsed <= 0:
            raise MaxElapsedError()
        self.max_elapsed -= 1
        return True

    async def wait(self):
        pass


class MockClient:
    async def read_all(self):
        return []


class MockStorage:
    async def get_ids(self):
        return []

    async def save(self, estates):
        pass


class MockQueue:
    def put(self, estate):
        pass
