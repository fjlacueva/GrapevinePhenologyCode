
class FutureException(Exception):
    def __init__(self, m):
        self.result = m
    def __str__(self):
        return str(self.result)
