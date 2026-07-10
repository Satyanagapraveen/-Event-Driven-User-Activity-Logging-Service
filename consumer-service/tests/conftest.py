import pytest


class MockStorage:
    def __init__(self):
        self.saved_events = []
        self.should_fail = False

    def save_event(self, event):
        if self.should_fail:
            return False
        self.saved_events.append(event)
        return True


@pytest.fixture
def mock_storage():
    return MockStorage()