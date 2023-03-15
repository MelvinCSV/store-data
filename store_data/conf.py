from pathlib import Path


class Conf:
    """"""

    def __init__(self):
        """"""
        self.f_base = Path(__file__).parent.parent
        self.f_test = self.f_base / "test"
        self.f_test_data = self.f_test / "data"


conf = Conf()
