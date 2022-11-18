class Calculator:
    def __init__(self, a, b):
        self.a: int = a
        self.b: int = b

    def division(self):
        try:
            return self.a / self.b
        except ZeroDivisionError:
            raise ZeroDivisionError("b could not be zero!")
