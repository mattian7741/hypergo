from jsonschema import ValidationError

class Ignorable:
    def __init__(self, should_be_ignored=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.should_be_ignored = should_be_ignored


class OutputValidationError(Ignorable, ValidationError):
    def __init__(self, message, should_be_ignored=False):
        super().__init__(message=message, should_be_ignored=should_be_ignored)
