import re


class RejectPattern:

    def __init__(self, patterns: list):
        if patterns:
            self.pattern = re.compile('|'.join(patterns))
        else:
            self.pattern = None

    def match(self, input):
        if self.pattern:
            return self.pattern.search(input)
        return False
