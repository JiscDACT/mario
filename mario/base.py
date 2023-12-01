from typing import List


class MarioBase:

    def __init__(self):
        self._properties = {}

    def set_property(self, name, value):
        self._properties[name] = value

    def get_property(self, name):
        if name in self._properties:
            return self._properties[name]
        return None

    @property
    def properties(self):
        return self._properties

    def add_properties(self, source, exclude: List[str] = None):
        for prop in source:
            if exclude is not None and prop not in exclude:
                self.set_property(prop, source[prop])