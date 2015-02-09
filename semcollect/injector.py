class Injector(object):
    """
    Injector framework.

    Provides a hierarchy of named types that can be required.
    """
    def __init__(self, components, parent=None):
        self._components = components
        self._parent = parent

    def require(self, name):
        c = self._components.get(name)

        if c is None:
            if self._parent is None:
                raise Exception(
                    "No component named '{0}' available".format(name))

            return self._parent.require(name)

        return c

    def free(self):
        for c in self._components.values():
            free = getattr(c, '_injectfree', None)

            if free is not None:
                free()
