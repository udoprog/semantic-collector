class Injector(object):
    """
    Injector framework.

    Provides a hierarchy of named types that can be required.
    """
    def __init__(self, components, parent=None):
        self._components = components
        self._parent = parent
        self._children = list()

    def require(self, name):
        c = self._components.get(name)

        if c is None:
            if self._parent is None:
                raise Exception(
                    "No component named '{0}' available".format(name))

            return self._parent.require(name)

        return c

    def child(self, components={}):
        components = dict(components)

        for n, c in self._components.items():
            child = getattr(c, '_injectchild', None)

            if child is not None:
                components[n] = child()

        child = Injector(components, self)
        self._children.append(child)
        return child

    def free(self):
        """
        Free this injector and all it's descendents.
        """
        for child in self._children:
            child.free()

        for c in self._components.values():
            free = getattr(c, '_injectfree', None)

            if free is not None:
                free()

        if self._parent is not None:
            self._parent._children.remove(self)
