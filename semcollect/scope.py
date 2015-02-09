from .injector import Injector


class Scope(Injector):
    """
    Application scope that provides functionality to collectors.
    """

    def __init__(self, config, registry, components):
        self._config = config
        self._registry = registry
        super().__init__(components)

    def collector(self, name):
        components = dict(registry=self._registry.group(),
                          config=self._config.get(name, {}))
        return Injector(components, self)
