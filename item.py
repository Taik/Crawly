from UserDict import DictMixin # Item wrapper


# Placeholder for required fields in the Item() class.
class Field(object):
    pass


# Item class -- taken from Scrapy
class BaseItem(object):
    pass


class DictItem(DictMixin, BaseItem):
    fields = {}

    def __init__(self):
        self._values = {}

    def __getitem__(self, key):
        return self._values[key]

    def __setitem__(self, key, value):
        if key in self.fields:
            self._values[key] = value
        else:
            raise KeyError("%s does not support field: %s" % (self.__class__.__name__, key))

    def __delitem__(self, key):
        del self._values[key]

    def __getattr__(self, name):
        if name in self.fields:
            raise AttributeError("Use item[%r] to get field value" % name)
        raise AttributeError(name)

    def __setattr__(self, name, value):
        if not name.startswith('_'):
            raise AttributeError("Use item[%r] = %r to set field value" % (name, value))
        super(DictItem, self).__setattr__(name, value)

    def keys(self):
        return self._values.keys()

    def __repr__(self):
        return '<Item: %s>' % self._values


class Item(BaseItem):
    pass