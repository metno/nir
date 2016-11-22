import yaml
import datetime


class ProductStatusElement(object):
    all = {}

    def __init__(self, entries, name):
        self.name = name
        if name not in self.all:
            self.all[name] = []
        self.all[name].append(self)

        for e in entries:
            for key, value in entries.items():
                if type(value) is list:
                    subelements = [ProductStatusElement(v, key) for v in value]
                    for s in subelements:
                        s.__dict__[name] = self
                    self.__dict__[key] = subelements
                else:
                    self.__dict__[key] = value

    def __eq__(self, other):
        return str(self) == str(other)

    def __repr__(self):
        return '%s:%s' % (self.name, self.id)


class ProductStatusEntry(dict):
    pass


class ProductStatus(object):

    def _make_entry(self, name, elements):
        ret = ProductStatusEntry()
        for p_spec in elements[name]:
            p = ProductStatusElement(p_spec, name)
            ret[p.id] = p
        return ret

    def __init__(self, yaml_stream):
        elements = yaml.load(yaml_stream)['productstatus']

        self.format = self._make_entry('format', elements)
        self.servicebackend = self._make_entry('servicebackend', elements)
        self.product = self._make_entry('product', elements)

        self.productinstance = ProductStatusEntry()
        for p in self.product.values():
            for pi in p.productinstance:
                self.productinstance[pi.id] = pi
                pi.complete = {}
                pi.reference_time = pi.reference_time.replace(tzinfo=datetime.timezone.utc)

        self.data = ProductStatusEntry()
        for p in self.productinstance.values():
            for d in p.data:
                self.data[d.id] = d

        self.datainstance = ProductStatusEntry()
        for d in self.data.values():
            for di in d.datainstance:
                di.servicebackend = self.servicebackend[di.servicebackend]
                di.format = self.format[di.format]
                self.datainstance[di.id] = di

        for pi in self.productinstance.values():
            for d in pi.data:
                for di in d.datainstance:
                    backend = di.servicebackend.resource_uri
                    format = di.format.resource_uri
                    if backend not in pi.complete:
                        pi.complete[backend] = {}
                    if format not in pi.complete[backend]:
                        pi.complete[backend][format] = {}
                    pi.complete[backend][format] = {'file_count': True}
