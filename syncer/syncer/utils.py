import dateutil.tz


class SerializeBase:
    __serializable__ = []

    def serialize(self):
        """
        Create JSON encodable representation of internal data structure.
        """
        serialized = {}
        for key in self.__serializable__:
            func_name = 'serialize_' + key
            func = getattr(self, func_name, None)
            serialized[key] = getattr(self, key)
            if callable(func):
                serialized[key] = func(serialized[key])
            elif hasattr(serialized[key], 'serialize'):
                serialized[key] = serialized[key].serialize()
        return serialized

    def _serialize_datetime(self, value):
        """
        Return a time zone-aware ISO 8601 string.
        """
        utc_time = value.replace(tzinfo=dateutil.tz.tzutc())
        return utc_time.isoformat().replace(' ', 'T').replace('+00:00', 'Z')
