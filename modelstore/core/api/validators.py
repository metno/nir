import tastypie.validation
import tastypie.exceptions
import tastypie.http
import datetime
import re
import core.models

HAL_ERROR = {"__all__": "Just what do you think you're doing, Dave? Feeding me an empty hash?"}
URI_REGEX = "[A-Za-z][A-Za-z0-9\+\.\-]*:([A-Za-z0-9\.\-_~:/\?#\[\]@!\$&'\(\)\*\+,;=]|%[A-Fa-f0-9]{2})+"


class BaseValidation(tastypie.validation.Validation):
    def _optional_parameters(self, bundle, keys):
        errors = []
        for key in keys:
            if key in bundle.data:
                if isinstance(bundle.data[key], str) and len(bundle.data[key]) == 0:
                    errors.append("Parameter '%s' can't be empty" % key)
        return errors

    def _required_parameters(self, bundle, keys):
        errors = self._optional_parameters(bundle, keys)
        for key in keys:
            if not key in bundle.data:
                errors.append("Missing required parameter '%s'" % key)
        return errors


class ModelValidation(BaseValidation):
    def is_valid(self, bundle, request=None):
        if not bundle.data:
            return HAL_ERROR

        errors = self._required_parameters(bundle, ['id', 'name'])
        return errors


class DatasetValidation(BaseValidation):
    def _validate_status(self, status):
        def _found(status):
            for code, desc in core.models.Dataset.STATUS_CHOICES:
                if code == status:
                    return True
            return False
        try:
            status = int(status)
            if not _found(status):
                return ['Status code %d is not valid, expected one of {%s}' % (status, ','.join([str(x[0]) for x in core.models.Dataset.STATUS_CHOICES]))]
        except ValueError, e:
            return ["Invalid status code: %s" % unicode(e)]
        return []

    # ValiDATE, heh, get it?
    def _validate_date(self, date):
        try:
            datetime.datetime.strptime(date, '%Y-%m-%d')
        except ValueError, e:
            return ["Invalid date: %s" % unicode(e)]
        return []

    # TERMinate any invalid data
    def _validate_term(self, term):
        try:
            term = int(term)
            if term < 0 or term > 23:
                return ['Term %d is not in accepted range 00-23' % term]
        except ValueError, e:
            return ["Invalid term: %s" % unicode(e)]
        return []

    def is_valid(self, bundle, request=None):
        if not bundle.data:
            return HAL_ERROR

        params = ['date', 'term', 'status']
        if bundle.obj.id:
            errors = self._optional_parameters(bundle, params)
        else:
            errors = self._required_parameters(bundle, params)
        if errors:
            return errors

        if 'term' in bundle.data:
            errors += self._validate_term(bundle.data['term'])
        if 'date' in bundle.data:
            errors += self._validate_date(bundle.data['date'])
        if 'status' in bundle.data:
            errors += self._validate_status(bundle.data['status'])

        return errors


class FileValidation(BaseValidation):
    def is_valid(self, bundle, request=None):
        if not bundle.data:
            return HAL_ERROR

        errors = self._required_parameters(bundle, ['uri'])
        if errors:
            return errors

        if not re.match(URI_REGEX, bundle.data['uri']):
            errors.append("Malformed URI, please read RFC 3986")

        return errors
