"""
Functionality relating to WDB2TS.
"""

import lxml.etree
import requests
import logging

import syncer.exceptions


class WDB2TS(object):

    def __init__(self, base_url, services):
        self.base_url = base_url
        self.session = requests.Session()
        self.status = dict.fromkeys(services, {})

    def request_status(self, service):
        """
        Request WDB2TS host for status for specified service and return xml.
        """
        status_url = "%s/%s?status" % (self.base_url, service)
        logging.info("Load status information from WDB2TS service %s: %s" % (service, status_url))

        status_xml = self._get_request(status_url)

        # Validate xml
        try:
            tree = lxml.etree.fromstring(status_xml)
        except lxml.etree.XMLSyntaxError, e:
            raise syncer.exceptions.WDB2TSMissingContentException("Could not parse XML content from request %s: %s."
                                                                  % (status_url, e))
        else:
            if not tree.xpath('boolean(/status)'):
                raise syncer.exceptions.WDB2TSMissingContentException(
                    "Content from status request %s is missing its /status element." % status_url)

        return status_xml

    def _get_request(self, url):
        """
        Wrapper for self.session.get with exception handling. Returns body of response.
        """
        try:
            response = self.session.get(url)
        except requests.ConnectionError, e:
            raise syncer.exceptions.WDB2TSConnectionFailure("Connection to WDB2TS failed: %s" % unicode(e))

        if response.status_code >= 500:
            exc = syncer.exceptions.WDB2TSServiceUnavailableException
        elif response.status_code >= 400:
            exc = syncer.exceptions.WDB2TSServiceClientErrorException
        else:
            return response.content

        raise exc("WDB2TS returned error code %d for request URI %s" % (response.status_code, response.request.url))

    def load_status(self):
        """
        Set status dict for all defined services.
        """
        for service in self.status.keys():
            self.status[service] = {}
            status_xml = self.request_status(service)
            self.set_status_for_service(service, status_xml)

        return self.status

    def set_status_for_service(self, service, status_xml):
        """
        Set status dict based on values from status_xml. Return status for the service.
        """
        self.status[service]['data_providers'] = WDB2TS.data_providers_from_status_response(status_xml)
        logging.debug("Data providers for service %s: %s" % (service, ', '.join(self.status[service]['data_providers'])))

        if len(self.status[service]['data_providers']) == 0:
            logging.warn("WDB2TS data providers for service %s set to empty list." % service)

        return self.status[service]

    @staticmethod
    def data_providers_from_status_response(status_xml):
        """
        Get all defined data_providers from status_xml.
        """
        tree = lxml.etree.fromstring(status_xml)
        provider_elements = tree.xpath('/status/defined_dataproviders/dataprovider/name')

        return [e.text for e in provider_elements]

    def update_wdb2ts(self, model, model_run):
        """
        Update all relevant wdb2ts services for the specified model and model_run
        """
        data_provider = model.data_provider

        for service in self.status:
            if data_provider in self.status[service]['data_providers']:
                self.update_wdb2ts_service(service, model, model_run)

    def update_wdb2ts_service(self, service, model, model_run):
        """
        Update a wdb2ts service for a given model and model_run
        """
        try:
            update_url = self.get_update_url(service, model_run.data_provider, model_run.reference_time, model_run.version)
        except TypeError, e:
            raise syncer.exceptions.WDB2TSClientUpdateFailure("Could not generate a correct update URL for WDB2TS: %s" % e)

        self.request_update(update_url)

    def get_update_url(self, service, data_provider, reference_time, version):
        """
        Generate update url for wdb2ts service.
        """
        return "%s/%supdate?%s=%s,%d" % (self.base_url, service, data_provider, reference_time, version)

    def request_update(self, update_url):
        """
        Send update request to wdb2ts and check if the update went through
        """

        # Raise separate exceptions for client update failures and server update failures.
        try:
            response = self._get_request(update_url)
        except (syncer.exceptions.WDB2TSServiceUnavailableException,
                syncer.exceptions.WDB2TSConnectionFailure), e:
            raise syncer.exceptions.WDB2TSServerUpdateFailure("WDB2TS update %s failed because of some server error: %s" % (update_url, e))
        except syncer.exceptions.WDB2TSServiceClientErrorException, e:
            raise syncer.exceptions.WDB2TSClientUpdateFailure("WDB2TS update %s failed because the URL is not correct: %s" % (update_url, e))
        else:
            if 'NoNewDataRefTime' in response:
                logging.info("WDB2TS already up to date: %s" % update_url)
            elif 'Updated' in response:
                logging.info("WDB2TS updated successfully: %s" % update_url)
            else:
                logging.info("Unknown response from WDB2TS on request %s: %s" % (update_url, response))

    def __repr__(self):
        return "WDB2TS(%s, %s)" % (self.base_url, ",".join(self.status.keys()))
