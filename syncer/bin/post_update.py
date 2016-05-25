import productstatus.api
import json
import urllib2
from datetime import datetime, timedelta


class ProductStatusContentCreator(object):
    def __init__(self, api, host, username, api_key):
        self.api = api
        self.host = host
        self.base_path = 'api/v1'
        self.credentials = '?username=%s&api_key=%s' % (username, api_key)

    def nofity(self, model_file, reference_time, version=1):
        wdb_timestring = reference_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        productinstance_id = self.create_productinstance('sofa', wdb_timestring, version)
        data_id = self.create_data(productinstance_id, wdb_timestring, (reference_time + timedelta(hours=2)).strftime('%Y-%m-%dT%H:%M:%SZ'))
        return self.create_datainstance(data_id, model_file), self.create_datainstance(data_id, model_file)
#         datainstance_id = self.create_datainstance(data_id, model_file)
#         datainstance_id = self.create_datainstance(data_id, model_file)
#         return self.api.datainstance[datainstance_id]

    def create_productinstance(self, product_id, reference_time, version=1):
        productinstance = self._create('productinstance',
                                       {'product': '/%s/%s/%s/' % (self.base_path, 'product', self.api.product[product_id].id),
                                        'reference_time': reference_time,
                                        'version': str(version)})
        print 'Productinstance:', productinstance
        return productinstance

    def create_data(self, productinstance_id, time_begin, time_end):
        return self._create('data',
                            {'productinstance': '/%s/%s/%s/' % (self.base_path, 'productinstance', productinstance_id),
                             'time_period_begin': time_begin,
                             'time_period_end': str(time_end)})

    def create_datainstance(self, data_id, url, dataformat_id=None, servicebackend_id=None):
        if dataformat_id is None:
            dataformat_id = self.api.dataformat['netcdf'].id
        if servicebackend_id is None:
            servicebackend_id = self.api.servicebackend['datastore1'].id
        datainstance = self._create('datainstance',
                                    {'data': self._reference('data', data_id),
                                     'format': self._reference('dataformat', dataformat_id),
                                     'servicebackend': self._reference('servicebackend', servicebackend_id),
                                     'url': url})
        return datainstance

    def _create(self, what, data):
        full_url = '%s/%s/%s/%s' % (self.host, self.base_path, what, self.credentials)
        data = json.dumps(data)
        request = urllib2.Request(full_url, data, {'Content-Type': 'application/json', 'Content-Length': len(data)})
        print data
        response = urllib2.urlopen(request)
        return response.info()['location'].split('/')[-2]

    def _reference(self, what, id):
        return '/%s/%s/%s/' % (self.base_path, what, id)


def filename_format(time):
    return time.strftime('%Y%m%dT%H%M%SZ')


def last_run():
    t = datetime.utcnow() - timedelta(minutes=8)
    offset = (t.second + (t.minute*60)) % 450
    return t - timedelta(seconds=offset, microseconds=t.microsecond)


reference_time = last_run()
model_file = 'http://thredds-staging.met.no/thredds/fileServer/metusers/christofferae/nowcasting/nowcastlcc.mos.pcappi-0-dbz.noclass-noclfilter-novpr-noclcorr-noblock.nordiclcc-1000.%s.nc' % (filename_format(reference_time),)
version = 2
api = productstatus.api.Api('http://localhost:8000')
c = ProductStatusContentCreator(api, 'http://localhost:8000', 'foo', 'd54f9200b680ff11eb1ffcb01a99bde2abcdefab')
# print c.nofity(model_file, reference_time, version).id
for r in c.nofity(model_file, reference_time, version):
    print r
