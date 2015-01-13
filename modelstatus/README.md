# modelstatus.met.no

## modelstatus.met.no REST API

### Example of requests that need to be made for updating the status of the model arome_metcoop_2500m
 
#### POST https://modelstatus.met.no/v0/model_run

```
{
    "data_provider": "arome_metcoop_2500m",
    "reference_time": "2015-01-12T06:00:00Z"
}
```

201 Created
Location: https://modelstatus.met.no/v0/model_run/1

```
{
    "id": "/v0/model_run/1",
    "data_provider": "arome_metcoop_2500m",
    "reference_time": "2015-01-12T06:00:00Z",
    "version": 0,
    "created_time": "2015-01-12T08:25:11Z"
}
```

#### POST https://modelstatus.met.no/v0/data

```
{
    "model_run": "/v0/model_run/1",
    "format": "netcdf4",
    "href": "opdata:///arome2_5/arome_metcoop_default2_5km_20150112T06Z.nc"
}    
```

201 Created
Location: https://modelstatus.met.no/v0/data/1

```
{
    "model_run": "/v0/model_run/1",
    "id": "/v0/data/1",
    "format": "netcdf4",
    "href": "opdata:///arome2_5/arome_metcoop_default2_5km_20150112T06Z.nc",
    "created_time": "2015-01-12T08:26:01Z"
}
```


#### POST https://modelstatus.met.no/v0/data

```
{
    "model_run": "/v0/model_run/1",
    "format": "netcdf4",
    "href": "opdata:///arome2_5/arome_metcoop2_5km_20150112T06Z.nc"
}
```

201 Created
Location: https://modelstatus.met.no/v0/data/2

```
{
    "model_run": "/v0/model_run/1",
    "id": "/v0/data/2",
    "format": "netcdf4",
    "href": "opdata:///arome2_5/arome_metcoop2_5km_20150112T06Z.nc",
    "created_time": "2015-01-12T08:26:03Z"
}
```

### Other example requests

#### GET https://modelstatus.met.no/v0/model_run/1

200 OK

```
{
    "id": "/v0/model_run/1",
    "data_provider": "arome_metcoop_2500m",
    "reference_time": "2015-01-12T06:00:00Z",
    "created_time": "2015-01-12T08:25:11Z",
    "version": 0,
    "data": [
                { 
                    "model_run": "/v0/model_run/1",
                    "id": "/v0/data/1",
                    "format": "netcdf4",
                    "href": "opdata:///arome2_5/arome_metcoop_default2_5km_20150112T06Z.nc",
                    "created_time": "2015-01-12T08:26:01Z"
                },
                {
                    "model_run": "/v0/model_run/1",
                    "id": "/v0/data/2",
                    "format": "netcdf4",
                    "href": "opdata:///arome2_5/arome_metcoop2_5km_20150112T06Z.nc",
                    "created_time": "2015-01-12T08:26:03Z"
                }
            
    ]
}
```

#### GET https://modelstatus.met.no/v0/data?data_provider=arome_metcoop_2500m&reference_time=2015-01-12T06:00:00Z

200 OK

```
[ 
  {
      "model_run": "/v0/model_run/1",
      "id": "/v0/data/1",
      "format": "netcdf4",
      "href": "opdata:///arome2_5/arome_metcoop_default2_5km_20150112T06Z.nc",
      "created_time": "2015-01-12T08:26:01Z"
  },
  {
      "model_run": "/v0/model_run/1",
      "id": "/v0/data/2",
      "format": "netcdf4",
      "href": "opdata:///arome2_5/arome_metcoop2_5km_20150112T06Z.nc",
      "created_time": "2015-01-12T08:26:03Z"
  } 
] 
```


