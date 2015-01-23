# modelstatus.met.no

## Start application

```
export PYTHONPATH=../
virtualenv virtualenv
source virtualenv/bin/activate
pip install -r requirements.txt
python app.py --config=config.ini
```

## Run tests for application

```
./run_tests.sh
```

## modelstatus.met.no REST API

### Example of requests that need to be made for updating the status of the model arome_metcoop_2500m
 
#### POST https://modelstatus.met.no/modelstatus/v0/model_run

```
{
    "data_provider": "arome_metcoop_2500m",
    "reference_time": "2015-01-12T06:00:00Z"
}
```

201 Created
Location: https://modelstatus.met.no/modelstatus/v0/model_run/1

```
{
    "id": 1,
    "data_provider": "arome_metcoop_2500m",
    "reference_time": "2015-01-12T06:00:00Z",
    "version": 0,
    "created_time": "2015-01-12T08:25:11Z",
    "data": []
}
```

#### POST https://modelstatus.met.no/modelstatus/v0/data

```
{
    "model_run_id": 1,
    "format": "netcdf4",
    "href": "opdata:///arome2_5/arome_metcoop_default2_5km_20150112T06Z.nc"
}
```

201 Created
Location: https://modelstatus.met.no/modelstatus/v0/data/1

```
{
    "model_run_id": 1,
    "id": 1,
    "format": "netcdf4",
    "href": "opdata:///arome2_5/arome_metcoop_default2_5km_20150112T06Z.nc",
    "created_time": "2015-01-12T08:26:01Z"
}
```


#### POST https://modelstatus.met.no/modelstatus/v0/data

```
{
    "model_run_id": 1,
    "format": "netcdf4",
    "href": "opdata:///arome2_5/arome_metcoop2_5km_20150112T06Z.nc"
}
```

201 Created
Location: https://modelstatus.met.no/modelstatus/v0/data/2

```
{
    "model_run": 1,
    "id": 2,
    "format": "netcdf4",
    "href": "opdata:///arome2_5/arome_metcoop2_5km_20150112T06Z.nc",
    "created_time": "2015-01-12T08:26:03Z"
}
```

### Other example requests

#### GET https://modelstatus.met.no/modelstatus/v0/model_run/1

200 OK

```
{
    "id": 1,
    "data_provider": "arome_metcoop_2500m",
    "reference_time": "2015-01-12T06:00:00Z",
    "created_time": "2015-01-12T08:25:11Z",
    "version": 0,
    "data": [
        {
             "model_run_id": 1,
              "id": 1,
              "format": "netcdf4",
              "href": "opdata:///arome2_5/arome_metcoop_default2_5km_20150112T06Z.nc",
              "created_time": "2015-01-12T08:26:01Z"
         },
         {
              "model_run": 1,
              "id": 2,
              "format": "netcdf4",
              "href": "opdata:///arome2_5/arome_metcoop2_5km_20150112T06Z.nc",
              "created_time": "2015-01-12T08:26:03Z"
          }
    ]
}
```

#### GET https://modelstatus.met.no/modelstatus/v0/model_run?data_provider=arome_metcoop_2500m

200 OK

```
[
    {
        "reference_time": "2015-01-22T12:00:00+00:00",
        "version": 1,
        "created_date": "2015-01-22T14:27:58.924145+00:00",
        "data_provider": "arome_metcoop_2500m",
        "data": [
            {
                "model_run_id": 3,
                "href": "opdata:///opdata/arome2_5/AROME_MetCoOp_12_DEF.nc",
                "id": 3,
                "format": "netcdf"
            }
        ],
        "id": 3
    },
    {
        "reference_time": "2015-01-22T18:00:00+00:00",
        "version": 1,
        "created_date": "2015-01-22T20:29:24.190916+00:00",
        "data_provider": "arome_metcoop_2500m",
        "data": [
            {
                "model_run_id": 11,
                "href": "opdata:///opdata/arome2_5/AROME_MetCoOp_18_DEF.nc",
                "id": 68,
                "format": "netcdf"
            }
        ],
        "id": 11
    }
]
```
