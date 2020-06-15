
## Generating rdb files for testing
The `create_test_rdb_file.py` file will start a redis container from RedisTimeSeries dockerhub repo, load the data and save
the rdb file in `rdbs/` directory with the appropriate version number.
The script will load the data from `GlobalLandTemperaturesByMajorCity.csv`.
```bash
python3  create_test_rdb_file.py 1.2.5
```


## Notice regarding GlobalLandTemperaturesByMajorCity.csv
Original data from [kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data/data) and released
under CC BY-NC-SA 4.0 license.
The raw data comes from the [Berkeley Earth data page](http://berkeleyearth.org/data/).
