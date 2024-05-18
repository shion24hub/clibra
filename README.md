# Simple Example

To add Bybit BTCUSDT data from January 1 to 3, 2024 to the data store
``` console
$ clibra update bybit BTCUSDT 20240101 20240103
Target: /Users/hospital/.clibra/candles/bybit/BTCUSDT/2024-01-01.csv.gz
    - Downloaded https://public.bybit.com/trading/BTCUSDT/BTCUSDT2024-01-01.csv.gz.
    - Processed the data. 61199 rows.
    - Saved the data.
...
All processes are completed.
Elapsed time: 0:00:19.009671
```

To save to csv file in the current directory
```
$ clibra generate bybit BTCUSDT 20240101 20240103 60
Target: /Users/hospital/.clibra/candles/bybit/BTCUSDT/2024-01-01.csv.gz
    - OK.
...
Saved the data to ./bybit_BTCUSDT_20240101_20240103_60.csv.gz.
All processes are completed.
Elapsed time: 0:00:00.285979
```

To check the symbols already stored
```
$ clibra show
Total size: 2.86 MB
bybit: BTCUSDT from 2024-01-01 to 2024-01-03, 0 missing dates
```

To delete symbol data that is no longer needed, specifying a date
```
$ clibra remove bybit BTCUSDT 20240101 20240102
Do you really want to remove 2024-01-01 00:00:00-2024-01-02 00:00:00 data for BTCUSDT? (y/n): y
Target: /Users/hospital/.clibra/candles/bybit/BTCUSDT/2024-01-01.csv.gz
    - removed.
...
All processes are completed.
Elapsed time: 0:00:02.020796
```

# How to delete

If for some reason you want to delete all data related to clibra, delete the `$HOME/.clibra` directory completely. (`$HOME/.clibra` is the only working directory used by clibra.)

**Be careful not to accidentally delete another directory.**