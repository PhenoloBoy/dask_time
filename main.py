from dask.distributed import Client, as_completed
import xarray as xr
import numpy as np
import pandas as pd
import time


def cleaner(pixel, **kwargs):

    cube = kwargs['cube']
    ts = cube[dict([('x', pixel[0]), ('y', pixel[1])])].to_series().astype(float)
    ts += 5

    time.sleep(0.2)

    return ts, pixel


def main():

    times = pd.date_range('2000-01-01', periods=100)
    x = range(30)
    y = range(30)
    cube = xr.DataArray(np.random.rand(len(times), len(x), len(y)), coords=[times, x, y], dims=['time', 'x', 'y'])

    x = int(cube.sizes['x'])
    y = int(cube.sizes['y'])

    pixels_pairs = np.indices((x, y)).transpose((1, 2, 0)).reshape((x * y, 2))

    client = Client()
    print('Client ready')

    futures = client.map(cleaner, pixels_pairs, cube=cube)
    print('End future creation')

    # import webbrowser
    # url = 'http://localhost:8787/status'
    # webbrowser.open_new(url)

    for future, result in as_completed(futures, with_results=True):
        ts, pixel = result
        cube[dict([('x', pixel[0]), ('y', pixel[1])])] = ts

    print(cube)
    print('Done!')

    client.close()


