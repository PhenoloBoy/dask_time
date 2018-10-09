from dask.distributed import Client, as_completed
from dask import delayed
import dask
import xarray as xr
import numpy as np
import pandas as pd
import time
import logging
import webbrowser
import dask.dataframe as dd
import sys


@dask.delayed
def cleaner(ts, **kwargs):

    # cube = kwargs['cube']
    # ts = cube[dict([('x', pixel[0]), ('y', pixel[1])])].to_series().astype(float)
    ts += 5
    return ts


def main_parallel():

    logging.basicConfig(level=logging.DEBUG)
    times = pd.date_range('2000-01-01', periods=100)
    x = range(30)
    y = range(30)
    cube = xr.DataArray(np.random.rand(len(times), len(x), len(y)), coords=[times, x, y], dims=['time', 'x', 'y'])
    logging.info('Cube created')

    x = int(cube.sizes['x'])
    y = int(cube.sizes['y'])

    pixels_pairs = np.indices((x, y)).transpose((1, 2, 0)).reshape((x * y, 2))
    logging.info('Pairs created')

    client = Client()
    logging.info('Client ready')

    cubes = client.scatter(cube)
    logging.info('Scattered')

    futures = client.map(cleaner, pixels_pairs, cube=cubes)
    logging.info('Futures ready')

    url = 'http://localhost:8787/status'
    webbrowser.open_new(url)

    for future, result in as_completed(futures, with_results=True):
        ts, pixel = result
        cube[dict([('x', pixel[0]), ('y', pixel[1])])] = ts

    print(cube)
    logging.info('Done!')

    client.close()


def save(ts, pixel, rowout):

    rowout.NDVI[pixel[0], pixel[1], :] = ts


def row_calc(xi, c3d):

    times = c3d.size['time']
    y_s = c3d.size['y']
    y_d, time = range(y_s), range(times)

    pixels_pairs = np.indices((xi, y_s)).transpose((1, 2, 0)) \
        .reshape((xi * y_s, 2))
    logging.info('Pairs created')

    rowout = xr.DataArray(np.random.rand(1, y_s, len(times)),
                          coords=[xi, y_d, times],
                          dims=['x', 'y', 'time'],
                          name='NDVI')

    delayed_objs = [delayed(cleaner)(c3d.isel(x=x, y=y).to_series()).persist()
                    for x, y in pixels_pairs]
    logging.info('Delayed array created')

    arr_out = [delayed(save)(ts, pixel, rowout) for ts, pixel in zip(delayed_objs, pixels_pairs)]
    logging.info('delayed wrintg created')

    logging.info('Computing created')


def main():

    logging.info('Start')

    size = 10
    times = pd.date_range('2000-01-01', periods=size)
    x, y = range(int(5)), range(int(20))

    cube = xr.DataArray(np.random.rand(len(x), len(y), len(times)),
                        coords=[x, y, times],
                        dims=['x', 'y', 'time'],
                        name='NDVI')
    logging.info('Cube created')

    x_s, y_s = int(cube.sizes['x']), int(cube.sizes['y'])

    c3d = delayed(cube)

    rows = [delayed(row_calc)(x_i, c3d) for x_i in range(x_s)]

    dd.compute(rows)

    logging.info('end')


#
# def out_cube():
#
#     t = 1
#     x, y = 40320, 15680
#     temp, precip = [np.empty((x,y,t))]*2
#     time = pd.date_range('2000-01-01', periods=t)
#
#     lon, lat = range(x), range(y)
#
#     da = xr.DataArray(temp, coords={'time':('t', time),'lat':('x', lat),'lon':('y',lon)}, dims=['time','lat','lon'])
#
#
#     # ds = xr.Dataset({'temperature':(['x', 'y', 't'], temp),
#     #                  'precipitation':(['x', 'y', 't'], precip)},
#     #                  coords={'latitude':('x', lat),
#     #                          'longitude':('y', lon),
#     #                          'time': pd.date_range('2000-01-01', periods=t)})


if __name__ == '__main__':
    logging.basicConfig(level=10)
    logging.info('Entrato')
    main()