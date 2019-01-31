from dask.distributed import Client, as_completed
import xarray as xr
import numpy as np
import pandas as pd
import webbrowser
import multiprocessing
from pympler import asizeof


def function(chunk, **kwargs):

    rowi = kwargs.pop('data', '')
    multiplier = kwargs.pop('multiplier', '')

    chunk_out = pd.DataFrame(index=rowi.time, columns=pd.DataFrame(chunk)[1])

    i = None
    try:
        for i in chunk:
            ts = rowi.isel(dict([('y', i[1])])).to_series().astype(float)
            ts += multiplier    # fake algorithm
            chunk_out[:, i[1]] = ts
            del ts
    except:
        pass

    return chunk_out


def main():

    # Data creation
    times = pd.date_range('2000-01-01', periods=100)
    x = range(3)
    y = range(int(14e4))
    cube = xr.DataArray(np.random.rand(len(times), len(x), len(y)), coords=[times, x, y], dims=['time', 'x', 'y'])
    print(asizeof.asizeof(cube)/1e6)
    pixels_pairs = np.argwhere(cube.isel(time=0).values)

    # Client
    client = Client(processes=False, n_workers=4, threads_per_worker=1)
    url = 'http://localhost:8787/status'
    webbrowser.open_new(url)

    for x in cube.x:
        row = cube.isel(dict([('x', x.values)]))
        print(asizeof.asizeof(row)/1e6)

        px_list = [x for x in pixels_pairs if x[1] == 1]
        output_carrier = pd.DataFrame(index=cube.time, columns=cube.y)

        chunks = np.array_split(px_list, multiprocessing.cpu_count()*4)
        rowi = client.scatter(row, broadcast=True)
        futures = client.map(function, chunks, **{'data': rowi, 'multiplier': 10})

        for future, result in as_completed(futures, with_results=True):
            output_carrier.update(result)
            del future, result

        cube[x] = np.expand_dims(output_carrier.transpose().values, axis=0)

    print(cube)
    client.close()


if __name__ == '__main__':
    main()

    # def save(ts, pixel, rowout):
    #
    #     rowout.NDVI[pixel[0], pixel[1], :] = ts

    # def row_calc(xi, c3d):
    #
    #     times = c3d.size['time']
    #     y_s = c3d.size['y']
    #     y_d, time = range(y_s), range(times)
    #
    #     pixels_pairs = np.indices((xi, y_s)).transpose((1, 2, 0)) \
    #         .reshape((xi * y_s, 2))
    #     logging.info('Pairs created')
    #
    #     rowout = xr.DataArray(np.random.rand(1, y_s, len(times)),
    #                           coords=[xi, y_d, times],
    #                           dims=['x', 'y', 'time'],
    #                           name='NDVI')
    #
    #     delayed_objs = [delayed(cleaner)(c3d.isel(x=x, y=y).to_series()).persist()
    #                     for x, y in pixels_pairs]
    #     logging.info('Delayed array created')
    #
    #     arr_out = [delayed(save)(ts, pixel, rowout) for ts, pixel in zip(delayed_objs, pixels_pairs)]
    #     logging.info('delayed wrintg created')
    #
    #     logging.info('Computing created')

    # def main():
    #
    #     logging.info('Start')
    #
    #     size = 10
    #     times = pd.date_range('2000-01-01', periods=size)
    #     x, y = range(int(5)), range(int(20))
    #
    #     cube = xr.DataArray(np.random.rand(len(x), len(y), len(times)),
    #                         coords=[x, y, times],
    #                         dims=['x', 'y', 'time'],
    #                         name='NDVI')
    #     logging.info('Cube created')
    #
    #     x_s, y_s = int(cube.sizes['x']), int(cube.sizes['y'])
    #
    #     c3d = delayed(cube)
    #
    #     rows = [delayed(row_calc)(x_i, c3d) for x_i in range(x_s)]
    #
    #     dd.compute(rows)
    #
    #     logging.info('end')

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
