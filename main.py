import sys
import time

from dask.delayed import delayed
from dask.distributed import Client, as_completed
import xarray as xr
import numpy as np
import pandas as pd
import webbrowser
import multiprocessing
import dask


@dask.delayed
def function(chunk, **kwargs):

    rowi = kwargs.pop('data', '')
    parameter = kwargs.pop('parameter', '')

    chunk_out = pd.DataFrame(index=rowi.time.values, columns=pd.DataFrame(chunk)[1])

    try:
        for i in chunk:
            ts = rowi.isel(dict([('y', i[1])])).to_series().astype(float)

            # ----- substitute algorithm -----

            ts += parameter + i[1]
            chunk_out[i[1]] = ts

            c = 0
            for g in range(int(1e1)):
                c += 1
            # ----- substitute algorithm -----

    except Exception:
        pass

    # ridx =

    return chunk_out


@dask.delayed
def updater(bit, container):
    container.iloc[:, bit.columns[0]:bit.columns[-1]] = bit
    return container

def main():

    # Data creation
    times = pd.date_range('2000-01-01', periods=100)  # to stress more the system just increase the period value
    x = range(5)
    y = range(int(14e2))
    cube = xr.DataArray(np.random.rand(len(times), len(x), len(y)), coords=[times, x, y], dims=['time', 'x', 'y'])
    pixels_pairs = np.argwhere(cube.isel(time=0).values)

    # Client
    # client = Client(processes=False, n_workers=1, threads_per_worker=1)
    client = Client()

    url = 'http://localhost:8787/status'
    webbrowser.open_new(url)

    for row_idx in cube.x.values:
        row = cube.isel(dict([('x', row_idx)]))

        px_list = [ith for ith in pixels_pairs if ith[0] == row_idx]

        output_carrier = pd.DataFrame(index=cube.time.values, columns=cube.y.values)

        chunks = np.array_split(px_list, multiprocessing.cpu_count()*5)

        rowi = client.scatter(row)
        result = [delayed(function)(chunk, data=rowi, parameter=0)for chunk in chunks]
        update =
        dask.compute(result)

        print('calculation done')

        cube[:, row_idx] = output_carrier.values

    print(cube)
    # client.close()


if __name__ == '__main__':
    main()
