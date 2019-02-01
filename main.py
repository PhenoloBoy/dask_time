from dask.distributed import Client, as_completed
import dask
import xarray as xr
import numpy as np
import pandas as pd
import webbrowser
import multiprocessing
import gc


def function(chunk, **kwargs):

    rowi = kwargs.pop('data', '')
    multiplier = kwargs.pop('multiplier', '')

    chunk_out = pd.DataFrame(index=rowi.time, columns=pd.DataFrame(chunk)[1])

    try:
        for i in chunk:
            ts = rowi.isel(dict([('y', i[1])])).to_series().astype(float)

            # ----- fake algorithm -----

            ts += multiplier
            chunk_out[:, i[1]] = ts

            # ----- fake algorithm -----

            del ts
            gc.collect()
    except:
        pass

    return chunk_out


def main():

    # Data creation
    times = pd.date_range('2000-01-01', periods=200) # to stress more the system just increase the period value
    x = range(3)
    y = range(int(14e4))
    cube = xr.DataArray(np.random.rand(len(times), len(x), len(y)), coords=[times, x, y], dims=['time', 'x', 'y'])
    pixels_pairs = np.argwhere(cube.isel(time=0).values)

    # Client
    client = Client(processes=False, n_workers=1, threads_per_worker=1)
    dask.config.set()

    url = 'http://localhost:8787/status'
    webbrowser.open_new(url)

    for row_idx in cube.x.values:
        row = cube.isel(dict([('x', row_idx)])).persist()

        px_list = [ith for ith in pixels_pairs if ith[0] == row_idx]
        output_carrier = pd.DataFrame(index=cube.time, columns=cube.y)

        chunks = np.array_split(px_list, multiprocessing.cpu_count()*4)
        rowi = client.scatter(row, broadcast=True)
        futures = client.map(function, chunks, **{'data': rowi, 'multiplier': 10})

        for future, result in as_completed(futures, with_results=True):
            output_carrier.update(result)
            del future, result
            gc.collect()

        cube[x] = np.expand_dims(output_carrier.transpose().values, axis=0)

    print(cube)
    client.close()


if __name__ == '__main__':
    main()
