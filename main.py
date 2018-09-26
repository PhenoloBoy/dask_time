from dask.distributed import Client, as_completed
import xarray as xr
import numpy as np
import nodata
import pandas as pd
import re


def read(path):
    return xr.open_rasterio(path)


def main():
    dataset = read(r'c:\temp\chianti.img')

    timedom = pd.to_datetime(re.findall(r'\d\d\d\d\d\d\d\d', dataset.attrs['band_names']))
    cube = dataset.assign_coords(band=timedom)

    x = int(cube.sizes['x'])
    y = int(cube.sizes['y'])

    pixels_pairs = np.indices((x, y)).transpose((1, 2, 0)).reshape((x * y, 2))

    client = Client()

    print('Start futures creation')
    futures = client.map(nodata.cleaner, pixels_pairs, cube=cube)
    print('End future creation')
    import webbrowser

    url = 'http://localhost:8787'
    webbrowser.open_new(url)

    for batch in as_completed(futures, with_results=True).batches():
        for future, result in batch:
            ts, pixel = result
            try:
                cube[dict([('x', pixel[0]), ('y', pixel[1])])] = ts
                cube.to_netcdf(r'c:\temp\test.nc')
            except (RuntimeError,):
                print('error')


if __name__ is '__main__':
    main()
