import pandas
import numpy as np
import time


def cleaner(pixel_pairs, **kwargs):
    try:
        cube = kwargs['cube']

        x = pixel_pairs[0]
        y = pixel_pairs[1]

        ts = cube[dict([('x', x), ('y', y)])]
        ts = ts.to_series()

        # mean with nodata value included
        aggreg = ts.groupby([ts.index.month, ts.index.day]).median()

        # mean without nan included
        ntnlTs = ts.mask(ts > 250)
        ntnlAgg = ntnlTs.groupby([ntnlTs.index.month, ntnlTs.index.day]).median()
        ntnlAgg_min = ntnlTs.min()

        ntnlAggMth = None
        aggTriMth = None
        ntnlAggTriMth = None
        aggMth = None
        aggYR = None

        nanList = ts.where(ts > 250).dropna()

        for index, value in nanList.items():
            aggVal = aggreg.loc[index.month, index.day]

            if value in [251, 252, 255]:
                if 250 < aggVal < 252.5 or aggVal >= 254.5:
                    if aggMth is None:
                        aggMth = ts.groupby([ts.index.month]).median()
                    if 250 < aggMth.loc[index.month] < 252.5 or aggMth.loc[index.month] >= 254.5:
                        if aggTriMth is None:
                            aggTriMth = ts.groupby([ts.index.quarter]).median()
                        if 250 < aggTriMth.loc[index.quarter] < 252.5:
                            if aggYR is None:
                                aggYR = ts.groupby([ts.index.year]).min()
                            # nanList.loc[index] = aggYR.loc[index.year]
                            nanList.loc[index] = aggYR.min()
                        else:
                            if ntnlAggTriMth is None:
                                ntnlAggTriMth = ntnlTs.groupby([ts.index.quarter]).median()
                            nanList.loc[index] = ntnlAggTriMth.loc[index.quarter]
                    else:
                        if ntnlAggMth is None:
                            ntnlAggMth = ntnlTs.groupby([ts.index.month]).median()
                        nanList.loc[index] = ntnlAggMth.loc[index.month]
                elif 252.5 <= aggVal < 254.5:
                    nanList.loc[index] = 0
                else:
                    nanList.loc[index] = ntnlAgg.loc[index.month, index.day]

            elif value == 253:
                if aggVal > 250:
                    nanList.loc[index] = ntnlAgg_min
                else:
                    # nanList.loc[index] = ntnlAgg.loc[index.month, index.day]
                    nanList.loc[index] = np.NaN

        ts.update(nanList)

        ts[ts == 253] = np.NaN
        ts = ts.interpolate()
        time.sleep(0.5)
    except (RuntimeError,):
        print('error on{}'.format(pixel_pairs))

    return ts, pixel_pairs