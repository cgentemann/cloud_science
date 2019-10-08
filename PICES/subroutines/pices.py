def weighted_mean_of_masked_data(data_in,data_mask,data_cond):
    #data_in = input xarray data to have weighted mean
    #data_mask = nan mask eg. land values
    #LME mask T or F values
    R = 6.37e6 #radius of earth in m
    grid_dy,grid_dx = (data_in.lat[0]-data_in.lat[1]).data,(data_in.lon[0]-data_in.lon[1]).data
    dϕ = np.deg2rad(grid_dy)
    dλ = np.deg2rad(grid_dx)
    dA = R**2 * dϕ * dλ * np.cos(np.deg2rad(ds.lat)) 
    pixel_area = dA.where(data_cond)  #pixel_area.plot()
    pixel_area = pixel_area.where(np.isfinite(data_mask))
    total_ocean_area = pixel_area.sum(dim=('lon', 'lat'))
    data_weighted_mean = (data_in * pixel_area).sum(dim=('lon', 'lat')) / total_ocean_area
    return data_weighted_mean

def weighted_mean_of_data(data_in,data_cond):
    import numpy as np
    import xarray as xr
    #data_in = input xarray data to have weighted mean
    #data_mask = nan mask eg. land values
    #LME mask T or F values
    R = 6.37e6 #radius of earth in m
    grid_dy,grid_dx = (data_in.lat[0]-data_in.lat[1]).data,(data_in.lon[0]-data_in.lon[1]).data
    dϕ = np.deg2rad(grid_dy)
    dλ = np.deg2rad(grid_dx)
    dA = R**2 * dϕ * dλ * np.cos(np.deg2rad(data_in.lat)) 
    pixel_area = dA.where(data_cond)  #pixel_area.plot()
    #pixel_area = pixel_area.where(np.isfinite(data_mask))
    sum_data=(data_in*pixel_area).sum(dim=('lon', 'lat'),keep_attrs=True)
    total_ocean_area = pixel_area.sum(dim=('lon', 'lat'))
    #print(sum_data)
    #print(total_ocean_area)
    data_weighted_mean = sum_data/total_ocean_area
    return data_weighted_mean


def get_filename(var):
    if (str(var).lower()=='sst') or (var==1):
        file='./data/sst.mnmean.nc'
    if (str(var).lower()=='wind') or (var==2):
        file='./data/wind.mnmean.nc'
    if (str(var).lower()=='current') or (var==3):
        file='./data/cur.mnmean.nc'
    if (str(var).lower()=='chl') or (var==4):
        file='./data/chl.mnmean.nc'
    return file
       
def get_mask():
    import xarray as xr
    #read in mask file
#    filename = './data/PICES_all_mask.nc'
#    ds_pices = xr.open_dataset(filename)
#    ds_pices.close()
    #read in mask file
    filename = './data/PICES_all_mask360.nc'
    ds_pices360 = xr.open_dataset(filename)
    ds_pices360.close()
    return ds_pices360
#    ds_pices_revlat = ds_pices.sortby(ds_pices.lat, ascending = False)
#    ds_pices360_revlat = ds_pices360.sortby(ds_pices360.lat, ascending = False)

    
def get_data(var, ilme, initial_date,final_date):
    import xarray as xr
    
    file = get_filename('sst')
    #print('opening:',file)
    ds = xr.open_dataset(file)
    ds.close()
    
    #subset to time of interest
    ds = ds.sel(time=slice(initial_date,final_date))   
    
    #read in mask
    ds_pices360 = get_mask()
    #interpolate mask
    mask_interp = ds_pices360.interp_like(ds,method='nearest')

    #create mean for pices region
    cond = (mask_interp.region_mask==ilme)
    tem = weighted_mean_of_data(ds,cond)
    data_mean=tem.assign_coords(region=ilme)

    #make climatology and anomalies using .groupby method
    data_climatology = data_mean.groupby('time.month').mean('time')
    data_anomaly = data_mean.groupby('time.month') - data_climatology

    return data_mean, data_climatology, data_anomaly
