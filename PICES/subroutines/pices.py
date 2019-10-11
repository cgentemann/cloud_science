def weighted_mean_of_masked_data(data_in,data_mask,data_cond):
    #data_in = input xarray data to have weighted mean
    #data_mask = nan mask eg. land values
    #LME mask T or F values
    global_attrs = data_in.attrs
    R = 6.37e6 #radius of earth in m
    grid_dy,grid_dx = (data_in.lat[0]-data_in.lat[1]).data,(data_in.lon[0]-data_in.lon[1]).data
    dϕ = np.deg2rad(grid_dy)
    dλ = np.deg2rad(grid_dx)
    dA = R**2 * dϕ * dλ * np.cos(np.deg2rad(ds.lat)) 
    pixel_area = dA.where(data_cond)  #pixel_area.plot()
    pixel_area = pixel_area.where(np.isfinite(data_mask))
    total_ocean_area = pixel_area.sum(dim=('lon', 'lat'))
    data_weighted_mean = (data_in * pixel_area).sum(dim=('lon', 'lat'),keep_attrs=True) / total_ocean_area
    data_weighted_mean.attrs = global_attrs  #save global attributes
    for a in data_in:                      #set attributes for each variable in dataset
        gatt = data_in[a].attrs
        data_weighted_mean[a].attrs=gatt
    return data_weighted_mean

def weighted_mean_of_data(data_in,data_cond):
    import numpy as np
    import xarray as xr
    #data_in = input xarray data to have weighted mean
    #data_mask = nan mask eg. land values
    #LME mask T or F values
    global_attrs = data_in.attrs
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
    data_weighted_mean.attrs = global_attrs  #save global attributes
    for a in data_in:                      #set attributes for each variable in dataset
        gatt = data_in[a].attrs
        data_weighted_mean[a].attrs=gatt

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
       
def get_pices_mask():
    import xarray as xr
    filename = './data/PICES/PICES_all_mask360.nc'
    ds = xr.open_dataset(filename)
    ds.close()
    return ds

def get_lme_mask():
    import xarray as xr
    filename = './data/LME/LME_all_mask.nc'
    ds = xr.open_dataset(filename)
    ds.close()
    return ds
    
def get_pices_data(var, ilme, initial_date,final_date):
    import xarray as xr
    import numpy as np
    
    file = get_filename(var)
    print('opening:',file)
    ds = xr.open_dataset(file)
    ds.close()
    
    #subset to time of interest
    ds = ds.sel(time=slice(initial_date,final_date))   
    
    if (str(var).lower()=='current') or (var==3):  #if current data need to mask
        m=ds.mask.sel(time=slice('1992-01-01','2010-01-01')).min('time')
        ds = ds.where(m==1,np.nan)
        ds = ds.drop('mask')
       
    #read in pices LME mask
    ds_mask = get_pices_mask()
    #interpolate mask
    mask_interp = ds_mask.interp_like(ds,method='nearest')

    #create mean for pices region
    cond = (mask_interp.region_mask==ilme)
    tem = weighted_mean_of_data(ds,cond)
    data_mean=tem.assign_coords(region=ilme)

    #make climatology and anomalies using .groupby method
    data_climatology = data_mean.groupby('time.month').mean('time',keep_attrs=True)
    data_anomaly = data_mean.groupby('time.month') - data_climatology
    global_attributes = ds.attrs
    data_anomaly.attrs = global_attributes
    
    return data_mean, data_climatology, data_anomaly

def get_lme_data(var, ilme, initial_date,final_date):
    import xarray as xr
    
    file = get_filename(var)
    #print('opening:',file)]
    ds = xr.open_dataset(file)
    ds.close()
    
    #subset to time of interest
    ds = ds.sel(time=slice(initial_date,final_date))   
    
    #read in mask
    ds_mask = get_lme_mask()
    #interpolate mask
    mask_interp = ds_mask.interp_like(ds,method='nearest')

    #create mean for pices region
    cond = (mask_interp.region_mask==ilme)
    tem = weighted_mean_of_data(ds,cond)
    data_mean=tem.assign_coords(region=ilme)

    #make climatology and anomalies using .groupby method
    data_climatology = data_mean.groupby('time.month').mean('time')
    data_anomaly = data_mean.groupby('time.month') - data_climatology

    return data_mean, data_climatology, data_anomaly
