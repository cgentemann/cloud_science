#pangeo
def get_data():
    import intake
    import dask
    import dask.array as dsa
    import gcsfs
    import fsspec
    import xarray as xr
    #climatology years
    cyr1,cyr2='1993-01-01','2018-12-31'
    
    # CCMP test
    cat_pangeo = intake.open_catalog("https://raw.githubusercontent.com/pangeo-data/pangeo-datastore/master/intake-catalogs/master.yaml")
    ds = cat_pangeo.atmosphere.nasa_ccmp_wind_vectors.to_dask()

    ds = ds.rename({'latitude':'lat','longitude':'lon'})
    ds.coords['lon'] = (ds.coords['lon'] + 180) % 360 - 180
    ds_ccmp = ds.sortby(ds.lon)
    ds_ccmp = ds_ccmp.drop('nobs')
    for var in ds_ccmp:
        tem = ds_ccmp[var].attrs
        tem['var_name']='ccmp_'+str(var)
        ds_ccmp[var].attrs=tem
    ds_ccmp = ds_ccmp.resample(time='D').mean()
    ds_ccmp_clim = ds_ccmp.sel(time=slice(cyr1,cyr2))
    ds_ccmp_clim = ds_ccmp_clim.groupby('time.dayofyear').mean('time',keep_attrs=True,skipna=False)
    
    # AVISO test
    fs = gcsfs.GCSFileSystem(project='pangeo-181919',requester_pays=True)
    zstore = 'gs://pangeo-cmems-duacs/sea_surface_height_clg'
    ds = xr.open_zarr(fs.get_mapper(zstore), consolidated=True)
    ds = ds.rename({'latitude':'lat','longitude':'lon'})
    ds.coords['lon'] = (ds.coords['lon'] + 180) % 360 - 180
    ds_aviso = ds.sortby(ds.lon).drop({'lat_bnds','lon_bnds','crs','err'})
    for var in ds_aviso:
        tem = ds_aviso[var].attrs
        tem['var_name']='aviso_'+str(var)
        ds_aviso[var].attrs=tem
    ds_aviso_clim = ds_aviso.sel(time=slice(cyr1,cyr2))
    ds_aviso_clim = ds_aviso_clim.groupby('time.dayofyear').mean('time',keep_attrs=True,skipna=False)    

    #sst
    #file_location = 's3://mur-sst/zarr'
    #ds = xr.open_zarr(fsspec.get_mapper(file_location, anon=True),consolidated=True)
    #ds_sst = ds.drop({'analysis_error','mask','sea_ice_fraction'})
    #tem = ds_sst.analysed_sst.attrs
    #tem['var_name']='mur_sst'
    #ds_sst.analysed_sst.attrs=tem
    #ds_sst_clim = ds_sst.sel(time=slice(cyr1,cyr2))
    #ds_sst_clim = ds_sst_clim.groupby('time.dayofyear').mean('time',keep_attrs=True,skipna=False)
    
    #get bathymetry from ETOPO1
    fname_topo = '/home/jovyan/data/ETOPO1_Ice_g_gmt4.grd'
    ds = xr.open_dataset(fname_topo)
    ds_topo = ds.rename_dims({'x':'lon','y':'lat'}).rename({'x':'lon','y':'lat'})
    tem = ds_topo.attrs
    ds_topo = ds_topo.rename({'z':'etopo_depth'})
    ds_topo.etopo_depth.attrs=tem
    _, index = np.unique(ds_topo['lon'], return_index=True)
    ds_topo = ds_topo.isel(lon=index)
    _, index = np.unique(ds_topo['lat'], return_index=True)
    ds_topo = ds_topo.isel(lat=index)
    
    ds_color = xr.open_dataset('https://rsg.pml.ac.uk/thredds/dodsC/CCI_ALL-v4.2-DAILY')
    for var in ds_color:
        if not var=='chlor_a':
            ds_color = ds_color.drop(var)

    #put data into a dictionary
    data_dict={'aviso':ds_aviso,
               'wnd':ds_ccmp, 
               'color':ds_color,
              'topo':ds_topo} # 'sst':ds_sst,
    clim_dict={'aviso_clim':ds_aviso_clim,
               'wnd_clim':ds_ccmp_clim}#,
               #'sst_clim':ds_sst_clim}
  
    return data_dict,clim_dict

# on AWS
def get_sst():
    import intake
    import dask
    import dask.array as dsa
    import gcsfs
    import fsspec
    import xarray as xr
    #climatology years
    cyr1,cyr2='1993-01-01','2018-12-31'

    #sst
    file_location = 's3://mur-sst/zarr'
    ds = xr.open_zarr(fsspec.get_mapper(file_location, anon=True),consolidated=True)
    ds_sst = ds.drop({'analysis_error','mask','sea_ice_fraction'})
    tem = ds_sst.analysed_sst.attrs
    tem['var_name']='mur_sst'
    ds_sst.analysed_sst.attrs=tem
    ds_sst_clim = ds_sst.sel(time=slice(cyr1,cyr2))
    ds_sst_clim = ds_sst_clim.groupby('time.dayofyear').mean('time',keep_attrs=True,skipna=False)
    
    #put data into a dictionary
    data_dict={'sst':ds_sst}
    clim_dict={'sst_clim':ds_sst_clim}
  
    return data_dict,clim_dict
