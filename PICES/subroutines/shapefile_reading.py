
import shapefile
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
import matplotlib
from shapely.geometry import Polygon
import pyproj
from shapely.geometry import Point
import matplotlib.path as mpltPath
from shapely.geometry.multipolygon import MultiPolygon
import xarray as xr

def explode_polygon(indata):
    indf = indata
    outdf = gpd.GeoDataFrame(columns=indf.columns)
    for idx, row in indf.iterrows():
        if type(row.geometry) == Polygon:
            #note: now redundant, but function originally worked on
            #a shapefile which could have combinations of individual polygons
            #and MultiPolygons
            outdf = outdf.append(row,ignore_index=True)
        if type(row.geometry) == MultiPolygon:
            multdf = gpd.GeoDataFrame(columns=indf.columns)
            recs = len(row.geometry)
            multdf = multdf.append([row]*recs,ignore_index=True)
            for geom in range(recs):
                multdf.loc[geom,'geometry'] = row.geometry[geom]
            outdf = outdf.append(multdf,ignore_index=True)
    return outdf

def get_pices_mask(lats,lons,directory_shapefiles):

    #create 2d grid from lats and lons
    [lon2d,lat2d]=np.meshgrid(lons,lats)

    #create a list of coordinates of all points within grid
    points=[]
    for latit in range(0,lats.size):
        for lonit in range(0,lons.size):
            point=(lon2d[latit,lonit],lat2d[latit,lonit])
            points.append(point)

    #turn into np array for later
    points=np.array(points)

    ##get the cube data - useful for later
    #fld=np.squeeze(cube.data)

    #create a mask array of zeros, same shape as fld, to be modified by
    #the code below
    mask=np.zeros_like(lon2d)

    #NOW, read the shapefile and extract the polygon for a single province

    iregion = []
    mask_name_array=[]
    for root, dirs, files in os.walk(directory_shapefiles, topdown=False):
        if root[len(directory_shapefiles):len(directory_shapefiles)+1]=='.':
            continue
        for name in files:
            if not name.endswith('.shp'):
                continue
            filename=os.path.join(root, name)
            s=int(''.join(filter(str.isdigit, filename)))
            print(s,name)

#    data_dir = 'F:/data/NASA_biophysical/pices/shapefiles/'
#    shp_file_base = 'PicesRegion'+str(iregion)+'.shp'
            df = gpd.read_file(filename)
            crs_source = ('+proj=natearth +ellps=GRS80 +unit=m +lon_0=180')
            df.crs = crs_source  
            df2 = df.to_crs(epsg=4326) 

            #this code from stack overflow Ian Ashpole
            #BritishColumbia.geometry.type reveals this to be a 'MultiPolygon'
            #i.e. several (in this case, thousands...) if individual polygons.
            #to 'explode' the MultiPolygon was found here:
            #https://gist.github.com/mhweber/cf36bb4e09df9deee5eb54dc6be74d26

            #---define function to explode MultiPolygons
            #Explode the MultiPolygon into its constituents
            Edf2=explode_polygon(df2)

            #Loop over each individual polygon and get external coordinates
            for index,row in Edf2.iterrows():

                #print('working on polygon', index)
                mypolygon=[]
                for pt in list(row['geometry'].exterior.coords):
                    #print(index,', ',pt)
                    mypolygon.append(pt)


                #See if any of the original grid points read from the netcdf file earlier
                #lie within the exterior coordinates of this polygon
                #pth.contains_points returns a boolean array (true/false), in the
                #shape of 'points'
                path=mpltPath.Path(mypolygon)
                inside=path.contains_points(points)


                #find the results in the array that were inside the polygon ('True')
                #and set them to missing. First, must reshape the result of the search
                #('points') so that it matches the mask & original data
                #reshape the result to the main grid array
                inside=np.array(inside).reshape(lon2d.shape)
                i=np.where(inside == True)
                mask[i]=1
            ds_mask_tem=xr.Dataset(data_vars={'region_mask': (('lat','lon'),mask) },coords={'lat':lats,'lon':lons})
            mask_name = str(name)+'_mask'
            ds_mask[mask_name]=ds_mask_tem.region_mask
            mask_name_array.append(mask_name)
    return ds_mask
    #print('fininshed checking for points inside all polygons')


