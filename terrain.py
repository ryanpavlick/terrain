#%%
import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
import shutil

import numpy as np
import requests
from filelock import FileLock
from shapely.geometry import box
from rtree import index
from osgeo import gdal
from osgeo.gdalconst import GA_ReadOnly

#%%
import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
import shutil
import requests
from filelock import FileLock
from shapely.geometry import box
from rtree import index
from osgeo import gdal
from osgeo.gdalconst import GA_ReadOnly

#%%
def get_cache_root():
    """
    Get the root directory for caching files.

    Returns:
        str: Path to the cache root directory.
    """
    return os.environ.get('TERRAIN_CACHE_ROOT', f"{tempfile.gettempdir()}/terrain")


def clear_cache():
    """
    Clears the entire cache directory after confirming it is safe to do so.
    """
    cache_dir = get_cache_root()
    if not cache_dir.startswith(tempfile.gettempdir()):
        raise ValueError(f"Refusing to clear unsafe cache directory: {cache_dir}")
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
        logging.info(f"Cache directory {cache_dir} cleared.")
    else:
        logging.info(f"Cache directory {cache_dir} does not exist.")


def clear_localdem_cache():
    """
    Clears the cache directory containing downloaded DEM tiles.

    This function deletes the `localdem` directory, which stores raw DEM tiles
    downloaded from the AWS Copernicus DEM repository. It is useful for freeing
    up disk space or resetting the cache when needed.

    Logs:
        - If the directory exists, logs that it has been cleared.
        - If the directory does not exist, logs a message indicating its absence.
    """
    localdem_dir = f"{get_cache_root()}/localdem"
    if os.path.exists(localdem_dir):
        shutil.rmtree(localdem_dir)
        logging.info(f"Local DEM cache cleared: {localdem_dir}")
    else:
        logging.info(f"Local DEM cache not found: {localdem_dir}")


def log_cache_usage():
    """
    Logs the total disk space used by the cache directory.

    This function calculates the total size of all files in the cache directory
    (including both `localdem` and `dem_cache` subdirectories) and logs the result
    in megabytes (MB).
    """
    cache_dir = get_cache_root()
    total_size = sum(
        os.path.getsize(os.path.join(dirpath, filename))
        for dirpath, _, filenames in os.walk(cache_dir)
        for filename in filenames
    )
    logging.info(f"Total cache usage: {total_size / (1024 * 1024):.2f} MB")


def download_file(file, url, chunk_size=1 * 1024 * 1024): # 1 MB Chunks
    """
    Download a file from a specified URL to a local path.

    Args:
        file (str): Local file path to save the downloaded file.
        url (str): URL of the file to download.
        chunk_size (int): Size of chunks to download at a time (in bytes).

    Raises:
        RuntimeError: If the download fails.
    """
    try:
        with requests.get(url, stream=True, timeout=30) as response:
            response.raise_for_status()
            with open(file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to download {url}: {e}")


def dem_generate(latitude, longitude, buffer_size=None, aws_dir='https://copernicus-dem-30m.s3.amazonaws.com/'):
    """
    Generate a DEM file covering a specified region.

    Args:
        latitude (array-like): Array of latitude values.
        longitude (array-like): Array of longitude values.
        buffer_size (float): Buffer size to expand the bounding box (in degrees).
        aws_dir (str): Base URL of the DEM tile directory.

    Returns:
        str: Path to the generated DEM file.
    """
    buffer_size = buffer_size or float(os.getenv("DEM_BUFFER_SIZE", 0.1))
    dem_cache_dir = f"{get_cache_root()}/dem_cache"
    os.makedirs(dem_cache_dir, exist_ok=True)  # Ensure the cache directory exists

    # Expand bounding box with buffer
    lon_min, lon_max = np.min(longitude) - buffer_size, np.max(longitude) + buffer_size
    lat_min, lat_max = np.min(latitude) - buffer_size, np.max(latitude) + buffer_size

    # Check for sufficient disk space (100 MB estimate per tile)
    check_disk_space(100)

    # Cached file name for the DEM
    cache_filename = f"{dem_cache_dir}/{int(lat_min)}_{int(lon_min)}_{int(lat_max)}_{int(lon_max)}.raw"
    cache_file_lock = FileLock(cache_filename + ".lock")
    with cache_file_lock:
        if os.path.exists(cache_filename):
            logging.info("Found cached DEM file %s", cache_filename)
            return cache_filename

        # Download tiles and merge them into a DEM file
        tile_files = download_dem_files(lon_min, lat_min, lon_max, lat_max, aws_dir)
        if not tile_files:
            raise FileNotFoundError("No DEM tiles available for the specified area.")

        logging.info("Merging DEM tiles into %s", cache_filename)
        merge_tiles(cache_filename, tile_files)

        # Optionally clean up intermediate tile files
        remove_intermediate_tiles(tile_files)

    return cache_filename


def remove_intermediate_tiles(tile_files):
    """
    Deletes intermediate tile files to save space.

    Args:
        tile_files (list): List of tile file paths to delete.
    """
    for tile in tile_files:
        if os.path.exists(tile):
            os.remove(tile)
            logging.info(f"Removed intermediate tile file: {tile}")



def dem_generate(latitude, longitude, buffer_size=None, aws_dir='https://copernicus-dem-30m.s3.amazonaws.com/'):
    """
    Generate a DEM file covering a specified region.

    Args:
        latitude (array-like): Array of latitude values.
        longitude (array-like): Array of longitude values.
        buffer_size (float): Buffer size to expand the bounding box (in degrees).
        aws_dir (str): Base URL of the DEM tile directory.

    Returns:
        str: Path to the generated DEM file.
    """
    buffer_size = buffer_size or float(os.getenv("DEM_BUFFER_SIZE", 0.1))
    dem_cache_dir = f"{get_cache_root()}/dem_cache"
    os.makedirs(dem_cache_dir, exist_ok=True)  # Ensure the cache directory exists

    # Expand bounding box with buffer
    lon_min, lon_max = np.min(longitude) - buffer_size, np.max(longitude) + buffer_size
    lat_min, lat_max = np.min(latitude) - buffer_size, np.max(latitude) + buffer_size


    # Cached file name for the DEM
    cache_filename = f"{dem_cache_dir}/{int(lat_min)}_{int(lon_min)}_{int(lat_max)}_{int(lon_max)}.raw"
    cache_file_lock = FileLock(cache_filename + ".lock")
    with cache_file_lock:
        if os.path.exists(cache_filename):
            logging.info("Found cached DEM file %s", cache_filename)
            return cache_filename

        # Download tiles and merge them into a DEM file
        tile_files = download_dem_files(lon_min, lat_min, lon_max, lat_max, aws_dir)
        if not tile_files:
            raise FileNotFoundError("No DEM tiles available for the specified area.")

        logging.info("Merging DEM tiles into %s", cache_filename)
        merge_tiles(cache_filename, tile_files)

        # Optionally clean up intermediate tile files
        remove_intermediate_tiles(tile_files)

    return cache_filename


def get_elevation(lats, lons, dem_file):
    """
    Extract elevation values for given latitude and longitude points.

    Args:
        lats (array-like): Array of latitude values.
        lons (array-like): Array of longitude values.
        dem_file (str): Path to the DEM file.

    Returns:
        numpy.ndarray: Array of elevation values.
    """
    dataset = gdal.Open(dem_file, GA_ReadOnly)
    geotransform = dataset.GetGeoTransform()

    def get_pixel(lat, lon):
        """
        Convert latitude and longitude to pixel coordinates.
        """
        x = int((lon - geotransform[0]) / geotransform[1])
        y = int((lat - geotransform[3]) / geotransform[5])
        return x, y

    elevations = []
    try:
        for lat, lon in zip(lats, lons):
            x, y = get_pixel(lat, lon)
            band = dataset.GetRasterBand(1)
            elevations.append(band.ReadAsArray(x, y, 1, 1)[0][0])
    finally:
        dataset = None  # Ensure GDAL dataset is closed

    return np.array(elevations)

# %%
