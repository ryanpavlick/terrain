#%%

import numpy as np
from terrain import get_cache_root, dem_generate, get_elevation, clear_cache

# Clear the cache (optional, for demonstration purposes)
print("Clearing cache...")
clear_cache()

# Define the bounding box for the region of interest
lat_min, lat_max = 34.0, 34.1  # Example latitudes (Los Angeles area)
lon_min, lon_max = -118.3, -118.2  # Example longitudes (Los Angeles area)

# Define the latitude and longitude arrays
latitude = np.array([lat_min, lat_max])
longitude = np.array([lon_min, lon_max])

# Generate the DEM file
print("Generating DEM file for the specified region...")
try:
    dem_file = dem_generate(latitude, longitude, buffer_size=0.1)
    print(f"DEM file generated: {dem_file}")
except FileNotFoundError as e:
    print(f"Error: {e}")

# Generate random points within the bounding box
print("Generating random points within the bounding box...")
num_points = 100
random_lats = np.random.uniform(lat_min, lat_max, num_points)
random_lons = np.random.uniform(lon_min, lon_max, num_points)

# Extract elevation for random points
print("Extracting elevation values for random points...")
try:
    elevations = get_elevation(random_lats, random_lons, dem_file)
    for i in range(min(10, num_points)):  # Display the first 10 points
        print(f"Point {i + 1}: Lat={random_lats[i]:.5f}, Lon={random_lons[i]:.5f}, Elevation={elevations[i]:.2f} meters")
    if num_points > 10:
        print(f"...and {num_points - 10} more points.")
except Exception as e:
    print(f"Error extracting elevation: {e}")
# %%
