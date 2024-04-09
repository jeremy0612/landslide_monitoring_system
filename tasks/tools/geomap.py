import json
from shapely.geometry import Point, Polygon  # for geometry manipulation
import geopandas as gpd
import matplotlib.pyplot as plt
from geodatasets import get_path

# Read location data from JSON file
with open('../../tmp/buffer/location_data.json', 'r') as infile:
    location_data = json.load(infile)

# Create a list of GeoPandas Point geometries
geometry = [Point(loc['longitude'], loc['latitude']) for loc in location_data]

# Create a GeoDataFrame with the points and any additional data (optional)
gdf = gpd.GeoDataFrame({'geometry': geometry}, crs="EPSG:4326")  # Adjust CRS if needed

# Plot the points on the base map
fig, ax = plt.subplots(figsize=(20, 16))
world = gpd.read_file(get_path("naturalearth.land"))

# We restrict to all the world
ax = world.plot(color="white", edgecolor="black")
# Add labels and title (optional)
ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')
ax.set_title('Landslide Occurences in the World')
# We can now plot our ``GeoDataFrame``.
gdf.plot(ax=ax, color="red", markersize=2) # Customize marker color and size
plt.tight_layout()
plt.savefig("../../assets/landslide_map.png") 

