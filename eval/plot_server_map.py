import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

# 1. Define map boundaries for North America
lat_min, lat_max = 15, 48     # For instance, from Central America up to Northern Canada
lon_min, lon_max = -125, -70  # Roughly west coast of Alaska to east coast of Canada/US

# 2. Read in the CSV data
df = pd.read_csv('location_distributions/server_netflix_oca.csv')

# 3. Filter rows to show only those within our North America boundaries
df_na = df[
    (df['Latitude'] >= lat_min) & 
    (df['Latitude'] <= lat_max) &
    (df['Longitude'] >= lon_min) & 
    (df['Longitude'] <= lon_max)
]

# 4. Set up the map using Basemap (in 'merc' projection)
plt.figure(figsize=(4, 3))
m = Basemap(projection='merc',
            llcrnrlat=lat_min, urcrnrlat=lat_max,
            llcrnrlon=lon_min, urcrnrlon=lon_max,
            resolution='i')  # 'i' for intermediate detail

# 5. Draw map features
m.fillcontinents(color='lightgray', lake_color='white')
m.drawmapboundary(fill_color='white')

# 6. Convert lat/lon to x/y on the map
x, y = m(df_na['Longitude'].values, df_na['Latitude'].values)

# 7. Plot server locations
m.scatter(x, y, marker='o', color='white', zorder=5, label='Edge server', s=20, edgecolor='black', linewidth=1.0)

plt.legend(loc='lower left', fontsize=15)
plt.savefig("plots/xdn_edge_servers_us.pdf", bbox_inches='tight')
plt.show()
