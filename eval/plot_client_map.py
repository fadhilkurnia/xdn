import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

lat_min, lat_max = 15, 48     # For instance, from Central America up to Northern Canada
lon_min, lon_max = -125, -70  # Roughly west coast of Alaska to east coast of Canada/US

# 1. Read in the CSV file
df = pd.read_csv('location_distributions/client_us_metro_population.csv')

# 2. Parse the 'location' column into separate latitude/longitude
#    The data is stored as "lat;lon". Let's split by ';'.
df = df[df['location'] != 'NA;NA']
df[['Latitude', 'Longitude']] = df['location'].str.split(';', expand=True).astype(float)
df = df.sort_values('population', ascending=False).head(50)

plt.figure(figsize=(4, 3))
m = Basemap(projection='merc',
            llcrnrlat=lat_min, urcrnrlat=lat_max,
            llcrnrlon=lon_min, urcrnrlon=lon_max,
            resolution='i')

m.fillcontinents(color='lightgray', lake_color='white')
m.drawmapboundary(fill_color='white')

x, y = m(df['Longitude'].values, df['Latitude'].values)

m.scatter(x, y,
          s=df['population'] / 100_000,   # You can adjust this scaling factor as you like
          color='red',
          alpha=0.7,
          marker='o',
          edgecolors='black',
          linewidths=1,
          zorder=5)

# 3. Create "dummy" points for a size legend
#    Each tuple is (population_value, legend_label)
legend_populations = [
    (100000, "100k"),
    (500000, "500k"),
    (1000000, "1M"),
    (5000000, "5M"),
]
for pop_value, label in legend_populations:
    plt.scatter(
        [], [],  # no x/y data
        s=pop_value / 100_000,
        color='red',
        alpha=0.7,
        marker='o',
        edgecolors='black',
        linewidths=1,
        label=label
    )
# Add a legend that explains circle sizes
leg = plt.legend(
    scatterpoints=1,
    frameon=True,
    title="Population",
    loc="lower center",
    ncol=4,
    reverse=True,
    borderpad=0.25,
    columnspacing=0.0,
    handletextpad=0.3,
    framealpha=0.90,
    fontsize=12
)
leg._legend_box.align = "center"

# Optionally, add city names as labels:
df = df.sort_values('population', ascending=False).head(15)
txt_left_margin=150000.0
txt_top_margin=200_000.0
excluded_city_names = {"Riverside", "Miami", "Detroit", "Phoenix", "Boston", "New York", "Philadelphia", "Washington"}
for city, xx, yy in zip(df['city'], x, y):
    if city in excluded_city_names:
      continue
    plt.text(xx+txt_left_margin, yy-txt_top_margin, city, fontsize=9, ha='left', va='bottom', zorder=30)

plt.savefig("plots/xdn_client_us.pdf", bbox_inches='tight')
plt.show()