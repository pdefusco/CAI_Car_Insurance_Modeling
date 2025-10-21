#****************************************************************************
# (C) Cloudera, Inc. 2020-2025
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

import osmnx as ox
import geopandas as gpd
import folium
import pandas as pd

# === 1. Load accident data ===
csv_path = "las_vegas_accidents.csv"
df = pd.read_csv(csv_path)

required_cols = [
    'latitude', 'longitude', 'accident_type', 'driver_age', 'driver_gender', 'driver_education',
    'car_make', 'car_model', 'car_year', 'is_driver_at_fault', 'policy_payout',
    'location_type', 'timestamp', 'weather', 'wkt'
]

missing = [col for col in required_cols if col not in df.columns]
assert not missing, f"Missing required columns in CSV: {missing}"

# === 2. Load Las Vegas Metro street network ===
places = [
    "Las Vegas, Nevada, USA", "North Las Vegas, Nevada, USA",
    "Henderson, Nevada, USA", "Summerlin South, Nevada, USA",
    "Paradise, Nevada, USA", "Enterprise, Nevada, USA",
    "Spring Valley, Nevada, USA", "Sunrise Manor, Nevada, USA"
]

ox.settings.default_crs = "EPSG:4326"
G = ox.graph_from_place(places, network_type='drive')
edges = ox.graph_to_gdfs(G, nodes=False, edges=True)

# === 3. Compute map center ===
center = edges.unary_union.centroid
m = folium.Map(location=[center.y, center.x], zoom_start=11, tiles="cartodbpositron")

# === 4. Add light-colored street lines ===
for _, row in edges.iterrows():
    geom = row['geometry']
    color = "#dddddd"  # very light gray
    if geom.geom_type == 'LineString':
        coords = [(lat, lon) for lon, lat in geom.coords]
        folium.PolyLine(coords, color=color, weight=1, opacity=0.4).add_to(m)
    elif geom.geom_type == 'MultiLineString':
        for line in geom:
            coords = [(lat, lon) for lon, lat in line.coords]
            folium.PolyLine(coords, color=color, weight=1, opacity=0.4).add_to(m)

# === 5. Add interactive CircleMarker for each accident ===
for _, row in df.iterrows():
    lat, lon = row['latitude'], row['longitude']

    # Tooltip (shown on hover)
    tooltip = f"{row['accident_type']} Accident"

    # HTML popup (shown on click)
    popup_html = f"""
    <strong>Accident Details</strong><br>
    <b>Type:</b> {row['accident_type']}<br>
    <b>Timestamp:</b> {row['timestamp']}<br>
    <b>Location Type:</b> {row['location_type']}<br>
    <b>Weather:</b> {row['weather']}<br><br>

    <strong>Driver Info</strong><br>
    <b>Age:</b> {row['driver_age']}<br>
    <b>Gender:</b> {row['driver_gender']}<br>
    <b>Education:</b> {row['driver_education']}<br><br>

    <strong>Vehicle Info</strong><br>
    <b>Make:</b> {row['car_make']}<br>
    <b>Model:</b> {row['car_model']}<br>
    <b>Year:</b> {row['car_year']}<br><br>

    <strong>Insurance</strong><br>
    <b>Driver at Fault:</b> {"Yes" if row['is_driver_at_fault'] else "No"}<br>
    <b>Policy Payout:</b> ${row['policy_payout']:,.2f}
    """

    popup = folium.Popup(popup_html, max_width=300, min_width=200)

    # Add CircleMarker
    folium.CircleMarker(
        location=[lat, lon],
        radius=4,
        color='red',
        fill=True,
        fill_color='red',
        fill_opacity=0.8,
        tooltip=tooltip,
        popup=popup
    ).add_to(m)

# === 6. Save and display ===
m.save("las_vegas_accidents_interactive.html")
print("✅ Map saved as 'las_vegas_accidents_interactive.html'")
m
