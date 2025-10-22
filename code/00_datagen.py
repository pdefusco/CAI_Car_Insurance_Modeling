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
import random
import numpy as np
import pandas as pd
from shapely.geometry import Point
import calendar
from datetime import datetime

# --- CONFIGURATION ---

hotspots = {
    "charleston_rainbow": {
        "coords": (36.1593, -115.2457),
        "accident_weights": {
            "t bone collision": 0.4,
            "rear end collision": 0.2,
            "side collision": 0.2,
            "head on collision": 0.1,
            "single vehicle accident": 0.1
        }
    },
    "tropicana_rainbow": {
        "coords": (36.1010, -115.2446),
        "accident_weights": {
            "rear end collision": 0.5,
            "t bone collision": 0.2,
            "side collision": 0.2,
            "head on collision": 0.05,
            "single vehicle accident": 0.05
        }
    },
    "flamingo_rainbow": {
        "coords": (36.1153, -115.2449),
        "accident_weights": {
            "side collision": 0.4,
            "rear end collision": 0.3,
            "t bone collision": 0.2,
            "head on collision": 0.05,
            "single vehicle accident": 0.05
        }
    },
    "sahara_decatur": {
        "coords": (36.1443, -115.2050),
        "accident_weights": {
            "head on collision": 0.4,
            "t bone collision": 0.3,
            "rear end collision": 0.15,
            "side collision": 0.1,
            "single vehicle accident": 0.05
        }
    },
    "charleston_lamb": {
        "coords": (36.1592, -115.0702),
        "accident_weights": {
            "single vehicle accident": 0.4,
            "t bone collision": 0.3,
            "rear end collision": 0.1,
            "side collision": 0.1,
            "head on collision": 0.1
        }
    },
    "las_vegas_strip": {
        "coords": (36.1147, -115.1728),
        "accident_weights": {
            "rear end collision": 0.35,
            "t bone collision": 0.35,
            "side collision": 0.15,
            "head on collision": 0.1,
            "single vehicle accident": 0.05
        },
        "strip_centroids": [
            (36.0825, -115.1719),
            (36.0878, -115.1721),
            (36.0999, -115.1723),
            (36.1069, -115.1725),
            (36.1126, -115.1727),
            (36.1210, -115.1729)
        ]
    }
}

HOLIDAYS = [(1,1), (7,4), (11,25), (12,25)]
WEATHER_TYPES = ["clear", "rain", "snow"]

genders = ["male", "female", "non-binary"]
education_levels = ["high school", "some college", "bachelor's", "master's", "PhD"]
car_makes_models = [
    ("Toyota", "Camry"), ("Honda", "Civic"), ("Ford", "Focus"),
    ("Chevrolet", "Malibu"), ("Nissan", "Altima"), ("Tesla", "Model 3")
]
default_accident_weights = {
    "t bone collision": 0.2,
    "rear end collision": 0.2,
    "side collision": 0.2,
    "head on collision": 0.2,
    "single vehicle accident": 0.2
}

# --- HELPER FUNCTIONS ---

def sample_near_hotspot(center, radius_meters=300):
    lat, lon = center
    delta_lat = radius_meters / 111_000
    delta_lon = radius_meters / (111_000 * np.cos(np.radians(lat)))
    new_lat = np.random.normal(lat, delta_lat / 2)
    new_lon = np.random.normal(lon, delta_lon / 2)
    return new_lat, new_lon

def sample_along_strip(centroids):
    base_point = random.choice(centroids)
    jitter_lat = np.random.normal(0, 0.00045)
    jitter_lon = np.random.normal(0, 0.00045)
    return base_point[0] + jitter_lat, base_point[1] + jitter_lon

def is_holiday(month, day):
    return (month, day) in HOLIDAYS

def generate_timestamp_and_weather(hotspot_key=None, year=None):
    if not year:
        year = random.randint(2020, 2025)

    month_weights = {
        12: 1.2, 1: 1.2, 2: 1.1, 6: 1.2, 7: 1.3, 8: 1.1,
        3: 1.0, 4: 1.0, 5: 1.0, 9: 1.0, 10: 1.0, 11: 1.0
    }
    months = list(month_weights.keys())
    month = random.choices(months, weights=month_weights.values(), k=1)[0]
    day = random.randint(1, calendar.monthrange(year, month)[1])
    holiday = is_holiday(month, day)
    weather = random.choices(WEATHER_TYPES, weights=[0.7, 0.25, 0.05])[0]

    if holiday:
        hour = random.choice(range(18,24))
    elif hotspot_key in ["charleston_rainbow", "tropicana_rainbow"]:
        hour = random.choice([0,1,2,3,21,22,23,12])
    elif hotspot_key in ["sahara_decatur", "charleston_lamb", "flamingo_rainbow"]:
        hour = random.choice([7,8,9,16,17,18,12])
    elif hotspot_key == "las_vegas_strip":
        hour = random.choice([22,23,0,1,2,3])
    else:
        hour = random.randint(0,23)

    minute = random.randint(0,59)
    try:
        dt = datetime(year, month, day, hour, minute, 0)
    except ValueError:
        dt = datetime(year, month, 1, hour, minute, 0)

    return dt.strftime("%Y-%m-%d %H:%M:%S"), weather, month

def choose_accident_type(base_weights, month, weather):
    weights = base_weights.copy()
    if month in [12,1,2]:
        weights["rear end collision"] += 0.1
        weights["single vehicle accident"] += 0.1
    elif month in [6,7,8]:
        weights["t bone collision"] += 0.1
        weights["side collision"] += 0.1
    if weather == "rain":
        weights["rear end collision"] += 0.2
        weights["single vehicle accident"] += 0.1
    elif weather == "snow":
        weights["single vehicle accident"] += 0.3
        weights["rear end collision"] += 0.2

    total = sum(weights.values())
    norm_weights = {k: v/total for k,v in weights.items()}
    return random.choices(list(norm_weights.keys()), weights=norm_weights.values(), k=1)[0]

# --- LOAD STREET NETWORK ---

print("Loading Las Vegas metro area street network...")
places = [
    "Las Vegas, Nevada, USA", "North Las Vegas, Nevada, USA",
    "Henderson, Nevada, USA", "Summerlin South, Nevada, USA",
    "Paradise, Nevada, USA", "Enterprise, Nevada, USA",
    "Spring Valley, Nevada, USA", "Sunrise Manor, Nevada, USA"
]
G = ox.graph_from_place(places, network_type='drive')
edges = ox.graph_to_gdfs(G, nodes=False, edges=True)

coords = []
for geom in edges['geometry']:
    if geom.geom_type == 'LineString':
        coords.extend(list(geom.coords))
    elif geom.geom_type == 'MultiLineString':
        for line in geom.geoms:
            coords.extend(list(line.coords))

# --- GENERATE DATA ---

total_accidents = 100000
year_weights = [1, 1.1, 1.2, 1.3, 1.4, 1.5]
year_labels = list(range(2020, 2026))
normalized = [w / sum(year_weights) for w in year_weights]
year_distribution = dict(zip(year_labels, [int(w * total_accidents) for w in normalized]))

data = []
accident_counter = 1

print("Generating synthetic accident data...")
for year, count in year_distribution.items():
    for _ in range(int(count * 0.6)):  # Hotspot accidents
        hotspot_key = random.choice(list(hotspots.keys()))
        hotspot = hotspots[hotspot_key]
        if hotspot_key == "las_vegas_strip":
            lat, lon = sample_along_strip(hotspot["strip_centroids"])
        else:
            lat, lon = sample_near_hotspot(hotspot["coords"])
        timestamp, weather, month = generate_timestamp_and_weather(hotspot_key, year)
        acc_type = choose_accident_type(hotspot["accident_weights"], month, weather)
        age = random.randint(18, 80)
        gender = random.choice(genders)
        education = random.choice(education_levels)
        make, model = random.choice(car_makes_models)
        year_car = random.randint(2000, 2022)
        is_fault = random.choice([0, 1])
        if hotspot_key == "las_vegas_strip":
            is_dui = random.choices([0, 1], weights=[0.7, 0.3])[0]
        else:
            is_dui = random.choices([0, 1], weights=[0.85, 0.15])[0]
        payout = 0 if is_dui else max(0, round(np.random.normal(8000 if not is_fault else 3000, 2000), 2))

        data.append({
            "accident_id": accident_counter,
            "latitude": lat,
            "longitude": lon,
            "accident_type": acc_type,
            "driver_age": age,
            "driver_gender": gender,
            "driver_education": education,
            "car_make": make,
            "car_model": model,
            "car_year": year_car,
            "is_driver_at_fault": is_fault,
            "is_driver_dui": is_dui,
            "policy_payout": payout,
            "location_type": "hotspot",
            "timestamp": timestamp,
            "weather": weather
        })
        accident_counter += 1

    for _ in range(int(count * 0.4)):  # General accidents
        lon, lat = random.choice(coords)
        timestamp, weather, month = generate_timestamp_and_weather(None, year)
        acc_type = choose_accident_type(default_accident_weights, month, weather)
        age = random.randint(18, 80)
        gender = random.choice(genders)
        education = random.choice(education_levels)
        make, model = random.choice(car_makes_models)
        year_car = random.randint(2000, 2022)
        is_fault = random.choice([0, 1])
        is_dui = random.choices([0, 1], weights=[0.9, 0.1])[0]
        payout = 0 if is_dui else max(0, round(np.random.normal(8000 if not is_fault else 3000, 2000), 2))

        data.append({
            "accident_id": accident_counter,
            "latitude": lat,
            "longitude": lon,
            "accident_type": acc_type,
            "driver_age": age,
            "driver_gender": gender,
            "driver_education": education,
            "car_make": make,
            "car_model": model,
            "car_year": year_car,
            "is_driver_at_fault": is_fault,
            "is_driver_dui": is_dui,
            "policy_payout": payout,
            "location_type": "general",
            "timestamp": timestamp,
            "weather": weather
        })
        accident_counter += 1

# --- EXPORT TO CSV ---

print("Saving data to Sedona‑compatible CSV...")
df = pd.DataFrame(data)
df["wkt"] = df.apply(lambda row: f"POINT ({row['longitude']} {row['latitude']})", axis=1)
df.to_csv("las_vegas_accidents.csv", index=False)

# --- 2. DOWNLOAD STREET NETWORK (DRIVEABLE) ---

las_vegas_metro = [
    "Las Vegas, Nevada, USA",
    "Henderson, Nevada, USA",
    "North Las Vegas, Nevada, USA",
    "Paradise, Nevada, USA",
    "Spring Valley, Nevada, USA",
    "Enterprise, Nevada, USA"
]

print("Downloading street network...")
G = ox.graph_from_place(las_vegas_metro, network_type='drive')
edges = ox.graph_to_gdfs(G, nodes=False, edges=True)

# Prepare edges DataFrame
edges = edges[["highway", "name", "geometry"]].copy()
edges['wkt'] = edges['geometry'].apply(lambda geom: geom.wkt)
edges.drop(columns='geometry', inplace=True)

# Export
edges.to_csv("las_vegas_streets.csv", index=False)

# ---- 3. Casinos POI's

import osmnx as ox

# Define casino-related OSM tags
casino_tags = {
    "amenity": ["casino"],
    "leisure": ["adult_gaming_centre"],
    "tourism": ["attraction"]
}

las_vegas_metro = [
    "Las Vegas, Nevada, USA",
    "Henderson, Nevada, USA",
    "North Las Vegas, Nevada, USA",
    "Paradise, Nevada, USA",
    "Spring Valley, Nevada, USA",
    "Enterprise, Nevada, USA"
]

# Get POIs for all places
#casino_pois = ox.features_from_place(las_vegas_metro, tags=casino_tags)
casino_pois = ox.geometries_from_place(las_vegas_metro, tags=casino_tags)


# Optional: Filter further by name (for known casinos)
casino_pois = casino_pois[casino_pois['name'].str.contains("casino", case=False, na=False)]

# Preview
print(casino_pois[['name', 'amenity', 'leisure', 'tourism', 'geometry']].head())

casino_pois.to_csv("las_vegas_casino_pois.csv", index=False)
