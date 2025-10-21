# Car Insurance Modeling in Cloudera AI

This project is WIP.

## Objective

Showcase how to build a car insurance risk and pricing model based on Las Vegas car accident data using Sedona Geospatial, Spark and MLFlow in CAI.

## Requirements

* Python 3.13 runtime

## High Level Steps

1. Create synthetic data simulating car accidents in Las Vegas. Data includes accident location, customer PII, vehicle information and associated fiancial cost.

2. Data Exploration: with Spark and Sedona, look for accident hotspots (clusters) and explore potential correlations with vehicles and customer PII.

3. Assign financial insurance cost to accidents based on types and gravity.

4. Feature engineering: Calculate distance between each policy holder location and accident cluster. Assign increased or decreased risk score and financial cost to drivers.

5. Modeling: train regression model to predict risk and financial cost to each driver in dataset.  

6. Customer Lifetime Value Analysis: project customer lifetime value into the futuer.
