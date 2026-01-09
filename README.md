
# Transit Performance Analytics Project

## Overview
This project analyzes courier shipment tracking data to evaluate transit efficiency,
facility performance, and delivery characteristics.

## Project Structure
transit_performance_project/
├── src/
│   └── transit_analysis.py
├── data/
│   └── Swift - Dataset (2).json
├── outputs/
│   ├── transit_performance_detailed.csv
│   └── transit_performance_summary.csv
└── README.md

## How to Run
1. Place the dataset JSON file in the data/ directory
2. Run:
   python src/transit_analysis.py

## Outputs
- Detailed shipment-level metrics CSV
- Network-level performance summary CSV

## Features
- Handles missing fields and timestamps
- Deduplicates events
- Computes transit time, facility metrics, and delivery performance
