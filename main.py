import streamlit as st
import json
import random
import boto3
from datetime import datetime

# Set the AWS region
AWS_REGION = "ap-south-1"

# Set up AWS clients
kinesis_client = boto3.client('kinesis', region_name=AWS_REGION)

# Streamlit app title
st.title('E-commerce Clickstream and Truck Telemetry Data')

# Section for Clickstream Data
st.header('Clickstream Data')
item_ids = ['ITEM001', 'ITEM002', 'ITEM003']
item_names = ['Mobile Phone', 'Laptop', 'Camera']

def generate_clickstream_data():
    data = []
    for i in range(3):
        item_data = {
            'item_id': item_ids[i],
            'item_name': item_names[i],
            'click_count': random.randint(1, 100)
        }
        data.append(item_data)
    return data

clickstream_data = generate_clickstream_data()
st.json(clickstream_data)

# Send clickstream data to Kinesis
for data in clickstream_data:
    kinesis_client.put_record(
        StreamName="clickstream_data_stream",
        Data=json.dumps(data),
        PartitionKey=data["item_id"]
    )

# Section for Truck Telemetry Data
st.header('Truck Telemetry Data')
truck_ids = ["TRK001", "TRK002", "TRK003"]

def generate_truck_data():
    data = []
    for truck_id in truck_ids:
        truck_data = {
            "truck_id": truck_id,
            "timestamp": datetime.utcnow().isoformat(),
            "gps_location": {
                "latitude": round(random.uniform(34.0, 41.0), 6),
                "longitude": round(random.uniform(-119.0, -73.0), 6),
                "altitude": round(random.uniform(0, 1000), 1),
                "speed": round(random.uniform(0, 100), 1)
            },
            "vehicle_speed": round(random.uniform(0, 100), 1),
            "engine_diagnostics": {
                "engine_rpm": round(random.uniform(1000, 4000), 1),
                "fuel_level": round(random.uniform(0, 100), 1),
                "temperature": round(random.uniform(70, 110), 1),
                "oil_pressure": round(random.uniform(20, 80), 1),
                "battery_voltage": round(random.uniform(12, 15), 1)
            },
            "odometer_reading": round(random.uniform(10000, 200000), 1),
            "fuel_consumption": round(random.uniform(5, 30), 1),
            "vehicle_health_and_maintenance": {
                "brake_status": random.choice(["Good", "Needs Inspection"]),
                "tire_pressure": {
                    "front_left": round(random.uniform(30, 40), 1),
                    "front_right": round(random.uniform(30, 40), 1),
                    "rear_left": round(random.uniform(30, 40), 1),
                    "rear_right": round(random.uniform(30, 40), 1)
                },
                "transmission_status": "Operational"
            },
            "environmental_conditions": {
                "temperature": round(random.uniform(-10, 40), 1),
                "humidity": round(random.uniform(10, 100), 1),
                "atmospheric_pressure": round(random.uniform(980, 1050), 2)
            }
        }
        data.append(truck_data)
    return data

truck_data = generate_truck_data()
st.json(truck_data)

# Send truck telemetry data to Kinesis
for data in truck_data:
    kinesis_client.put_record(
        StreamName="truck_telemetry_stream",
        Data=json.dumps(data),
        PartitionKey=data["truck_id"]
    )
