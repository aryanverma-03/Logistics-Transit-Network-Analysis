import json
import pandas as pd
from datetime import datetime
import dateutil.parser

# This function converts different timestamp formats into Python datetime
# Handles MongoDB epoch format and ISO datetime strings
def parse_timestamp(ts):
    if ts is None:
        return None
    if isinstance(ts, dict) and "$numberLong" in ts:
        return datetime.utcfromtimestamp(int(ts["$numberLong"]) / 1000)
    if isinstance(ts, str):
        return dateutil.parser.parse(ts)
    return None

# Helper function to classify whether a service is express or not
def is_express(service_type):
    if not service_type:
        return False
    return "EXPRESS" in service_type.upper() or "PRIORITY" in service_type.upper()

# Main function to generate detailed transit performance CSV
def generate_detailed_csv(input_json, output_csv):

    # Load JSON data from file
    with open(input_json, "r") as f:
        data = json.load(f)

    rows = []

    # Loop through each tracking response
    for record in data:
        for shipment in record.get("trackDetails", []):

            # Extract all events for the shipment
            events = shipment.get("events", []) or []

            # Convert events into a DataFrame for easier processing
            df = pd.DataFrame([{
                "event_type": e.get("eventType"),
                "event_time": parse_timestamp(e.get("timestamp")),
                "arrival_location": e.get("arrivalLocation"),
                "city": e.get("address", {}).get("city"),
                "state": e.get("address", {}).get("stateOrProvinceCode"),
                "postal": e.get("address", {}).get("postalCode")
            } for e in events])

            # Remove duplicate events (same event type and timestamp)
            df = df.drop_duplicates(
                subset=["event_type", "event_time", "arrival_location", "city"]
            )

            # Identify pickup and delivery timestamps
            pickup = df.loc[df["event_type"] == "PU", "event_time"].min()
            delivery = df.loc[df["event_type"] == "DL", "event_time"].max()

            # Calculate total transit time in hours
            total_hours = (
                (delivery - pickup).total_seconds() / 3600
                if pickup is not None and delivery is not None else None
            )

            # Filter events that happened at facilities
            facility_events = df[
                df["arrival_location"].fillna("").str.contains("FACILITY")
            ]

            # Calculate time spent moving between facilities
            inter_facility = (
                facility_events.sort_values("event_time")["event_time"]
                .diff().dt.total_seconds().sum() / 3600
                if facility_events.shape[0] > 1 else None
            )

            # Collect all shipment-level metrics
            rows.append({
                "tracking_number": shipment.get("trackingNumber"),
                "service_type": shipment.get("service", {}).get("type"),
                "carrier_code": shipment.get("carrierCode"),
                "package_weight_kg": shipment.get("packageWeight", {}).get("value"),
                "packaging_type": shipment.get("packaging", {}).get("type"),
                "origin_city": shipment.get("shipperAddress", {}).get("city"),
                "origin_state": shipment.get("shipperAddress", {}).get("stateOrProvinceCode"),
                "destination_city": shipment.get("destinationAddress", {}).get("city"),
                "destination_state": shipment.get("destinationAddress", {}).get("stateOrProvinceCode"),
                "pickup_datetime_ist": pickup,
                "delivery_datetime_ist": delivery,
                "total_transit_hours": total_hours,
                "num_facilities_visited": facility_events[
                    ["city", "state", "arrival_location"]
                ].drop_duplicates().shape[0],
                "num_in_transit_events": df[df["event_type"] == "IT"].shape[0],
                "time_in_inter_facility_transit_hours": inter_facility,
                "avg_hours_per_facility": (
                    total_hours / facility_events.shape[0]
                    if total_hours and facility_events.shape[0] > 0 else None
                ),
                "is_express_service": is_express(
                    shipment.get("service", {}).get("type")
                ),
                "delivery_location_type": shipment.get("deliveryLocationType"),
                "num_out_for_delivery_attempts": df[df["event_type"] == "OD"].shape[0],
                "first_attempt_delivery": df[df["event_type"] == "OD"].shape[0] == 1,
                "total_events_count": df.shape[0]
            })

    # Write final output to CSV
    pd.DataFrame(rows).to_csv(output_csv, index=False)
