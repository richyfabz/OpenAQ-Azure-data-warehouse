TARGET_LOCATIONS = {
    17: "R K Puram, Delhi - DPCC",
    2669: "München/Stachus",
    3549: "St-Laurent-Blangy",
    225464: "Karoo-NAQI"
}  # Final monitoring locations selected during source exploration


AIR_QUALITY_SENSOR_IDS = [
    12234786,  # India PM10
    12234787,  # India PM2.5
    6686,      # Germany PM10
    6688,      # Germany PM2.5
    7951,      # France PM10
    9014237,   # France PM2.5
    1303615,   # South Africa PM10
    1303537    # South Africa PM2.5
]  # Comparable PM2.5/PM10 sensors use µg/m³ across all four locations


ENVIRONMENT_SENSOR_IDS = [
    12234788,  # Relative humidity
    12234790,  # Temperature
    14340714,  # Wind direction
    14340715  # Wind speed
]  # R K Puram provides the environmental-analysis branch


TARGET_SENSOR_IDS = (AIR_QUALITY_SENSOR_IDS + ENVIRONMENT_SENSOR_IDS)