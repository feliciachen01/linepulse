"""Shared constants for the simulated plant.

All numbers here are illustrative assumptions for a simulated flatbread line,
not FGF Brands data. Change them in one place and everything downstream follows.
"""

PLANT_ID = "SIM-PLANT-01"

LINES = {
    "L1": {"sku": "NAAN-GARLIC-2PK", "product": "Garlic naan, 2 pack"},
    "L2": {"sku": "NAAN-PLAIN-4PK", "product": "Plain naan, 4 pack"},
}

# Oven zone setpoints (deg C) and other process signals.
SETPOINTS = {
    "oven_z1_c": 300.0,
    "oven_z2_c": 330.0,
    "oven_z3_c": 310.0,
    "belt_speed_mpm": 6.0,
    "proofer_rh_pct": 82.0,
}
SENSOR_NOISE_SD = {
    "oven_z1_c": 1.5,
    "oven_z2_c": 1.5,
    "oven_z3_c": 1.5,
    "belt_speed_mpm": 0.05,
    "proofer_rh_pct": 0.8,
}
# Normal process wander (no fault): slow oscillation amplitude per signal.
# Real ovens and fillers hunt around setpoint; without this, any threshold looks perfect.
WANDER_AMPLITUDE = {
    "oven_z1_c": 2.5,
    "oven_z2_c": 2.5,
    "oven_z3_c": 2.5,
    "belt_speed_mpm": 0.05,
    "proofer_rh_pct": 1.0,
    "fill_weight_g": 0.45,
}
# Physical bounds used by data-quality rules. Outside = sensor fault, not process drift.
SENSOR_BOUNDS = {
    "oven_z1_c": (0.0, 600.0),
    "oven_z2_c": (0.0, 600.0),
    "oven_z3_c": (0.0, 600.0),
    "belt_speed_mpm": (0.0, 30.0),
    "proofer_rh_pct": (0.0, 100.0),
}
OVEN_SENSORS = ["oven_z1_c", "oven_z2_c", "oven_z3_c"]
TELEMETRY_SIGNALS = OVEN_SENSORS + ["belt_speed_mpm", "proofer_rh_pct"]

# How root causes show up in the simulated process (see simulator.py).
OVEN_HEAT_PER_MPM_SLOWDOWN = 5.0     # deg C every zone gains per m/min the belt runs slow (less dough load)
BAKE_DEFECT_PER_MPM_SLOWDOWN = 0.03  # extra bake-defect probability per m/min of slowdown (longer bake)
FAILING_SENSOR_NOISE_FACTOR = 2.5    # a drifting thermocouple also reads noisier

# Checkweigher: label weight is what the pack declares; target is where the filler aims.
LABEL_WEIGHT_G = 90.0
TARGET_WEIGHT_G = 92.0
WEIGHT_SD_G = 1.2
UNDERWEIGHT_REJECT_G = LABEL_WEIGHT_G * 0.98  # below this, the checkweigher kicks it off
UNIT_WEIGHT_BOUNDS = (20.0, 300.0)            # outside = bad reading

# Cost assumption used to price giveaway (dough + toppings, CAD per kg). Illustrative.
GIVEAWAY_COST_CAD_PER_KG = 2.50

TELEMETRY_INTERVAL_S = 10
UNITS_PER_MINUTE = 60
LOT_MINUTES = 30

# Detector thresholds: signal -> (alert when |EWMA deviation| >= threshold, units)
DETECTOR = {
    "alpha": 0.3,             # EWMA smoothing on per-minute means
    "min_consecutive": 3,     # minutes over threshold before raising an alert
    "thresholds": {
        "oven_z1_c": 5.0,
        "oven_z2_c": 5.0,
        "oven_z3_c": 5.0,
        "fill_weight_g": 1.0,  # deviation of mean accepted weight from TARGET_WEIGHT_G
        "belt_speed_mpm": 0.3,  # normal wander is about 0.05 m/min
        "proofer_rh_pct": 6.0,  # wide alarm band; mostly context for investigations
    },
}

SHIFTS = [("A", 6, 14), ("B", 14, 22), ("C", 22, 6)]
