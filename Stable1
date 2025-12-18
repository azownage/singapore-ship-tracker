"""
Singapore AIS Tracker with S&P Maritime Risk Intelligence
Real-time vessel tracking with compliance and risk indicators
Enhanced with persistent storage, vessel polygons, and maritime zones
"""

import streamlit as st
import asyncio
import websockets
import json
from datetime import datetime, timezone
import pandas as pd
import pydeck as pdk
from collections import defaultdict
import time
import requests
from urllib.parse import quote
from typing import List, Dict, Any, Optional, Tuple
import pickle
import os
import math

st.set_page_config(
    page_title="Singapore Ship Risk Tracker",
    page_icon="🚢",
    layout="wide"
)

# File-based persistent storage
STORAGE_FILE = "ship_data_cache.pkl"
RISK_DATA_FILE = "risk_data_cache.pkl"

# AIS vessel type codes
VESSEL_TYPE_NAMES = {
    0: "Not available",
    20: "Wing in ground",
    21: "Wing in ground - Hazardous A",
    22: "Wing in ground - Hazardous B",
    23: "Wing in ground - Hazardous C",
    24: "Wing in ground - Hazardous D",
    30: "Fishing",
    31: "Towing",
    32: "Towing - Large",
    33: "Dredging",
    34: "Diving ops",
    35: "Military ops",
    36: "Sailing",
    37: "Pleasure Craft",
    40: "High Speed Craft",
    41: "High Speed Craft - Hazardous A",
    42: "High Speed Craft - Hazardous B",
    43: "High Speed Craft - Hazardous C",
    44: "High Speed Craft - Hazardous D",
    50: "Pilot Vessel",
    51: "Search and Rescue",
    52: "Tug",
    53: "Port Tender",
    54: "Anti-pollution",
    55: "Law Enforcement",
    56: "Spare Local 1",
    57: "Spare Local 2",
    58: "Medical Transport",
    59: "Noncombatant",
    60: "Passenger",
    61: "Passenger - Hazardous A",
    62: "Passenger - Hazardous B",
    63: "Passenger - Hazardous C",
    64: "Passenger - Hazardous D",
    65: "Passenger - Reserved",
    66: "Passenger - Reserved",
    67: "Passenger - Reserved",
    68: "Passenger - Reserved",
    69: "Passenger - No info",
    70: "Cargo",
    71: "Cargo - Hazardous A",
    72: "Cargo - Hazardous B",
    73: "Cargo - Hazardous C",
    74: "Cargo - Hazardous D",
    75: "Cargo - Reserved",
    76: "Cargo - Reserved",
    77: "Cargo - Reserved",
    78: "Cargo - Reserved",
    79: "Cargo - No info",
    80: "Tanker",
    81: "Tanker - Hazardous A",
    82: "Tanker - Hazardous B",
    83: "Tanker - Hazardous C",
    84: "Tanker - Hazardous D",
    85: "Tanker - Reserved",
    86: "Tanker - Reserved",
    87: "Tanker - Reserved",
    88: "Tanker - Reserved",
    89: "Tanker - No info",
    90: "Other",
    91: "Other - Hazardous A",
    92: "Other - Hazardous B",
    93: "Other - Hazardous C",
    94: "Other - Hazardous D",
    95: "Other - Reserved",
    96: "Other - Reserved",
    97: "Other - Reserved",
    98: "Other - Reserved",
    99: "Other - No info",
}

# Navigation status codes
NAV_STATUS_NAMES = {
    0: "Under way using engine",
    1: "At anchor",
    2: "Not under command",
    3: "Restricted maneuverability",
    4: "Constrained by draught",
    5: "Moored",
    6: "Aground",
    7: "Engaged in fishing",
    8: "Under way sailing",
    9: "Reserved for HSC",
    10: "Reserved for WIG",
    11: "Reserved",
    12: "Reserved",
    13: "Reserved",
    14: "AIS-SART",
    15: "Not defined",
}


def get_vessel_type_category(type_code: int) -> str:
    """Get vessel type category from AIS type code"""
    if type_code is None:
        return "Unknown"
    if 70 <= type_code <= 79:
        return "Cargo"
    elif 80 <= type_code <= 89:
        return "Tanker"
    elif 60 <= type_code <= 69:
        return "Passenger"
    elif type_code in [31, 32, 52]:
        return "Tug"
    elif type_code == 30:
        return "Fishing"
    elif 40 <= type_code <= 49:
        return "High Speed Craft"
    elif type_code == 50:
        return "Pilot"
    elif type_code == 51:
        return "SAR"
    elif type_code == 53:
        return "Port Tender"
    elif type_code == 55:
        return "Law Enforcement"
    elif 90 <= type_code <= 99:
        return "Other"
    else:
        return "Other"


def load_cache() -> Tuple[Dict, Dict]:
    """Load cached ship and risk data from disk"""
    ship_cache = {}
    risk_cache = {}
    
    if os.path.exists(STORAGE_FILE):
        try:
            with open(STORAGE_FILE, 'rb') as f:
                ship_cache = pickle.load(f)
        except Exception:
            pass
    
    if os.path.exists(RISK_DATA_FILE):
        try:
            with open(RISK_DATA_FILE, 'rb') as f:
                risk_cache = pickle.load(f)
        except Exception:
            pass
    
    return ship_cache, risk_cache


def save_cache(ship_cache: Dict, risk_cache: Dict):
    """Save ship and risk data to disk"""
    try:
        with open(STORAGE_FILE, 'wb') as f:
            pickle.dump(ship_cache, f)
        with open(RISK_DATA_FILE, 'wb') as f:
            pickle.dump(risk_cache, f)
    except Exception as e:
        st.warning(f"Could not save cache: {e}")


def load_maritime_zones(excel_path: str) -> Dict[str, List[Dict]]:
    """Load maritime zones from Excel file"""
    zones = {"Anchorages": [], "Channels": [], "Fairways": []}
    
    try:
        sheets = pd.read_excel(excel_path, sheet_name=None)
        
        for sheet_name in ["Anchorages", "Channels", "Fairways"]:
            if sheet_name not in sheets:
                continue
                
            df = sheets[sheet_name]
            
            # Get the name column (first column with 'Name' in it)
            name_col = None
            for col in df.columns:
                if 'Name' in col:
                    name_col = col
                    break
            
            if name_col is None:
                continue
            
            # Group by zone name to create polygons
            for zone_name in df[name_col].unique():
                zone_df = df[df[name_col] == zone_name]
                
                if 'Decimal Latitude' in zone_df.columns and 'Decimal Longitude' in zone_df.columns:
                    coords = []
                    for _, row in zone_df.iterrows():
                        lat = row['Decimal Latitude']
                        lon = row['Decimal Longitude']
                        if pd.notna(lat) and pd.notna(lon):
                            coords.append([float(lon), float(lat)])
                    
                    if len(coords) >= 3:
                        # Close the polygon if needed
                        if coords[0] != coords[-1]:
                            coords.append(coords[0])
                        
                        zones[sheet_name].append({
                            "name": zone_name,
                            "polygon": coords
                        })
        
        return zones
    except Exception as e:
        st.warning(f"Could not load maritime zones: {e}")
        return zones


def create_vessel_polygon(lat: float, lon: float, heading: float, 
                         length: float = 60, width: float = 16) -> List[List[float]]:
    """
    Create a vessel-shaped polygon (rectangle) based on position, heading, and dimensions.
    Uses vessel dimensions A, B, C, D if available, otherwise defaults.
    """
    # Convert heading to radians (heading 0 = North)
    heading_rad = math.radians(heading if heading and heading < 360 else 0)
    
    # Default dimensions if not provided
    if length <= 0:
        length = 60
    if width <= 0:
        width = 16
    
    # Scale factor for geographic coordinates (approximate meters to degrees)
    # At Singapore latitude (~1.3°N), 1 degree latitude ≈ 111,000 meters
    # 1 degree longitude ≈ 111,000 * cos(1.3°) ≈ 110,700 meters
    lat_scale = 1 / 111000
    lon_scale = 1 / (111000 * math.cos(math.radians(lat)))
    
    # Scale up for visibility (multiply by factor based on zoom level)
    visibility_scale = 3  # Increase size for visibility
    
    half_length = (length * visibility_scale / 2) * lat_scale
    half_width = (width * visibility_scale / 2) * lon_scale
    
    # Define corners relative to center (bow at top)
    # Rectangle corners: front-left, front-right, back-right, back-left
    corners = [
        (-half_width, half_length),   # Front left
        (half_width, half_length),    # Front right
        (half_width, -half_length),   # Back right
        (-half_width, -half_length),  # Back left
    ]
    
    # Rotate corners by heading
    cos_h = math.cos(heading_rad)
    sin_h = math.sin(heading_rad)
    
    rotated_corners = []
    for dx, dy in corners:
        # Rotate point
        new_dx = dx * cos_h - dy * sin_h
        new_dy = dx * sin_h + dy * cos_h
        
        # Translate to actual position
        new_lon = lon + new_dx / lon_scale * lat_scale
        new_lat = lat + new_dy
        
        rotated_corners.append([new_lon, new_lat])
    
    # Close the polygon
    rotated_corners.append(rotated_corners[0])
    
    return rotated_corners


# Initialize session state for persistent storage
if 'ship_static_cache' not in st.session_state:
    ship_cache, risk_cache = load_cache()
    st.session_state.ship_static_cache = ship_cache
    st.session_state.risk_data_cache = risk_cache
    st.session_state.last_save = time.time()

if 'selected_vessel' not in st.session_state:
    st.session_state.selected_vessel = None

if 'map_center' not in st.session_state:
    st.session_state.map_center = {"lat": 1.27, "lon": 103.85, "zoom": 11}


class SPMaritimeAPI:
    """S&P Maritime API Integration for compliance screening"""
    
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_url = "https://webservices.maritime.spglobal.com/RiskAndCompliance/CompliancesByImos"
    
    def get_ship_compliance_data(self, imo_numbers: List[str]) -> Dict[str, Dict]:
        """Get compliance indicators for multiple IMO numbers (with caching)"""
        if not imo_numbers:
            return {}
        
        cache = st.session_state.risk_data_cache
        uncached_imos = [imo for imo in imo_numbers if imo not in cache]
        
        if not uncached_imos:
            return {imo: cache[imo] for imo in imo_numbers}
        
        st.info(f"🔍 Fetching compliance data for {len(uncached_imos)} vessels from S&P API...")
        
        try:
            # Batch API call (max 100 per request)
            batches = [uncached_imos[i:i+100] for i in range(0, len(uncached_imos), 100)]
            
            for batch in batches:
                imo_string = ','.join(batch)
                url = f"{self.base_url}?imos={imo_string}"
                
                response = requests.get(
                    url,
                    auth=(self.username, self.password),
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if isinstance(data, list):
                        for ship in data:
                            imo = str(ship.get('lrno', ''))
                            if not imo:
                                continue
                            
                            cache[imo] = {
                                'legal_overall': ship.get('shipLegalOverall', 0),
                                'ship_bes_sanction': ship.get('shipBESSanctionList', 0),
                                'ship_eu_sanction': ship.get('shipEUSanctionList', 0),
                                'ship_ofac_sanction': ship.get('shipOFACSanctionList', 0),
                                'ship_swiss_sanction': ship.get('shipSwissSanctionList', 0),
                                'ship_un_sanction': ship.get('shipUNSanctionList', 0),
                                'dark_activity': ship.get('shipDarkActivityIndicator', 0),
                                'flag_disputed': ship.get('shipFlagDisputed', 0),
                                'port_call_3m': ship.get('shipSanctionedCountryPortCallLast3m', 0),
                                'port_call_6m': ship.get('shipSanctionedCountryPortCallLast6m', 0),
                                'port_call_12m': ship.get('shipSanctionedCountryPortCallLast12m', 0),
                                'owner_ofac': ship.get('shipOwnerOFACSanctionList', 0),
                                'owner_un': ship.get('shipOwnerUNSanctionList', 0),
                                'cached_at': datetime.now().isoformat()
                            }
                
                time.sleep(0.5)  # Rate limiting
            
            st.session_state.risk_data_cache = cache
            save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache)
            st.success(f"✅ Cached compliance data for {len(uncached_imos)} vessels")
                
        except Exception as e:
            st.error(f"⚠️ S&P API error: {str(e)}")
        
        return {imo: cache.get(imo, {}) for imo in imo_numbers}


class AISTracker:
    """AIS data collection and vessel tracking"""
    
    def __init__(self):
        self.ships = defaultdict(lambda: {
            'latest_position': None,
            'static_data': None
        })
    
    def get_ship_color(self, legal_overall: int = 0) -> List[int]:
        """Return color based on legal overall compliance status"""
        if legal_overall == 2:
            return [220, 53, 69, 200]  # Red - Severe
        elif legal_overall == 1:
            return [255, 193, 7, 200]  # Orange/Yellow - Warning
        else:
            return [40, 167, 69, 200]  # Green - Clear
    
    async def collect_data(self, duration: int = 30, api_key: str = "", bounding_box: List = None):
        """Collect AIS data from AISStream.io"""
        if bounding_box is None:
            bounding_box = [[[1.15, 103.55], [1.50, 104.10]]]  # Full Singapore waters
        
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
                subscription = {
                    "APIKey": api_key,
                    "BoundingBoxes": bounding_box,
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                }
                
                await ws.send(json.dumps(subscription))
                start_time = time.time()
                
                async for message_json in ws:
                    if time.time() - start_time > duration:
                        break
                    
                    ais_message = json.loads(message_json)
                    message_type = ais_message.get("MessageType")
                    
                    if message_type == "PositionReport":
                        self.process_position(ais_message)
                    elif message_type == "ShipStaticData":
                        self.process_static(ais_message)
        except Exception as e:
            st.error(f"AIS connection error: {e}")
    
    def process_position(self, ais_message: Dict):
        """Process AIS position report"""
        metadata = ais_message.get('MetaData', {})
        position_data = ais_message.get('Message', {}).get('PositionReport', {})
        
        mmsi = position_data.get('UserID')
        if not mmsi:
            return
        
        self.ships[mmsi]['latest_position'] = {
            'latitude': position_data.get('Latitude'),
            'longitude': position_data.get('Longitude'),
            'sog': position_data.get('Sog', 0),
            'cog': position_data.get('Cog', 0),
            'true_heading': position_data.get('TrueHeading', 511),
            'nav_status': position_data.get('NavigationalStatus', 15),
            'ship_name': metadata.get('ShipName', 'Unknown'),
            'timestamp': datetime.now().isoformat()
        }
    
    def process_static(self, ais_message: Dict):
        """Process AIS static data report"""
        static_data = ais_message.get('Message', {}).get('ShipStaticData', {})
        
        mmsi = static_data.get('UserID')
        if not mmsi:
            return
        
        dimension = static_data.get('Dimension', {})
        imo = str(static_data.get('ImoNumber', 0))
        
        dim_a = dimension.get('A', 0) or 0
        dim_b = dimension.get('B', 0) or 0
        dim_c = dimension.get('C', 0) or 0
        dim_d = dimension.get('D', 0) or 0
        
        static_info = {
            'name': static_data.get('Name', 'Unknown'),
            'imo': imo,
            'type': static_data.get('Type'),
            'dimension_a': dim_a,
            'dimension_b': dim_b,
            'dimension_c': dim_c,
            'dimension_d': dim_d,
            'length': dim_a + dim_b,
            'width': dim_c + dim_d,
            'destination': static_data.get('Destination', 'Unknown'),
            'call_sign': static_data.get('CallSign', ''),
            'cached_at': datetime.now().isoformat()
        }
        
        self.ships[mmsi]['static_data'] = static_info
        st.session_state.ship_static_cache[str(mmsi)] = static_info
        
        if time.time() - st.session_state.last_save > 60:
            save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache)
            st.session_state.last_save = time.time()
    
    def get_dataframe_with_compliance(self, sp_api: Optional[SPMaritimeAPI] = None) -> pd.DataFrame:
        """Get dataframe with compliance indicators"""
        data = []
        
        for mmsi, ship_data in self.ships.items():
            pos = ship_data.get('latest_position')
            
            static = ship_data.get('static_data')
            if not static:
                static = st.session_state.ship_static_cache.get(str(mmsi), {})
            
            if not pos or pos.get('latitude') is None or pos.get('longitude') is None:
                continue
            
            name = static.get('name') or pos.get('ship_name') or 'Unknown'
            name = name.strip() if name else 'Unknown'
            
            ship_type = static.get('type')
            imo = str(static.get('imo', '0'))
            
            # Get heading (use true_heading, fall back to COG)
            true_heading = pos.get('true_heading', 511)
            if true_heading == 511:  # 511 = not available
                heading = pos.get('cog', 0)
            else:
                heading = true_heading
            
            # Get dimensions
            dim_a = static.get('dimension_a', 0) or 0
            dim_b = static.get('dimension_b', 0) or 0
            dim_c = static.get('dimension_c', 0) or 0
            dim_d = static.get('dimension_d', 0) or 0
            length = dim_a + dim_b
            width = dim_c + dim_d
            
            data.append({
                'mmsi': mmsi,
                'name': name,
                'imo': imo,
                'latitude': pos.get('latitude'),
                'longitude': pos.get('longitude'),
                'speed': pos.get('sog', 0),
                'course': pos.get('cog', 0),
                'heading': heading,
                'nav_status': pos.get('nav_status', 15),
                'nav_status_name': NAV_STATUS_NAMES.get(pos.get('nav_status', 15), 'Unknown'),
                'type': ship_type,
                'type_name': get_vessel_type_category(ship_type),
                'length': length,
                'width': width,
                'has_dimensions': (dim_a > 0 or dim_b > 0),
                'destination': (static.get('destination') or 'Unknown').strip(),
                'call_sign': static.get('call_sign', ''),
                'has_static': bool(static.get('name')),
                'legal_overall': 0,
                'un_sanction': 0,
                'ofac_sanction': 0,
                'dark_activity': 0,
                'color': self.get_ship_color(0)
            })
        
        df = pd.DataFrame(data)
        
        if len(df) == 0:
            return df
        
        # Get IMO numbers for compliance checking
        valid_imos = [str(imo) for imo in df['imo'].unique() if imo and imo != '0']
        
        if valid_imos and sp_api:
            compliance_data = sp_api.get_ship_compliance_data(valid_imos)
            
            for idx, row in df.iterrows():
                imo = str(row['imo'])
                if imo in compliance_data and compliance_data[imo]:
                    comp = compliance_data[imo]
                    df.at[idx, 'legal_overall'] = comp.get('legal_overall', 0)
                    df.at[idx, 'un_sanction'] = comp.get('ship_un_sanction', 0)
                    df.at[idx, 'ofac_sanction'] = comp.get('ship_ofac_sanction', 0)
                    df.at[idx, 'dark_activity'] = comp.get('dark_activity', 0)
                    df.at[idx, 'color'] = self.get_ship_color(comp.get('legal_overall', 0))
        
        return df


def create_vessel_layer(df: pd.DataFrame) -> pdk.Layer:
    """Create PyDeck polygon layer for vessel rectangles"""
    if len(df) == 0:
        return None
    
    vessel_polygons = []
    
    for _, row in df.iterrows():
        polygon = create_vessel_polygon(
            lat=row['latitude'],
            lon=row['longitude'],
            heading=row['heading'],
            length=row['length'] if row['length'] > 0 else 60,
            width=row['width'] if row['width'] > 0 else 16
        )
        
        vessel_polygons.append({
            'polygon': polygon,
            'name': row['name'],
            'imo': row['imo'],
            'mmsi': row['mmsi'],
            'speed': row['speed'],
            'heading': row['heading'],
            'nav_status': row['nav_status_name'],
            'type': row['type_name'],
            'legal_overall': row['legal_overall'],
            'destination': row['destination'],
            'color': row['color']
        })
    
    return pdk.Layer(
        'PolygonLayer',
        data=vessel_polygons,
        get_polygon='polygon',
        get_fill_color='color',
        get_line_color=[0, 0, 0, 0],  # No border
        pickable=True,
        auto_highlight=True,
        extruded=False,
    )


def create_zone_layer(zones: List[Dict], color: List[int], layer_id: str) -> pdk.Layer:
    """Create PyDeck polygon layer for maritime zones"""
    if not zones:
        return None
    
    zone_data = []
    for zone in zones:
        zone_data.append({
            'polygon': zone['polygon'],
            'name': zone['name'],
            'color': color
        })
    
    return pdk.Layer(
        'PolygonLayer',
        data=zone_data,
        id=layer_id,
        get_polygon='polygon',
        get_fill_color='color',
        get_line_color=[100, 100, 100, 100],
        line_width_min_pixels=1,
        pickable=True,
        auto_highlight=True,
        extruded=False,
    )


# ============= STREAMLIT UI =============

st.title("🚢 Singapore Ship Risk Tracker")
st.markdown("Real-time vessel tracking with S&P Maritime compliance screening")

# Sidebar Configuration
st.sidebar.header("⚙️ Configuration")

# Try to load credentials from secrets
try:
    sp_username = st.secrets["sp_maritime"]["username"]
    sp_password = st.secrets["sp_maritime"]["password"]
    ais_api_key = st.secrets.get("aisstream", {}).get("api_key", "")
    st.sidebar.success("🔐 Using credentials from secrets")
except Exception:
    with st.sidebar.expander("🔐 API Credentials", expanded=False):
        st.warning("⚠️ Credentials should be in Streamlit Secrets")
        sp_username = st.text_input("S&P Username", type="password")
        sp_password = st.text_input("S&P Password", type="password")
        ais_api_key = st.text_input("AISStream API Key", type="password")

# AIS Settings
duration = st.sidebar.slider("AIS collection time (seconds)", 10, 120, 30)
enable_compliance = st.sidebar.checkbox("Enable S&P compliance screening", value=True)

# Maritime Zones
st.sidebar.header("🗺️ Maritime Zones")
show_anchorages = st.sidebar.checkbox("Show Anchorages", value=False)
show_channels = st.sidebar.checkbox("Show Channels", value=False)
show_fairways = st.sidebar.checkbox("Show Fairways", value=False)

# Load maritime zones if any are enabled
maritime_zones = {"Anchorages": [], "Channels": [], "Fairways": []}
excel_paths = [
    "/mnt/project/Anchorages_Channels_Fairways_Details.xlsx",
    "/mnt/user-data/uploads/Anchorages_Channels_Fairways_Details.xlsx",
    "Anchorages_Channels_Fairways_Details.xlsx"
]

if show_anchorages or show_channels or show_fairways:
    for path in excel_paths:
        if os.path.exists(path):
            maritime_zones = load_maritime_zones(path)
            break

# Filters
st.sidebar.header("🔍 Filters")

# Compliance filters
st.sidebar.subheader("Compliance")
show_severe_only = st.sidebar.checkbox("Severe only (🔴)", value=False)
show_warning_plus = st.sidebar.checkbox("Warning+ (🟡 & 🔴)", value=False)
show_un_ofac = st.sidebar.checkbox("UN/OFAC sanctions only", value=False)

# Vessel type filter
st.sidebar.subheader("Vessel Type")
vessel_types = ["All", "Cargo", "Tanker", "Passenger", "Tug", "Fishing", 
                "High Speed Craft", "Pilot", "SAR", "Port Tender", "Law Enforcement", "Other", "Unknown"]
selected_type = st.sidebar.selectbox("Type", vessel_types)

# Navigation status filter
st.sidebar.subheader("Navigation Status")
nav_statuses = ["All"] + list(NAV_STATUS_NAMES.values())
selected_nav = st.sidebar.selectbox("Status", nav_statuses)

# Static data filter
show_static_only = st.sidebar.checkbox("Ships with static data only", value=False)

# Cache statistics
st.sidebar.header("💾 Cache Statistics")
st.sidebar.info(f"""
**Static Data Cache:** {len(st.session_state.ship_static_cache)} vessels
**Compliance Cache:** {len(st.session_state.risk_data_cache)} vessels
""")

if st.sidebar.button("🗑️ Clear All Cache"):
    st.session_state.ship_static_cache = {}
    st.session_state.risk_data_cache = {}
    save_cache({}, {})
    st.sidebar.success("Cache cleared!")
    st.rerun()

# Main content placeholders
status_placeholder = st.empty()
stats_placeholder = st.empty()
map_placeholder = st.empty()
table_header = st.empty()
table_placeholder = st.empty()


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all filters to the dataframe"""
    if len(df) == 0:
        return df
    
    filtered_df = df.copy()
    
    # Compliance filters
    if show_severe_only:
        filtered_df = filtered_df[filtered_df['legal_overall'] == 2]
    elif show_warning_plus:
        filtered_df = filtered_df[filtered_df['legal_overall'] >= 1]
    
    if show_un_ofac:
        filtered_df = filtered_df[(filtered_df['un_sanction'] == 2) | (filtered_df['ofac_sanction'] == 2)]
    
    # Vessel type filter
    if selected_type != "All":
        filtered_df = filtered_df[filtered_df['type_name'] == selected_type]
    
    # Navigation status filter
    if selected_nav != "All":
        filtered_df = filtered_df[filtered_df['nav_status_name'] == selected_nav]
    
    # Static data filter
    if show_static_only:
        filtered_df = filtered_df[filtered_df['has_static'] == True]
    
    return filtered_df


def format_compliance_value(val: int) -> str:
    """Format compliance values with emoji"""
    if val == 2:
        return "🔴"
    elif val == 1:
        return "🟡"
    else:
        return "✅"


def update_display():
    """Main function to collect data and update display"""
    
    # Initialize API
    sp_api = None
    if enable_compliance and sp_username and sp_password:
        sp_api = SPMaritimeAPI(sp_username, sp_password)
    
    # Collect AIS data
    with status_placeholder:
        with st.spinner(f'🔄 Collecting AIS data for {duration} seconds...'):
            tracker = AISTracker()
            if ais_api_key:
                asyncio.run(tracker.collect_data(duration, ais_api_key))
            else:
                st.warning("⚠️ No AISStream API key provided. Please add it to secrets.")
                return
            
            df = tracker.get_dataframe_with_compliance(sp_api)
    
    status_placeholder.empty()
    
    if df.empty:
        st.warning("⚠️ No ships detected. Try increasing collection time or check API key.")
        return
    
    # Store full dataframe before filtering
    full_df = df.copy()
    
    # Apply filters
    df = apply_filters(df)
    
    if df.empty:
        st.info("ℹ️ No ships match the selected filters.")
        return
    
    # Display statistics
    with stats_placeholder:
        cols = st.columns(7)
        cols[0].metric("🚢 Total Ships", len(df))
        cols[1].metric("⚡ Moving", len(df[df['speed'] > 1]))
        cols[2].metric("📡 Has Static", int(df['has_static'].sum()))
        cols[3].metric("📐 Real Dims", int(df['has_dimensions'].sum()))
        
        severe_count = len(df[df['legal_overall'] == 2])
        warning_count = len(df[df['legal_overall'] == 1])
        clear_count = len(df[df['legal_overall'] == 0])
        
        cols[4].metric("🔴 Severe", severe_count)
        cols[5].metric("🟡 Warning", warning_count)
        cols[6].metric("✅ Clear", clear_count)
    
    # Determine map view
    if st.session_state.selected_vessel:
        vessel = df[df['mmsi'] == st.session_state.selected_vessel]
        if len(vessel) > 0:
            center_lat = vessel.iloc[0]['latitude']
            center_lon = vessel.iloc[0]['longitude']
            zoom = 15
        else:
            center_lat = st.session_state.map_center['lat']
            center_lon = st.session_state.map_center['lon']
            zoom = st.session_state.map_center['zoom']
    else:
        center_lat = 1.27
        center_lon = 103.85
        zoom = 11
    
    # Create map layers
    layers = []
    
    # Add maritime zone layers (underneath vessels)
    if show_anchorages and maritime_zones['Anchorages']:
        anchorage_layer = create_zone_layer(
            maritime_zones['Anchorages'], 
            [0, 255, 255, 50],  # Cyan with 80% transparency
            "anchorages"
        )
        if anchorage_layer:
            layers.append(anchorage_layer)
    
    if show_channels and maritime_zones['Channels']:
        channel_layer = create_zone_layer(
            maritime_zones['Channels'], 
            [255, 255, 0, 50],  # Yellow with 80% transparency
            "channels"
        )
        if channel_layer:
            layers.append(channel_layer)
    
    if show_fairways and maritime_zones['Fairways']:
        fairway_layer = create_zone_layer(
            maritime_zones['Fairways'], 
            [255, 165, 0, 50],  # Orange with 80% transparency
            "fairways"
        )
        if fairway_layer:
            layers.append(fairway_layer)
    
    # Add vessel layer (on top)
    vessel_layer = create_vessel_layer(df)
    if vessel_layer:
        layers.append(vessel_layer)
    
    # Create map
    with map_placeholder:
        view_state = pdk.ViewState(
            latitude=center_lat,
            longitude=center_lon,
            zoom=zoom,
            pitch=0,
        )
        
        # Use default open street map tiles (light theme, no token needed)
        deck = pdk.Deck(
            map_style='',  # Empty = use default open tiles
            initial_view_state=view_state,
            layers=layers,
            tooltip={
                'html': '''
                    <b>{name}</b><br/>
                    IMO: {imo}<br/>
                    Type: {type}<br/>
                    Status: {nav_status}<br/>
                    Speed: {speed} kts<br/>
                    Heading: {heading}°<br/>
                    Legal: {legal_overall}<br/>
                    Destination: {destination}
                ''',
                'style': {'backgroundColor': 'steelblue', 'color': 'white'}
            }
        )
        
        st.pydeck_chart(deck, use_container_width=True)
    
    # Vessel table with View buttons
    with table_header:
        col1, col2 = st.columns([6, 1])
        col1.subheader("📋 Vessel Details")
        if col2.button("🔄 Reset View"):
            st.session_state.selected_vessel = None
            st.session_state.map_center = {"lat": 1.27, "lon": 103.85, "zoom": 11}
            st.rerun()
    
    with table_placeholder:
        # Sort by legal_overall (most severe first) then by speed
        display_df = df.sort_values(['legal_overall', 'speed'], ascending=[False, False]).copy()
        
        # Create a scrollable container with fixed height
        table_container = st.container(height=500)
        
        with table_container:
            # Header row
            header_cols = st.columns([3, 2, 2.5, 1.2, 0.8, 0.8, 0.8, 1])
            header_cols[0].markdown("**Name**")
            header_cols[1].markdown("**Type**")
            header_cols[2].markdown("**Nav Status**")
            header_cols[3].markdown("**Speed**")
            header_cols[4].markdown("**Legal**")
            header_cols[5].markdown("**UN**")
            header_cols[6].markdown("**OFAC**")
            header_cols[7].markdown("**View**")
            
            st.divider()
            
            for idx, row in display_df.iterrows():
                cols = st.columns([3, 2, 2.5, 1.2, 0.8, 0.8, 0.8, 1])
                
                cols[0].write(row['name'])
                cols[1].write(row['type_name'])
                cols[2].write(row['nav_status_name'])
                cols[3].write(f"{row['speed']:.1f} kts")
                cols[4].write(format_compliance_value(row['legal_overall']))
                cols[5].write(format_compliance_value(row['un_sanction']))
                cols[6].write(format_compliance_value(row['ofac_sanction']))
                
                if cols[7].button("🗺️", key=f"view_{row['mmsi']}"):
                    st.session_state.selected_vessel = row['mmsi']
                    st.rerun()
    
    # Save cache
    save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache)
    
    st.success(f"✅ Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# Control buttons
col1, col2 = st.columns([1, 5])
if col1.button("🔄 Refresh Now", type="primary"):
    update_display()

# Auto-refresh option
auto_refresh = st.sidebar.checkbox("Auto-refresh every 60s", value=False)
if auto_refresh:
    update_display()
    time.sleep(60)
    st.rerun()

# Initial load button hint
if 'data_loaded' not in st.session_state:
    st.info("👆 Click 'Refresh Now' to start collecting AIS data")
    st.session_state.data_loaded = False

# Legend
st.sidebar.markdown("---")
st.sidebar.markdown("### 🎨 Color Legend")
st.sidebar.markdown("""
- 🔴 **Red**: Severe compliance issue
- 🟡 **Yellow/Orange**: Warning
- 🟢 **Green**: Clear
""")

st.sidebar.markdown("### 🚨 Compliance Indicators")
st.sidebar.markdown("""
- **Legal Overall**: 0=Clear, 1=Warning, 2=Severe
- **UN**: UN Security Council sanctions
- **OFAC**: US Treasury OFAC SDN list
- **Dark**: Suspected dark activity
""")

st.sidebar.markdown("### 🗺️ Zone Colors")
st.sidebar.markdown("""
- 🔵 **Cyan**: Anchorages
- 🟡 **Yellow**: Channels
- 🟠 **Orange**: Fairways
""")

st.sidebar.markdown("---")
st.sidebar.caption("Data: AISStream.io + S&P Global Maritime")
