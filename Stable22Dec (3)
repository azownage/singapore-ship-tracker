"""
Singapore AIS Tracker with S&P Maritime Risk Intelligence
Real-time vessel tracking with compliance and risk indicators
Enhanced with persistent storage, vessel polygons, and maritime zones
v8 - Fixed: MMSI after IMO in table, IMO lookup from Ships API, SGT timestamp
"""

import streamlit as st
import asyncio
import websockets
import json
from datetime import datetime, timezone, timedelta
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

# Singapore timezone (GMT+8)
SGT = timezone(timedelta(hours=8))

# File-based persistent storage
STORAGE_FILE = "ship_data_cache.pkl"
RISK_DATA_FILE = "risk_data_cache.pkl"
MMSI_IMO_CACHE_FILE = "mmsi_imo_cache.pkl"
VESSEL_POSITION_FILE = "vessel_positions_cache.pkl"

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


def format_datetime(dt_string: str) -> str:
    """Format ISO datetime string to readable format: '20 Dec 2025, 11:54 PM'"""
    if not dt_string or dt_string == 'Unknown' or dt_string == 'Never':
        return dt_string if dt_string else 'Never'
    
    try:
        # Parse ISO format
        dt = datetime.fromisoformat(dt_string)
        # Format nicely
        return dt.strftime('%d %b %Y, %I:%M %p')
    except Exception:
        return dt_string  # Return original if parsing fails


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


def load_cache() -> Tuple[Dict, Dict, Dict, Dict]:
    """Load cached ship, risk, MMSI-to-IMO, and vessel position data from disk"""
    ship_cache = {}
    risk_cache = {}
    mmsi_imo_cache = {}
    vessel_positions = {}
    
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
    
    if os.path.exists(MMSI_IMO_CACHE_FILE):
        try:
            with open(MMSI_IMO_CACHE_FILE, 'rb') as f:
                mmsi_imo_cache = pickle.load(f)
        except Exception:
            pass
    
    if os.path.exists(VESSEL_POSITION_FILE):
        try:
            with open(VESSEL_POSITION_FILE, 'rb') as f:
                vessel_positions = pickle.load(f)
        except Exception:
            pass
    
    return ship_cache, risk_cache, mmsi_imo_cache, vessel_positions


def save_cache(ship_cache: Dict, risk_cache: Dict, mmsi_imo_cache: Dict = None, vessel_positions: Dict = None):
    """Save ship, risk, MMSI-to-IMO, and vessel position data to disk"""
    try:
        with open(STORAGE_FILE, 'wb') as f:
            pickle.dump(ship_cache, f)
        with open(RISK_DATA_FILE, 'wb') as f:
            pickle.dump(risk_cache, f)
        if mmsi_imo_cache is not None:
            with open(MMSI_IMO_CACHE_FILE, 'wb') as f:
                pickle.dump(mmsi_imo_cache, f)
        if vessel_positions is not None:
            with open(VESSEL_POSITION_FILE, 'wb') as f:
                pickle.dump(vessel_positions, f)
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
                         length: float = 60, width: float = 16,
                         zoom: float = 10,
                         dim_a: float = 0, dim_b: float = 0,
                         dim_c: float = 0, dim_d: float = 0) -> List[List[float]]:
    """
    Create a vessel-shaped polygon with pointed bow based on position, heading, and dimensions.
    
    AIS position is at the antenna location, not vessel center.
    Dimensions A, B, C, D define antenna position relative to vessel:
    - A: Distance from antenna to bow (forward)
    - B: Distance from antenna to stern (backward)  
    - C: Distance from antenna to port (left)
    - D: Distance from antenna to starboard (right)
    
    Args:
        lat: Latitude of antenna position (AIS reported)
        lon: Longitude of antenna position (AIS reported)
        heading: Heading in degrees (0 = North, 90 = East, clockwise per AIS standard)
        length: Vessel length in meters (bow to stern) = A + B
        width: Vessel width/beam in meters = C + D
        zoom: Map zoom level (used for dynamic scaling)
        dim_a, dim_b, dim_c, dim_d: AIS dimension fields
    
    Returns:
        List of [lon, lat] coordinates forming a closed polygon with pointed bow
    """
    # Validate position
    if lat is None or lon is None:
        return [[lon or 0, lat or 0]] * 7
    
    # Validate and constrain heading (0-360)
    if heading is None or heading < 0 or heading >= 360:
        heading = 0
    
    # AIS heading: 0° = North, 90° = East (clockwise from North)
    # Math convention: 0° = East, counterclockwise
    # To rotate a shape that points North (up/+Y) to point in AIS heading direction:
    # We rotate clockwise by the heading angle
    # In standard math rotation (counterclockwise positive), this is -heading
    heading_rad = math.radians(-heading)
    
    # Sanity check dimensions - reject unrealistic values
    if length <= 0 or length > 500:
        length = 50
    if width <= 0 or width > 80:
        width = 10
    
    # Geographic scale factors
    meters_per_deg_lat = 111320.0
    meters_per_deg_lon = 111320.0 * math.cos(math.radians(lat))
    
    # Visibility scaling based on zoom (only used for polygon layer at high zoom)
    if zoom >= 16:
        visibility_scale = 1.0
    elif zoom >= 14:
        visibility_scale = 2.0
    elif zoom >= 12:
        visibility_scale = 4.0
    else:
        visibility_scale = 6.0
    
    # Apply scale to dimensions
    scaled_length = length * visibility_scale
    scaled_width = width * visibility_scale
    
    # Calculate antenna offset from vessel center
    # If A, B, C, D are provided, use them; otherwise assume antenna at center
    if dim_a > 0 or dim_b > 0:
        # Offset from antenna to center along length axis
        # Positive = antenna is aft of center (move bow forward)
        offset_forward = ((dim_a - dim_b) / 2.0) * visibility_scale
    else:
        offset_forward = 0
    
    if dim_c > 0 or dim_d > 0:
        # Offset from antenna to center along width axis
        # Positive = antenna is to starboard of center (move port side left)
        offset_port = ((dim_c - dim_d) / 2.0) * visibility_scale
    else:
        offset_port = 0
    
    # Convert to degrees
    half_length = scaled_length / 2.0 / meters_per_deg_lat
    half_width = scaled_width / 2.0 / meters_per_deg_lon
    offset_fwd_deg = offset_forward / meters_per_deg_lat
    offset_port_deg = offset_port / meters_per_deg_lon
    
    # Bow triangle is WITHIN the vessel length
    # Triangle takes up front 25% of the vessel
    bow_point = half_length                # Bow tip at the front edge (within total length)
    bow_start = half_length * 0.5          # Rectangle ends, triangle starts at 50% forward
    
    # Define ship shape relative to center, pointing NORTH (up = +lat)
    # Ship outline: stern (flat) -> port side -> bow point -> starboard side -> back to stern
    # Total length from stern (-half_length) to bow (half_length) = vessel length
    corners_local = [
        (-half_width, -half_length),      # Stern port
        (-half_width, bow_start),          # Port side where triangle starts
        (0, bow_point),                    # Bow point (triangle tip) - at front edge
        (half_width, bow_start),           # Starboard side where triangle starts
        (half_width, -half_length),        # Stern starboard
        (-half_width, -half_length),       # Close polygon back to stern port
    ]
    
    # Rotate and translate corners
    cos_h = math.cos(heading_rad)
    sin_h = math.sin(heading_rad)
    
    rotated_corners = []
    for d_lon, d_lat in corners_local:
        # Apply antenna offset (in local coordinates before rotation)
        d_lat_offset = d_lat + offset_fwd_deg * meters_per_deg_lat / meters_per_deg_lat
        d_lon_offset = d_lon - offset_port_deg * meters_per_deg_lon / meters_per_deg_lon
        
        # Actually the offset should be applied differently - let me recalculate
        # The offset is in the ship's frame, so apply before rotation
        d_lat_adj = d_lat - offset_fwd_deg * (meters_per_deg_lat / meters_per_deg_lat)  # This simplifies
        d_lon_adj = d_lon + offset_port_deg * (meters_per_deg_lon / meters_per_deg_lon)
        
        # Standard 2D rotation (x' = x*cos - y*sin, y' = x*sin + y*cos)
        # Here x = lon direction, y = lat direction
        rotated_lon = d_lon_adj * cos_h - d_lat_adj * sin_h
        rotated_lat = d_lon_adj * sin_h + d_lat_adj * cos_h
        
        # Translate to actual position
        final_lon = lon + rotated_lon
        final_lat = lat + rotated_lat
        
        rotated_corners.append([final_lon, final_lat])
    
    return rotated_corners


# Initialize session state for persistent storage
if 'ship_static_cache' not in st.session_state:
    ship_cache, risk_cache, mmsi_imo_cache, vessel_positions = load_cache()
    st.session_state.ship_static_cache = ship_cache
    st.session_state.risk_data_cache = risk_cache
    st.session_state.mmsi_to_imo_cache = mmsi_imo_cache
    st.session_state.vessel_positions = vessel_positions
    st.session_state.last_save = time.time()
    st.session_state.last_data_update = vessel_positions.get('_last_update', None)

if 'selected_vessel' not in st.session_state:
    st.session_state.selected_vessel = None

if 'show_details_imo' not in st.session_state:
    st.session_state.show_details_imo = None
    st.session_state.show_details_name = None

if 'map_center' not in st.session_state:
    st.session_state.map_center = {"lat": 1.28, "lon": 103.85, "zoom": 3}


class SPShipsAPI:
    """S&P Ships API for MMSI to IMO lookup and ship details"""
    
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_url = "https://shipsapi.maritime.spglobal.com/MaritimeWCF/APSShipService.svc/RESTFul"
    
    def get_ship_details_by_imo(self, imo: str) -> Optional[Dict]:
        """Get full ship details including dark activity by IMO"""
        try:
            url = f"{self.base_url}/GetShipsByIHSLRorIMONumbersAll?imoNumbers={imo}"
            
            response = requests.get(
                url,
                auth=(self.username, self.password),
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Response structure: ShipResult[0].APSShipDetail
                if 'ShipResult' in data and data['ShipResult']:
                    ship_result = data['ShipResult'][0] if isinstance(data['ShipResult'], list) else data['ShipResult']
                    if 'APSShipDetail' in ship_result:
                        detail = ship_result['APSShipDetail']
                        
                        # Extract key fields
                        result = {
                            'imo': detail.get('IHSLRorIMOShipNo', ''),
                            'ship_name': detail.get('ShipName', ''),
                            'flag': detail.get('FlagName', ''),
                            'ship_type': detail.get('ShiptypeLevel5', ''),
                            'year_built': detail.get('YearOfBuild', ''),
                            'gross_tonnage': detail.get('GrossTonnage', ''),
                            'deadweight': detail.get('Deadweight', ''),
                            'status': detail.get('ShipStatus', ''),
                            'classification': detail.get('ClassificationSociety', ''),
                            # Ownership
                            'registered_owner': detail.get('RegisteredOwner', ''),
                            'group_beneficial_owner': detail.get('GroupBeneficialOwner', ''),
                            'operator': detail.get('Operator', ''),
                            'ship_manager': detail.get('ShipManager', ''),
                            'technical_manager': detail.get('TechnicalManager', ''),
                            'doc_company': detail.get('DOCCompany', ''),
                            # Compliance status
                            'legal_overall': int(detail.get('LegalOverall', 0) or 0),
                            'dark_activity_indicator': int(detail.get('ShipDarkActivityIndicator', 0) or 0),
                            'flag_disputed': int(detail.get('ShipFlagDisputed', 0) or 0),
                            'flag_sanctioned': int(detail.get('ShipFlagSanctionedCountry', 0) or 0),
                            # Dark Activity Confirmed events
                            'dark_activity_events': []
                        }
                        
                        # Parse dark activity confirmed events
                        if 'DarkActivityConfirmed' in detail and detail['DarkActivityConfirmed']:
                            for event in detail['DarkActivityConfirmed']:
                                dark_event = {
                                    'dark_time': event.get('Dark_Time', ''),
                                    'next_seen': event.get('NextSeen', ''),
                                    'dark_hours': event.get('Dark_Hours', ''),
                                    'dark_activity_type': event.get('Dark_Activity', ''),
                                    'dark_status': event.get('Dark_Status', ''),
                                    'area_name': event.get('Area_Name', ''),
                                    'dark_lat': float(event.get('Dark_Latitude', 0) or 0),
                                    'dark_lon': float(event.get('Dark_Longitude', 0) or 0),
                                    'dark_speed': event.get('Dark_Speed', ''),
                                    'dark_heading': event.get('Dark_Heading', ''),
                                    'dark_draught': event.get('Dark_Draught', ''),
                                    'next_lat': float(event.get('NextSeen_Latitude', 0) or 0),
                                    'next_lon': float(event.get('NextSeen_Longitude', 0) or 0),
                                    'next_speed': event.get('NextSeen_Speed', ''),
                                    'next_draught': event.get('NextSeen_Draught', ''),
                                    'dark_destination': event.get('Dark_Reported_Destination', ''),
                                    'next_destination': event.get('NextSeen_Reported_Destination', ''),
                                }
                                result['dark_activity_events'].append(dark_event)
                        
                        return result
            
            return None
            
        except Exception as e:
            st.error(f"Error fetching ship details: {e}")
            return None
    
    def get_imo_by_mmsi(self, mmsi: str) -> Optional[str]:
        """Look up IMO number from MMSI using Ships API
        
        API: GetShipDataByMMSI
        Response structure:
        {
            "shipCount": 1,
            "APSShipDetail": {
                "IHSLRorIMOShipNo": "9750892",
                ...
            },
            "APSStatus": {...}
        }
        """
        try:
            url = f"{self.base_url}/GetShipDataByMMSI?MMSI={mmsi}"
            
            response = requests.get(
                url,
                auth=(self.username, self.password),
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Check shipCount first - if 0, no ship found
                ship_count = data.get('shipCount', 0)
                if ship_count == 0:
                    return None
                
                # APSShipDetail is directly in root (not wrapped in ShipResult)
                # Field name is IHSLRorIMOShipNo (exact case)
                if 'APSShipDetail' in data and data['APSShipDetail']:
                    detail = data['APSShipDetail']
                    imo = detail.get('IHSLRorIMOShipNo')
                    if imo and str(imo) != '0' and str(imo) != '':
                        return str(imo)
                
                # Fallback: try lowercase variant (some API versions)
                if 'apsShipDetail' in data and data['apsShipDetail']:
                    detail = data['apsShipDetail']
                    imo = detail.get('IHSLRorIMOShipNo') or detail.get('ihslRorIMOShipNo')
                    if imo and str(imo) != '0' and str(imo) != '':
                        return str(imo)
            
            return None
            
        except Exception as e:
            return None
    
    def batch_get_imo_by_mmsi(self, mmsi_list: List[str]) -> Dict[str, str]:
        """Look up IMO numbers for multiple MMSIs (with caching)"""
        results = {}
        
        # Check cache first (now persisted to disk)
        if 'mmsi_to_imo_cache' not in st.session_state:
            st.session_state.mmsi_to_imo_cache = {}
        
        cache = st.session_state.mmsi_to_imo_cache
        
        # Only skip MMSIs that have a VALID IMO cached (not None)
        # This allows retrying failed lookups
        uncached_mmsis = [m for m in mmsi_list if m not in cache or cache.get(m) is None]
        
        # Return cached results for already looked up MMSIs (only valid ones)
        for mmsi in mmsi_list:
            if mmsi in cache and cache[mmsi] is not None:
                results[mmsi] = cache[mmsi]
        
        if not uncached_mmsis:
            return results
        
        st.info(f"🔍 Looking up IMO for {len(uncached_mmsis)} vessels via MMSI (Ships API)...")
        
        # Look up each MMSI (with rate limiting)
        progress_bar = st.progress(0)
        found_this_batch = 0
        for i, mmsi in enumerate(uncached_mmsis):
            imo = self.get_imo_by_mmsi(mmsi)
            
            if imo:
                cache[mmsi] = imo
                results[mmsi] = imo
                found_this_batch += 1
            # Don't cache failures - allow retry next time
            
            progress_bar.progress((i + 1) / len(uncached_mmsis))
            time.sleep(0.1)  # Rate limiting (10 req/sec max)
        
        progress_bar.empty()
        
        # Save cache to session state and disk
        st.session_state.mmsi_to_imo_cache = cache
        save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache, cache)
        
        st.success(f"✅ Found IMO for {found_this_batch}/{len(uncached_mmsis)} vessels (this lookup)")
        
        return results


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
            received_imos = set()
            
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
                    
                    # Response is a list of compliance records
                    # Each record has lrimoShipNo as the IMO identifier
                    if isinstance(data, list):
                        for ship in data:
                            # IMO field is lrimoShipNo (not lrno)
                            imo = str(ship.get('lrimoShipNo', ''))
                            if not imo:
                                continue
                            
                            received_imos.add(imo)
                            cache[imo] = {
                                'legal_overall': ship.get('legalOverall', 0),
                                'ship_bes_sanction': ship.get('shipBESSanctionList', 0),
                                'ship_eu_sanction': ship.get('shipEUSanctionList', 0),
                                'ship_ofac_sanction': ship.get('shipOFACSanctionList', 0),
                                'ship_ofac_non_sdn': ship.get('shipOFACNonSDNSanctionList', 0),
                                'ship_ofac_advisory': ship.get('shipOFACAdvisoryList', 0),
                                'ship_swiss_sanction': ship.get('shipSwissSanctionList', 0),
                                'ship_un_sanction': ship.get('shipUNSanctionList', 0),
                                'dark_activity': ship.get('shipDarkActivityIndicator', 0),
                                'flag_disputed': ship.get('shipFlagDisputed', 0),
                                'flag_sanctioned': ship.get('shipFlagSanctionedCountry', 0),
                                'flag_historical_sanctioned': ship.get('shipHistoricalFlagSanctionedCountry', 0),
                                'port_call_3m': ship.get('shipSanctionedCountryPortCallLast3m', 0),
                                'port_call_6m': ship.get('shipSanctionedCountryPortCallLast6m', 0),
                                'port_call_12m': ship.get('shipSanctionedCountryPortCallLast12m', 0),
                                'owner_ofac': ship.get('shipOwnerOFACSanctionList', 0),
                                'owner_un': ship.get('shipOwnerUNSanctionList', 0),
                                'owner_eu': ship.get('shipOwnerEUSanctionList', 0),
                                'owner_bes': ship.get('shipOwnerBESSanctionList', 0),
                                'owner_swiss': ship.get('shipOwnerSwissSanctionList', 0),
                                'owner_uae': ship.get('shipOwnerUAESanctionList', 0),
                                'owner_australian': ship.get('shipOwnerAustralianSanctionList', 0),
                                'owner_canadian': ship.get('shipOwnerCanadianSanctionList', 0),
                                'owner_fatf': ship.get('shipOwnerFATFJurisdiction', 0),
                                'owner_ofac_country': ship.get('shipOwnerOFACSanctionedCountry', 0),
                                'owner_historical_ofac_country': ship.get('shipOwnerHistoricalOFACSanctionedCountry', 0),
                                'owner_parent_non_compliance': ship.get('shipOwnerParentCompanyNonCompliance', 0),
                                'owner_parent_fatf': ship.get('shipOwnerParentFATFJurisdiction', 0),
                                'owner_parent_ofac_country': ship.get('shipOwnerParentOFACSanctionedCountry', 0),
                                'sts_partner_non_compliance': ship.get('shipSTSPartnerNonComplianceLast12m', 0),
                                'security_legal_dispute': ship.get('shipSecurityLegalDisputeEvent', 0),
                                'details_no_longer_maintained': ship.get('shipDetailsNoLongerMaintained', 0),
                                'date_amended': ship.get('dateAmended', ''),
                                'cached_at': datetime.now(SGT).isoformat()
                            }
                
                time.sleep(0.5)  # Rate limiting
            
            # For IMOs that were queried but not returned by API, mark as checked with default clear values
            # This prevents showing ❓ for vessels not in the compliance database
            for imo in uncached_imos:
                if imo not in received_imos:
                    cache[imo] = {
                        'legal_overall': 0,
                        'ship_un_sanction': 0,
                        'ship_ofac_sanction': 0,
                        'dark_activity': 0,
                        'checked_but_not_found': True,  # Flag to indicate queried but not in database
                        'cached_at': datetime.now(SGT).isoformat()
                    }
            
            st.session_state.risk_data_cache = cache
            save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache, st.session_state.get('mmsi_to_imo_cache', {}))
            st.success(f"✅ Cached compliance data for {len(received_imos)}/{len(uncached_imos)} vessels (others not in database)")
                
        except Exception as e:
            st.error(f"⚠️ S&P API error: {str(e)}")
        
        return {imo: cache.get(imo, {}) for imo in imo_numbers}


class AISTracker:
    """AIS data collection and vessel tracking"""
    
    def __init__(self, use_cached_positions: bool = True):
        # Load cached positions if available
        if use_cached_positions and 'vessel_positions' in st.session_state:
            self.ships = defaultdict(lambda: {
                'latest_position': None,
                'static_data': None
            })
            # Restore from cache
            cached = st.session_state.vessel_positions
            for mmsi, data in cached.items():
                if mmsi != '_last_update':  # Skip metadata
                    self.ships[mmsi] = data
        else:
            self.ships = defaultdict(lambda: {
                'latest_position': None,
                'static_data': None
            })
    
    def get_ship_color(self, legal_overall: int = -1) -> List[int]:
        """Return color based on legal overall compliance status
        -1 = Unknown (gray)
        0 = Clear (green)
        1 = Warning (yellow)
        2 = Severe (red)
        """
        if legal_overall == 2:
            return [220, 53, 69, 200]  # Red - Severe
        elif legal_overall == 1:
            return [255, 193, 7, 200]  # Orange/Yellow - Warning
        elif legal_overall == 0:
            return [40, 167, 69, 200]  # Green - Clear
        else:  # -1 or any other value = unknown
            return [128, 128, 128, 200]  # Gray - Unknown/Not checked
    
    def save_positions_to_cache(self):
        """Save current vessel positions to session state and disk"""
        # Convert defaultdict to regular dict for pickling
        positions_dict = dict(self.ships)
        positions_dict['_last_update'] = datetime.now(SGT).isoformat()
        
        st.session_state.vessel_positions = positions_dict
        st.session_state.last_data_update = positions_dict['_last_update']
        
        # Save to disk
        save_cache(
            st.session_state.ship_static_cache,
            st.session_state.risk_data_cache,
            st.session_state.get('mmsi_to_imo_cache', {}),
            positions_dict
        )
    
    async def collect_data(self, duration: int = 30, api_key: str = "", bounding_box: List = None):
        """Collect AIS data from AISStream.io"""
        if bounding_box is None:
            # Extended coverage area for dark fleet tracking:
            # Covers Malacca Strait, Singapore Strait, South China Sea approaches
            # From northern Malacca Strait to Natuna Sea, including all transit routes
            bounding_box = [
                [[0.5, 102.0], [2.5, 106.0]]  # Large area: Malacca Strait to South China Sea
            ]
        
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
            
            # Save positions after collection completes
            self.save_positions_to_cache()
            
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
            'timestamp': datetime.now(SGT).isoformat()
        }
        # Track when this vessel was last seen (for expiry)
        self.ships[mmsi]['last_seen'] = datetime.now(SGT).isoformat()
    
    def process_static(self, ais_message: Dict):
        """Process AIS static data report - merges with cached data to preserve dimensions"""
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
        
        # Get existing cached data for this vessel (to preserve dimensions if new data has 0s)
        existing_cached = st.session_state.ship_static_cache.get(str(mmsi), {})
        
        # If new dimensions are 0 but we have cached dimensions, keep the cached ones
        if dim_a == 0 and dim_b == 0:
            dim_a = existing_cached.get('dimension_a', 0) or 0
            dim_b = existing_cached.get('dimension_b', 0) or 0
        if dim_c == 0 and dim_d == 0:
            dim_c = existing_cached.get('dimension_c', 0) or 0
            dim_d = existing_cached.get('dimension_d', 0) or 0
        
        # If new IMO is 0 but we have cached IMO, keep the cached one
        if imo == '0' and existing_cached.get('imo', '0') != '0':
            imo = existing_cached.get('imo')
        
        static_info = {
            'name': static_data.get('Name', 'Unknown') or existing_cached.get('name', 'Unknown'),
            'imo': imo,
            'type': static_data.get('Type') or existing_cached.get('type'),
            'dimension_a': dim_a,
            'dimension_b': dim_b,
            'dimension_c': dim_c,
            'dimension_d': dim_d,
            'length': dim_a + dim_b,
            'width': dim_c + dim_d,
            'destination': static_data.get('Destination', 'Unknown') or existing_cached.get('destination', 'Unknown'),
            'call_sign': static_data.get('CallSign', '') or existing_cached.get('call_sign', ''),
            'cached_at': datetime.now(SGT).isoformat()
        }
        
        self.ships[mmsi]['static_data'] = static_info
        st.session_state.ship_static_cache[str(mmsi)] = static_info
        
        if time.time() - st.session_state.last_save > 60:
            save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache, st.session_state.get('mmsi_to_imo_cache', {}))
            st.session_state.last_save = time.time()
    
    def get_dataframe_with_compliance(self, sp_api: Optional[SPMaritimeAPI] = None, ships_api: Optional[SPShipsAPI] = None, expiry_hours: Optional[int] = None) -> pd.DataFrame:
        """Get dataframe with compliance indicators
        
        Args:
            sp_api: S&P Maritime API for compliance data
            ships_api: S&P Ships API for MMSI-to-IMO lookups
            expiry_hours: Remove vessels not seen in this many hours (None = keep all)
        """
        data = []
        now = datetime.now(SGT)
        
        for mmsi, ship_data in self.ships.items():
            pos = ship_data.get('latest_position')
            
            # Check vessel expiry
            if expiry_hours is not None:
                last_seen_str = ship_data.get('last_seen')
                if last_seen_str:
                    try:
                        last_seen = datetime.fromisoformat(last_seen_str)
                        hours_since_seen = (now - last_seen).total_seconds() / 3600
                        if hours_since_seen > expiry_hours:
                            continue  # Skip expired vessel
                    except:
                        pass  # If parsing fails, include vessel
            
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
            
            # Get dimensions from static data
            dim_a = static.get('dimension_a', 0) or 0
            dim_b = static.get('dimension_b', 0) or 0
            dim_c = static.get('dimension_c', 0) or 0
            dim_d = static.get('dimension_d', 0) or 0
            length = dim_a + dim_b
            width = dim_c + dim_d
            
            # Check if we have real dimensions
            has_real_dimensions = (length > 0 and width > 0)
            if not has_real_dimensions:
                # Use fixed default for unknown dimensions
                length = 50
                width = 10
            
            # Get last_seen timestamp
            last_seen_str = ship_data.get('last_seen', pos.get('timestamp', ''))
            
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
                'dim_a': dim_a,
                'dim_b': dim_b,
                'dim_c': dim_c,
                'dim_d': dim_d,
                'has_dimensions': has_real_dimensions,  # True only if real AIS dimensions
                'destination': (static.get('destination') or 'Unknown').strip(),
                'call_sign': static.get('call_sign', ''),
                'has_static': bool(static.get('name')),
                'last_seen': last_seen_str,
                # Core compliance fields
                'legal_overall': -1,  # -1 = not checked yet
                'un_sanction': -1,
                'ofac_sanction': -1,
                'dark_activity': -1,
                # Additional ship sanction fields
                'bes_sanction': -1,
                'eu_sanction': -1,
                'swiss_sanction': -1,
                'ofac_non_sdn': -1,
                'ofac_advisory': -1,
                # Flag fields
                'flag_disputed': -1,
                'flag_sanctioned': -1,
                # Port call fields
                'port_call_3m': -1,
                'port_call_6m': -1,
                'port_call_12m': -1,
                # Owner sanctions
                'owner_ofac': -1,
                'owner_un': -1,
                'owner_eu': -1,
                'owner_bes': -1,
                # STS and other
                'sts_partner_non_compliance': -1,
                'compliance_checked': False,  # Track if checked
                'color': self.get_ship_color(-1)  # Default unknown color
            })
        
        df = pd.DataFrame(data)
        
        if len(df) == 0:
            return df
        
        # Step 1: Get vessels WITH IMO from AIS static data
        valid_imos = [str(imo) for imo in df['imo'].unique() if imo and imo != '0']
        
        # Step 2: For vessels WITHOUT IMO, try to look up via MMSI using Ships API
        # Only do this if ships_api is provided (i.e., NOT in View Cached mode)
        missing_imo_mask = (df['imo'] == '0') | (df['imo'] == '')
        missing_imo_mmsis = df.loc[missing_imo_mask, 'mmsi'].astype(str).unique().tolist()
        
        if missing_imo_mmsis and ships_api:
            # Look up IMO by MMSI (will use cache, only API call for new ones)
            mmsi_to_imo = ships_api.batch_get_imo_by_mmsi(missing_imo_mmsis)
            
            # Update dataframe with found IMOs
            for idx, row in df.iterrows():
                if str(row['mmsi']) in mmsi_to_imo and mmsi_to_imo[str(row['mmsi'])]:
                    found_imo = mmsi_to_imo[str(row['mmsi'])]
                    df.at[idx, 'imo'] = found_imo
                    if found_imo not in valid_imos:
                        valid_imos.append(found_imo)
        elif missing_imo_mmsis and not ships_api:
            # View Cached mode: Use MMSI→IMO cache without API calls
            cache = st.session_state.get('mmsi_to_imo_cache', {})
            for idx, row in df.iterrows():
                mmsi_str = str(row['mmsi'])
                if mmsi_str in cache and cache[mmsi_str]:
                    found_imo = cache[mmsi_str]
                    df.at[idx, 'imo'] = found_imo
                    if found_imo not in valid_imos:
                        valid_imos.append(found_imo)
        
        # Step 3: Get compliance data for all IMOs
        # If sp_api provided: fetch from API (which uses cache internally)
        # If sp_api is None: use cache directly
        compliance_cache = st.session_state.get('risk_data_cache', {})
        
        if valid_imos and sp_api:
            compliance_data = sp_api.get_ship_compliance_data(valid_imos)
        else:
            # View Cached mode: use cache directly
            compliance_data = {imo: compliance_cache.get(imo, {}) for imo in valid_imos}
        
        # Apply compliance data to dataframe with ALL fields
        for idx, row in df.iterrows():
            imo = str(row['imo'])
            if imo in compliance_data and compliance_data[imo]:
                comp = compliance_data[imo]
                legal_overall = comp.get('legal_overall', -1)
                # Convert to int if needed
                if isinstance(legal_overall, str):
                    legal_overall = int(legal_overall) if legal_overall.isdigit() else -1
                
                # Core compliance fields
                df.at[idx, 'legal_overall'] = legal_overall
                df.at[idx, 'un_sanction'] = int(comp.get('ship_un_sanction', 0) or 0)
                df.at[idx, 'ofac_sanction'] = int(comp.get('ship_ofac_sanction', 0) or 0)
                df.at[idx, 'dark_activity'] = int(comp.get('dark_activity', 0) or 0)
                
                # Additional ship sanction fields
                df.at[idx, 'bes_sanction'] = int(comp.get('ship_bes_sanction', 0) or 0)
                df.at[idx, 'eu_sanction'] = int(comp.get('ship_eu_sanction', 0) or 0)
                df.at[idx, 'swiss_sanction'] = int(comp.get('ship_swiss_sanction', 0) or 0)
                df.at[idx, 'ofac_non_sdn'] = int(comp.get('ship_ofac_non_sdn', 0) or 0)
                df.at[idx, 'ofac_advisory'] = int(comp.get('ship_ofac_advisory', 0) or 0)
                
                # Flag fields
                df.at[idx, 'flag_disputed'] = int(comp.get('flag_disputed', 0) or 0)
                df.at[idx, 'flag_sanctioned'] = int(comp.get('flag_sanctioned', 0) or 0)
                
                # Port call fields
                df.at[idx, 'port_call_3m'] = int(comp.get('port_call_3m', 0) or 0)
                df.at[idx, 'port_call_6m'] = int(comp.get('port_call_6m', 0) or 0)
                df.at[idx, 'port_call_12m'] = int(comp.get('port_call_12m', 0) or 0)
                
                # Owner sanctions
                df.at[idx, 'owner_ofac'] = int(comp.get('owner_ofac', 0) or 0)
                df.at[idx, 'owner_un'] = int(comp.get('owner_un', 0) or 0)
                df.at[idx, 'owner_eu'] = int(comp.get('owner_eu', 0) or 0)
                df.at[idx, 'owner_bes'] = int(comp.get('owner_bes', 0) or 0)
                
                # STS and other
                df.at[idx, 'sts_partner_non_compliance'] = int(comp.get('sts_partner_non_compliance', 0) or 0)
                
                df.at[idx, 'compliance_checked'] = True
                df.at[idx, 'color'] = self.get_ship_color(legal_overall)
        
        return df


def create_vessel_layers(df: pd.DataFrame, zoom: float = 10) -> List[pdk.Layer]:
    """Create PyDeck layers for vessels - hybrid approach
    
    At low zoom (< 12): Use ScatterplotLayer (dots) for visibility
    At high zoom (>= 12): Use PolygonLayer (ship shapes with bow)
    
    Args:
        df: DataFrame with vessel data
        zoom: Current map zoom level
    
    Returns:
        List of layers to render
    """
    if len(df) == 0:
        return []
    
    layers = []
    
    # Build tooltip for each vessel
    vessel_data = []
    for _, row in df.iterrows():
        vessel_length = row['length'] if row['length'] > 0 and row['length'] < 500 else 50
        vessel_width = row['width'] if row['width'] > 0 and row['width'] < 80 else 10
        
        # Compliance emoji
        legal_val = row['legal_overall']
        if legal_val == 2:
            legal_emoji = '🔴'
        elif legal_val == 1:
            legal_emoji = '🟡'
        elif legal_val == 0:
            legal_emoji = '🟢'
        else:
            legal_emoji = '❓'
        
        # Dimension text - show (est.) if no real dimensions
        if row['has_dimensions']:
            dim_text = f"{vessel_length:.0f}m x {vessel_width:.0f}m"
        else:
            dim_text = f"{vessel_length:.0f}m x {vessel_width:.0f}m (est.)"
        
        tooltip_text = (
            f"<b>{row['name']}</b><br/>"
            f"IMO: {row['imo']}<br/>"
            f"MMSI: {row['mmsi']}<br/>"
            f"Type: {row['type_name']}<br/>"
            f"Size: {dim_text}<br/>"
            f"Heading: {row['heading']:.0f}°<br/>"
            f"Speed: {row['speed']:.1f} kts<br/>"
            f"Status: {row['nav_status_name']}<br/>"
            f"Compliance: {legal_emoji}<br/>"
            f"Dest: {row['destination']}"
        )
        
        vessel_data.append({
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'name': row['name'],
            'tooltip': tooltip_text,
            'color': row['color'],
            'heading': row['heading'],
            'length': vessel_length,
            'width': vessel_width,
            'dim_a': row.get('dim_a', 0) or 0,
            'dim_b': row.get('dim_b', 0) or 0,
            'dim_c': row.get('dim_c', 0) or 0,
            'dim_d': row.get('dim_d', 0) or 0,
        })
    
    if zoom < 12:
        # Low zoom: Use ScatterplotLayer (colored dots)
        # Dots stay same pixel size regardless of map zoom
        scatter_layer = pdk.Layer(
            'ScatterplotLayer',
            data=vessel_data,
            get_position=['longitude', 'latitude'],
            get_fill_color='color',
            get_radius=300,  # meters - will appear as small dots
            radius_min_pixels=4,  # minimum 4 pixels
            radius_max_pixels=15,  # maximum 15 pixels
            pickable=True,
            auto_highlight=True,
        )
        layers.append(scatter_layer)
    else:
        # High zoom: Use PolygonLayer (ship shapes with pointed bow)
        vessel_polygons = []
        for v in vessel_data:
            polygon = create_vessel_polygon(
                lat=v['latitude'],
                lon=v['longitude'],
                heading=v['heading'],
                length=v['length'],
                width=v['width'],
                zoom=zoom,
                dim_a=v['dim_a'],
                dim_b=v['dim_b'],
                dim_c=v['dim_c'],
                dim_d=v['dim_d']
            )
            vessel_polygons.append({
                'polygon': polygon,
                'name': v['name'],
                'tooltip': v['tooltip'],
                'color': v['color']
            })
        
        polygon_layer = pdk.Layer(
            'PolygonLayer',
            data=vessel_polygons,
            get_polygon='polygon',
            get_fill_color='color',
            get_line_color=[50, 50, 50, 100],
            line_width_min_pixels=1,
            pickable=True,
            auto_highlight=True,
            extruded=False,
        )
        layers.append(polygon_layer)
    
    return layers


def create_zone_layer(zones: List[Dict], color: List[int], layer_id: str) -> pdk.Layer:
    """Create PyDeck polygon layer for maritime zones"""
    if not zones:
        return None
    
    zone_data = []
    zone_type_name = layer_id.replace('_', ' ').title()
    for zone in zones:
        zone_data.append({
            'polygon': zone['polygon'],
            'name': zone['name'],
            'tooltip': f"<b>{zone['name']}</b><br/>Type: {zone_type_name}"
        })
    
    return pdk.Layer(
        'PolygonLayer',
        data=zone_data,
        id=layer_id,
        get_polygon='polygon',
        get_fill_color=color,
        get_line_color=[100, 100, 100, 150],
        line_width_min_pixels=1,
        pickable=True,
        auto_highlight=True,
        extruded=False,
    )


def show_vessel_details_panel(imo: str, vessel_name: str):
    """Display detailed vessel information including dark activity events"""
    
    if not imo or imo == '0':
        st.warning("⚠️ No IMO number available for this vessel.")
        return
    
    with st.expander(f"📋 Vessel Details: {vessel_name} (IMO: {imo})", expanded=True):
        # Close button
        if st.button("❌ Close Details", key="close_details"):
            st.session_state.show_details_imo = None
            st.session_state.show_details_name = None
            st.rerun()
        
        # Fetch details from Ships API
        ships_api = SPShipsAPI(sp_username, sp_password)
        
        with st.spinner("🔍 Fetching vessel details from S&P Ships API..."):
            details = ships_api.get_ship_details_by_imo(imo)
        
        if not details:
            st.error("❌ Could not fetch vessel details. Please try again.")
            return
        
        # Vessel Information
        st.subheader("🚢 Vessel Information")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"**Name:** {details.get('ship_name', 'N/A')}")
            st.markdown(f"**IMO:** {details.get('imo', 'N/A')}")
            st.markdown(f"**Flag:** {details.get('flag', 'N/A')}")
            st.markdown(f"**Type:** {details.get('ship_type', 'N/A')}")
        
        with col2:
            st.markdown(f"**Year Built:** {details.get('year_built', 'N/A')}")
            st.markdown(f"**GT:** {details.get('gross_tonnage', 'N/A')}")
            st.markdown(f"**DWT:** {details.get('deadweight', 'N/A')}")
            st.markdown(f"**Status:** {details.get('status', 'N/A')}")
        
        with col3:
            st.markdown(f"**Class:** {details.get('classification', 'N/A')}")
            legal = details.get('legal_overall', 0)
            legal_emoji = '🔴 Severe' if legal == 2 else ('🟡 Warning' if legal == 1 else '🟢 Clear')
            st.markdown(f"**Legal Status:** {legal_emoji}")
            dark_ind = details.get('dark_activity_indicator', 0)
            dark_emoji = '🔴 Severe' if dark_ind == 2 else ('🟡 Warning' if dark_ind == 1 else '🟢 Clear')
            st.markdown(f"**Dark Activity:** {dark_emoji}")
        
        # Ownership Information
        st.subheader("🏢 Ownership & Management")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"**Registered Owner:** {details.get('registered_owner', 'N/A')}")
            st.markdown(f"**Group Beneficial Owner:** {details.get('group_beneficial_owner', 'N/A')}")
            st.markdown(f"**Operator:** {details.get('operator', 'N/A')}")
        
        with col2:
            st.markdown(f"**Ship Manager:** {details.get('ship_manager', 'N/A')}")
            st.markdown(f"**Technical Manager:** {details.get('technical_manager', 'N/A')}")
            st.markdown(f"**DOC Company:** {details.get('doc_company', 'N/A')}")
        
        # Dark Activity Events
        dark_events = details.get('dark_activity_events', [])
        
        if dark_events:
            st.subheader(f"🌑 Dark Activity Events ({len(dark_events)} recorded)")
            
            for i, event in enumerate(dark_events):
                with st.container():
                    st.markdown(f"---")
                    st.markdown(f"**Event {i+1}: {event.get('dark_activity_type', 'Unknown')}**")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.markdown("**🔴 When Dark:**")
                        st.markdown(f"Time: {event.get('dark_time', 'N/A')}")
                        st.markdown(f"Area: {event.get('area_name', 'N/A')}")
                        st.markdown(f"Duration: {event.get('dark_hours', 'N/A')} hours")
                        st.markdown(f"Position: {event.get('dark_lat', 0):.4f}, {event.get('dark_lon', 0):.4f}")
                    
                    with col2:
                        st.markdown("**🟢 When Seen Again:**")
                        st.markdown(f"Time: {event.get('next_seen', 'N/A')}")
                        st.markdown(f"Position: {event.get('next_lat', 0):.4f}, {event.get('next_lon', 0):.4f}")
                        st.markdown(f"Speed: {event.get('next_speed', 'N/A')} kts")
                        st.markdown(f"Draught: {event.get('next_draught', 'N/A')}m")
                    
                    with col3:
                        st.markdown("**📍 Movement Details:**")
                        st.markdown(f"Speed (dark): {event.get('dark_speed', 'N/A')} kts")
                        st.markdown(f"Heading (dark): {event.get('dark_heading', 'N/A')}°")
                        st.markdown(f"Draught (dark): {event.get('dark_draught', 'N/A')}m")
                        st.markdown(f"Dest (dark): {event.get('dark_destination', 'N/A')}")
                        st.markdown(f"Dest (next): {event.get('next_destination', 'N/A')}")
            
            # Create a mini-map showing dark activity locations
            st.subheader("🗺️ Dark Activity Locations")
            dark_locations = []
            for i, event in enumerate(dark_events):
                if event.get('dark_lat') and event.get('dark_lon'):
                    dark_locations.append({
                        'lat': event['dark_lat'],
                        'lon': event['dark_lon'],
                        'type': 'dark',
                        'label': f"Dark {i+1}",
                        'color': [255, 0, 0, 200]
                    })
                if event.get('next_lat') and event.get('next_lon'):
                    dark_locations.append({
                        'lat': event['next_lat'],
                        'lon': event['next_lon'],
                        'type': 'seen',
                        'label': f"Seen {i+1}",
                        'color': [0, 255, 0, 200]
                    })
            
            if dark_locations:
                dark_df = pd.DataFrame(dark_locations)
                
                # Calculate center
                center_lat = dark_df['lat'].mean()
                center_lon = dark_df['lon'].mean()
                
                dark_layer = pdk.Layer(
                    'ScatterplotLayer',
                    data=dark_df,
                    get_position=['lon', 'lat'],
                    get_fill_color='color',
                    get_radius=5000,
                    pickable=True,
                )
                
                dark_view = pdk.ViewState(
                    latitude=center_lat,
                    longitude=center_lon,
                    zoom=8,
                    pitch=0,
                )
                
                dark_deck = pdk.Deck(
                    map_style='mapbox://styles/mapbox/dark-v10',
                    initial_view_state=dark_view,
                    layers=[dark_layer],
                    tooltip={'text': '{label}'}
                )
                
                st.pydeck_chart(dark_deck, use_container_width=True)
                st.caption("🔴 Red = Position when went dark | 🟢 Green = Position when seen again")
        else:
            st.info("✅ No dark activity events recorded for this vessel.")


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
st.sidebar.header("📡 AIS Settings")
duration = st.sidebar.slider("AIS collection time (seconds)", 10, 120, 60)
enable_compliance = st.sidebar.checkbox("Enable S&P compliance screening", value=True)

# Coverage Area Selection
st.sidebar.subheader("Coverage Area")
coverage_options = {
    "Singapore Strait Only": [[[1.15, 103.55], [1.50, 104.10]]],
    "Singapore + Approaches": [[[1.0, 103.3], [1.6, 104.3]]],
    "Malacca to South China Sea (Dark Fleet)": [[[0.5, 102.0], [2.5, 106.0]]],
    "Extended Malacca Strait": [[[-0.5, 100.0], [3.0, 106.0]]],
    "Full Regional (Max Coverage)": [[[-1.0, 99.0], [4.0, 108.0]]]
}
selected_coverage = st.sidebar.selectbox(
    "Select coverage area",
    options=list(coverage_options.keys()),
    index=2,  # Default to "Malacca to SCS (Dark Fleet)"
    help="Larger areas = more vessels but longer collection time recommended"
)
coverage_bbox = coverage_options[selected_coverage]

# Show coverage info
coverage_info = {
    "Singapore Strait Only": "~50km² - Singapore port and anchorages",
    "Singapore + Approaches": "~150km² - Includes eastern/western approaches", 
    "Malacca to South China Sea (Dark Fleet)": "~800km² - Main transit route for dark fleet",
    "Extended Malacca Strait": "~2000km² - Full Malacca Strait coverage",
    "Full Regional (Max Coverage)": "~4000km² - Maximum regional coverage"
}
st.sidebar.caption(coverage_info[selected_coverage])

# Vessel Expiry Setting
st.sidebar.subheader("⏱️ Vessel Expiry")
expiry_options = {
    "1 hour": 1,
    "2 hours": 2,
    "4 hours": 4,
    "8 hours": 8,
    "12 hours": 12,
    "24 hours": 24,
    "Never (keep forever)": None
}
selected_expiry = st.sidebar.selectbox(
    "Remove vessels not seen in:",
    options=list(expiry_options.keys()),
    index=2,  # Default to 4 hours
    help="Vessels not detected within this time will be removed from display"
)
vessel_expiry_hours = expiry_options[selected_expiry]

# Maritime Zones
st.sidebar.header("🗺️ Maritime Zones")
show_anchorages = st.sidebar.checkbox("Show Anchorages", value=True)
show_channels = st.sidebar.checkbox("Show Channels", value=True)
show_fairways = st.sidebar.checkbox("Show Fairways", value=True)

# Map Zoom Control
st.sidebar.header("🔍 Map View")
zoom_level = st.sidebar.slider(
    "Zoom Level",
    min_value=6,
    max_value=18,
    value=st.session_state.get('user_zoom', 10),
    help="Higher zoom = closer view, vessels at actual scale. Lower zoom = wider view, vessels enlarged for visibility."
)
st.session_state.user_zoom = zoom_level

# Show scale info
scale_info = {
    6: "20x scale (regional)",
    7: "20x scale (regional)",
    8: "12x scale (wide area)",
    9: "12x scale (wide area)",
    10: "6x scale (port area)",
    11: "6x scale (port area)",
    12: "3x scale (harbor)",
    13: "3x scale (harbor)",
    14: "1.5x scale (close)",
    15: "1.5x scale (close)",
    16: "1:1 actual scale",
    17: "1:1 actual scale",
    18: "1:1 actual scale"
}
st.sidebar.caption(f"Vessel scale: {scale_info.get(zoom_level, '1:1')}")

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

# Quick Filter Presets
st.sidebar.subheader("Quick Filters")
quick_filter = st.sidebar.radio(
    "Preset",
    ["All Vessels", "Dark Fleet Focus", "Sanctioned Only", "Custom"],
    index=0,
    horizontal=True
)

# Set filter defaults based on quick filter
if quick_filter == "Dark Fleet Focus":
    default_compliance = ["Severe (🔴)", "Warning (🟡)"]
    default_sanctions = ["Dark Activity"]
    default_types = ["Tanker", "Cargo"]
elif quick_filter == "Sanctioned Only":
    default_compliance = ["Severe (🔴)"]
    default_sanctions = ["UN Sanctions", "OFAC Sanctions"]
    default_types = ["All"]
else:
    default_compliance = ["All"]
    default_sanctions = ["All"]
    default_types = ["All"]

# Compliance filters
st.sidebar.subheader("Compliance")
compliance_options = ["All", "Severe (🔴)", "Warning (🟡)", "Clear (🟢)"]
selected_compliance = st.sidebar.multiselect(
    "Legal Status", 
    compliance_options, 
    default=default_compliance if quick_filter != "Custom" else ["All"]
)

sanction_options = ["All", "UN Sanctions", "OFAC Sanctions", "Dark Activity"]
selected_sanctions = st.sidebar.multiselect(
    "Sanctions & Dark Activity", 
    sanction_options, 
    default=default_sanctions if quick_filter != "Custom" else ["All"]
)

# Vessel type filter
st.sidebar.subheader("Vessel Type")
vessel_types = ["All", "Cargo", "Tanker", "Passenger", "Tug", "Fishing", 
                "High Speed Craft", "Pilot", "SAR", "Port Tender", "Law Enforcement", "Other", "Unknown"]
selected_types = st.sidebar.multiselect(
    "Types", 
    vessel_types, 
    default=default_types if quick_filter != "Custom" else ["All"]
)

# Navigation status filter
st.sidebar.subheader("Navigation Status")
nav_status_options = ["All"] + list(NAV_STATUS_NAMES.values())
selected_nav_statuses = st.sidebar.multiselect("Status", nav_status_options, default=["All"])

# Static data filter
show_static_only = st.sidebar.checkbox("Ships with static data only", value=False)

# Cache statistics
st.sidebar.header("💾 Cache Statistics")
mmsi_cache = st.session_state.get('mmsi_to_imo_cache', {})
mmsi_imo_count = len(mmsi_cache)
mmsi_imo_found = len([v for v in mmsi_cache.values() if v])
mmsi_imo_failed = len([v for v in mmsi_cache.values() if v is None])
vessel_count = len([k for k in st.session_state.get('vessel_positions', {}).keys() if k != '_last_update'])
last_update_raw = st.session_state.get('last_data_update', 'Never')
last_update_fmt = format_datetime(last_update_raw) if last_update_raw else 'Never'

st.sidebar.info(f"""
**Cached Vessels:** {vessel_count}

**Static Data:** {len(st.session_state.ship_static_cache)} vessels

**Compliance:** {len(st.session_state.risk_data_cache)} vessels

**MMSI→IMO:** {mmsi_imo_found} found

**Last Update:** {last_update_fmt}
""")

col1, col2 = st.sidebar.columns(2)
if col1.button("🗑️ Clear All"):
    st.session_state.ship_static_cache = {}
    st.session_state.risk_data_cache = {}
    st.session_state.mmsi_to_imo_cache = {}
    st.session_state.vessel_positions = {}
    st.session_state.last_data_update = None
    save_cache({}, {}, {}, {})
    st.sidebar.success("Cache cleared!")
    st.rerun()

if col2.button("🔄 Retry IMO"):
    # Clear only the MMSI cache to force re-lookup
    st.session_state.mmsi_to_imo_cache = {}
    st.sidebar.success("MMSI→IMO cache cleared! Will retry lookups.")
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
    
    # Compliance filters (skip if "All" is selected OR nothing selected)
    if selected_compliance and "All" not in selected_compliance:
        compliance_map = {
            "Severe (🔴)": 2,
            "Warning (🟡)": 1,
            "Clear (🟢)": 0
        }
        selected_levels = [compliance_map[c] for c in selected_compliance if c in compliance_map]
        if selected_levels:
            filtered_df = filtered_df[filtered_df['legal_overall'].isin(selected_levels)]
    
    # Sanctions & Dark Activity filter (skip if "All" is selected OR nothing selected)
    if selected_sanctions and "All" not in selected_sanctions:
        sanction_mask = pd.Series([False] * len(filtered_df), index=filtered_df.index)
        if "UN Sanctions" in selected_sanctions:
            sanction_mask = sanction_mask | (filtered_df['un_sanction'] == 2)
        if "OFAC Sanctions" in selected_sanctions:
            sanction_mask = sanction_mask | (filtered_df['ofac_sanction'] == 2)
        if "Dark Activity" in selected_sanctions:
            sanction_mask = sanction_mask | (filtered_df['dark_activity'] >= 1)  # Include both warning (1) and severe (2)
        filtered_df = filtered_df[sanction_mask]
    
    # Vessel type filter (skip if "All" is selected OR nothing selected)
    if selected_types and "All" not in selected_types:
        filtered_df = filtered_df[filtered_df['type_name'].isin(selected_types)]
    
    # Navigation status filter (skip if "All" is selected OR nothing selected)
    if selected_nav_statuses and "All" not in selected_nav_statuses:
        filtered_df = filtered_df[filtered_df['nav_status_name'].isin(selected_nav_statuses)]
    
    # Static data filter
    if show_static_only:
        filtered_df = filtered_df[filtered_df['has_static'] == True]
    
    return filtered_df


def format_compliance_value(val) -> str:
    """Format compliance values with emoji
    -1 or None = Not checked (unknown)
    0 = Clear
    1 = Warning
    2 = Severe
    """
    if val is None or val == -1 or val == '-1':
        return "❓"  # Unknown/not checked
    elif val == 2 or val == '2':
        return "🔴"  # Severe
    elif val == 1 or val == '1':
        return "🟡"  # Warning
    elif val == 0 or val == '0':
        return "🟢"  # Clear
    else:
        return "❓"  # Default to unknown


def display_cached_data():
    """Display cached vessel data without collecting new AIS data"""
    
    # Check if we have cached data
    if 'vessel_positions' not in st.session_state or not st.session_state.vessel_positions:
        st.info("ℹ️ No cached vessel data. Click 'Refresh Now' to collect AIS data.")
        return
    
    cached_positions = st.session_state.vessel_positions
    last_update = cached_positions.get('_last_update', 'Unknown')
    
    # For View Cached: DON'T pass API objects to avoid re-querying
    # We just use whatever compliance data is already in cache
    # New API lookups only happen on "Refresh Now"
    
    # Create tracker with cached positions
    tracker = AISTracker(use_cached_positions=True)
    
    # Get dataframe using ONLY cached data (sp_api=None, ships_api=None)
    # This prevents any new API calls - just displays cached data
    df = tracker.get_dataframe_with_compliance(sp_api=None, ships_api=None, expiry_hours=vessel_expiry_hours)
    
    if df.empty:
        st.info("ℹ️ No cached vessel data. Click 'Refresh Now' to collect AIS data.")
        return
    
    # Apply filters
    df = apply_filters(df)
    
    if df.empty:
        st.warning("⚠️ No vessels match the current filters. Adjust filters to see vessels.")
        # Still show empty map with zones
        display_vessel_data(df, last_update, is_cached=True, show_empty_message=True)
        return
    
    # Display the data (same as update_display)
    display_vessel_data(df, last_update, is_cached=True)


def display_vessel_data(df: pd.DataFrame, last_update: str, is_cached: bool = False, show_empty_message: bool = False):
    """Common function to display vessel data on map and table"""
    
    # Display statistics
    with stats_placeholder:
        cols = st.columns(8)
        cols[0].metric("🚢 Total Ships", len(df))
        cols[1].metric("⚡ Moving", len(df[df['speed'] > 1]) if len(df) > 0 else 0)
        cols[2].metric("📡 Has Static", int(df['has_static'].sum()) if len(df) > 0 else 0)
        
        if len(df) > 0:
            severe_count = len(df[df['legal_overall'] == 2])
            warning_count = len(df[df['legal_overall'] == 1])
            clear_count = len(df[df['legal_overall'] == 0])
            unknown_count = len(df[df['legal_overall'] < 0])
            real_dims = int(df['has_dimensions'].sum())
        else:
            severe_count = warning_count = clear_count = unknown_count = real_dims = 0
        
        cols[3].metric("🔴 Severe", severe_count)
        cols[4].metric("🟡 Warning", warning_count)
        cols[5].metric("🟢 Clear", clear_count)
        cols[6].metric("❓ Unknown", unknown_count)
        cols[7].metric("📐 Real Dims", real_dims)
    
    # Determine map view
    # Use user-selected zoom level from sidebar, or default
    user_zoom = st.session_state.get('user_zoom', 10)
    
    # Singapore bounds: lat 1.15-1.47, lon 103.6-104.1
    # Center point that shows all of Singapore nicely
    SINGAPORE_CENTER_LAT = 1.28  # Centered on Singapore
    SINGAPORE_CENTER_LON = 103.85
    SINGAPORE_DEFAULT_ZOOM = 3  # Shows whole Singapore nicely
    
    if st.session_state.selected_vessel:
        vessel = df[df['mmsi'] == st.session_state.selected_vessel]
        if len(vessel) > 0:
            center_lat = vessel.iloc[0]['latitude']
            center_lon = vessel.iloc[0]['longitude']
            zoom = max(user_zoom, 14)  # At least zoom 14 when viewing a specific vessel
        else:
            center_lat = st.session_state.map_center['lat']
            center_lon = st.session_state.map_center['lon']
            zoom = user_zoom
    else:
        center_lat = SINGAPORE_CENTER_LAT
        center_lon = SINGAPORE_CENTER_LON
        zoom = user_zoom
    
    # Create map layers
    layers = []
    
    # Add maritime zone layers (underneath vessels)
    if show_anchorages and maritime_zones['Anchorages']:
        anchorage_layer = create_zone_layer(
            maritime_zones['Anchorages'], 
            [0, 255, 255, 50],
            "anchorages"
        )
        if anchorage_layer:
            layers.append(anchorage_layer)
    
    if show_channels and maritime_zones['Channels']:
        channel_layer = create_zone_layer(
            maritime_zones['Channels'], 
            [255, 255, 0, 50],
            "channels"
        )
        if channel_layer:
            layers.append(channel_layer)
    
    if show_fairways and maritime_zones['Fairways']:
        fairway_layer = create_zone_layer(
            maritime_zones['Fairways'], 
            [255, 165, 0, 50],
            "fairways"
        )
        if fairway_layer:
            layers.append(fairway_layer)
    
    # Add vessel layers (on top) - hybrid: dots at low zoom, shapes at high zoom
    vessel_layers = create_vessel_layers(df, zoom=zoom)
    layers.extend(vessel_layers)
    
    # Create map
    with map_placeholder:
        view_state = pdk.ViewState(
            latitude=center_lat,
            longitude=center_lon,
            zoom=zoom,
            pitch=0,
        )
        
        deck = pdk.Deck(
            map_style='',
            initial_view_state=view_state,
            layers=layers,
            tooltip={
                'html': '{tooltip}',
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
            st.session_state.map_center = {"lat": 1.28, "lon": 103.85, "zoom": 10}
            st.rerun()
    
    with table_placeholder:
        if len(df) == 0:
            st.info("No vessels to display. Adjust filters or refresh data.")
        else:
            # Create a sort key that orders: Severe(2) > Warning(1) > Not Checked(-1) > Clear(0)
            # Map: 2->3, 1->2, -1->1, 0->0 for descending sort
            def compliance_sort_key(val):
                if val == 2:
                    return 3  # Severe - highest
                elif val == 1:
                    return 2  # Warning
                elif val == -1:
                    return 1  # Not checked
                else:
                    return 0  # Clear - lowest
            
            display_df = df.copy()
            display_df['_sort_key'] = display_df['legal_overall'].apply(compliance_sort_key)
            display_df = display_df.sort_values(['_sort_key', 'name'], ascending=[False, True])
            display_df = display_df.drop(columns=['_sort_key'])
            
            # Format boolean/status columns with emojis
            display_df['has_static_display'] = display_df['has_static'].map({True: '✅', False: '❌'})
            display_df['legal_display'] = display_df['legal_overall'].apply(format_compliance_value)
            display_df['un_display'] = display_df['un_sanction'].apply(format_compliance_value)
            display_df['ofac_display'] = display_df['ofac_sanction'].apply(format_compliance_value)
            display_df['dark_display'] = display_df['dark_activity'].apply(format_compliance_value)
            
            # Additional compliance indicators
            display_df['bes_display'] = display_df['bes_sanction'].apply(format_compliance_value)
            display_df['eu_display'] = display_df['eu_sanction'].apply(format_compliance_value)
            display_df['swiss_display'] = display_df['swiss_sanction'].apply(format_compliance_value)
            display_df['flag_disp_display'] = display_df['flag_disputed'].apply(format_compliance_value)
            display_df['flag_sanc_display'] = display_df['flag_sanctioned'].apply(format_compliance_value)
            display_df['port3m_display'] = display_df['port_call_3m'].apply(format_compliance_value)
            display_df['port6m_display'] = display_df['port_call_6m'].apply(format_compliance_value)
            display_df['port12m_display'] = display_df['port_call_12m'].apply(format_compliance_value)
            display_df['owner_ofac_display'] = display_df['owner_ofac'].apply(format_compliance_value)
            display_df['owner_un_display'] = display_df['owner_un'].apply(format_compliance_value)
            display_df['sts_display'] = display_df['sts_partner_non_compliance'].apply(format_compliance_value)
            
            # Select and rename columns for display - organized by category
            # Core info | Ship Sanctions | Owner Sanctions | Dark/STS | Port Calls | Flag
            table_df = display_df[[
                'name', 'imo', 'mmsi', 'type_name', 'nav_status_name', 
                'legal_display', 'un_display', 'ofac_display', 'eu_display', 'bes_display', 'swiss_display',
                'owner_un_display', 'owner_ofac_display',
                'dark_display', 'sts_display',
                'port3m_display', 'port6m_display', 'port12m_display',
                'flag_sanc_display', 'flag_disp_display'
            ]].copy()
            table_df.columns = [
                'Name', 'IMO', 'MMSI', 'Type', 'Nav Status',
                'Legal', 'UN', 'OFAC', 'EU', 'UK', 'Swiss',
                'Own UN', 'Own OFAC',
                'Dark', 'STS',
                'Port 3m', 'Port 6m', 'Port 12m',
                'Flag Sanc', 'Flag Disp'
            ]
            
            # Display the dataframe with selection
            selected_rows = st.dataframe(
                table_df,
                use_container_width=True,
                height=500,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row"
            )
            
            # Handle row selection for map view
            if selected_rows and selected_rows.selection and selected_rows.selection.rows:
                selected_idx = selected_rows.selection.rows[0]
                selected_mmsi = display_df.iloc[selected_idx]['mmsi']
                selected_imo = display_df.iloc[selected_idx]['imo']
                
                col1, col2, col3 = st.columns([3, 1, 1])
                col1.info(f"Selected: **{table_df.iloc[selected_idx]['Name']}** (IMO: {table_df.iloc[selected_idx]['IMO']}, MMSI: {table_df.iloc[selected_idx]['MMSI']})")
                if col2.button("🗺️ View on Map"):
                    st.session_state.selected_vessel = selected_mmsi
                    st.rerun()
                if col3.button("📋 View Details"):
                    st.session_state.show_details_imo = selected_imo
                    st.session_state.show_details_name = table_df.iloc[selected_idx]['Name']
            
            # Show vessel details panel if requested
            if st.session_state.get('show_details_imo') and sp_username and sp_password:
                show_vessel_details_panel(st.session_state.show_details_imo, st.session_state.get('show_details_name', ''))
    
    # Display timestamp
    cache_indicator = " 📦 (cached)" if is_cached else ""
    if isinstance(last_update, str) and last_update != 'Unknown':
        formatted_time = format_datetime(last_update)
        st.success(f"✅ Last updated: {formatted_time} SGT{cache_indicator}")
    else:
        sgt_now = datetime.now(SGT)
        st.success(f"✅ Last updated: {sgt_now.strftime('%d %b %Y, %I:%M %p')} SGT{cache_indicator}")


def update_display():
    """Main function to collect data and update display"""
    
    # Initialize APIs
    sp_api = None
    ships_api = None
    if enable_compliance and sp_username and sp_password:
        sp_api = SPMaritimeAPI(sp_username, sp_password)
        ships_api = SPShipsAPI(sp_username, sp_password)  # Same credentials for Ships API
    
    # Collect AIS data
    with status_placeholder:
        with st.spinner(f'🔄 Collecting AIS data for {duration} seconds ({selected_coverage})...'):
            tracker = AISTracker(use_cached_positions=True)  # Start with cached, then update
            if ais_api_key:
                asyncio.run(tracker.collect_data(duration, ais_api_key, coverage_bbox))
            else:
                st.warning("⚠️ No AISStream API key provided. Please add it to secrets.")
                return
            
            df = tracker.get_dataframe_with_compliance(sp_api, ships_api, expiry_hours=vessel_expiry_hours)
    
    status_placeholder.empty()
    
    if df.empty:
        st.warning("⚠️ No ships detected. Try increasing collection time or check API key.")
        return
    
    # Apply filters
    df = apply_filters(df)
    
    if df.empty:
        st.warning("⚠️ No vessels match the current filters. Adjust filters to see vessels.")
        # Get last update time
        last_update = st.session_state.get('last_data_update', datetime.now(SGT).strftime('%Y-%m-%d %H:%M:%S'))
        # Still show empty map with zones
        display_vessel_data(df, last_update, is_cached=False, show_empty_message=True)
        return
    
    # Get last update time
    last_update = st.session_state.get('last_data_update', datetime.now(SGT).strftime('%Y-%m-%d %H:%M:%S'))
    
    # Display the data
    display_vessel_data(df, last_update, is_cached=False)
    
    # Save cache
    save_cache(
        st.session_state.ship_static_cache, 
        st.session_state.risk_data_cache, 
        st.session_state.get('mmsi_to_imo_cache', {}),
        st.session_state.get('vessel_positions', {})
    )


# Control buttons
col1, col2, col3 = st.columns([1, 1, 4])
if col1.button("🔄 Refresh Now", type="primary"):
    st.session_state.last_refresh_time = time.time()
    update_display()
    st.session_state.data_loaded = True
if col2.button("📦 View Cached"):
    display_cached_data()
    st.session_state.data_loaded = True

# Auto-refresh option
st.sidebar.markdown("---")
st.sidebar.markdown("### ⏱️ Auto-Refresh")
auto_refresh = st.sidebar.checkbox("Enable auto-refresh", value=False)

if auto_refresh:
    refresh_interval = st.sidebar.selectbox(
        "Refresh interval",
        options=[30, 60, 120, 300, 600],
        format_func=lambda x: f"{x}s" if x < 60 else f"{x//60} min",
        index=1
    )
    
    # Initialize last refresh time if not set
    if 'last_refresh_time' not in st.session_state:
        st.session_state.last_refresh_time = 0
    
    # Calculate elapsed time
    elapsed = time.time() - st.session_state.last_refresh_time
    
    if elapsed >= refresh_interval:
        # Time to refresh
        st.session_state.last_refresh_time = time.time()
        update_display()
        st.session_state.data_loaded = True
    else:
        # Show countdown and cached data
        remaining = int(refresh_interval - elapsed)
        mins, secs = divmod(remaining, 60)
        if mins > 0:
            countdown_text = f"{mins}m {secs}s"
        else:
            countdown_text = f"{secs}s"
        
        st.sidebar.info(f"⏱️ Next refresh in **{countdown_text}**")
        
        # Display cached data while waiting
        if st.session_state.get('data_loaded'):
            display_cached_data()
        
        # Wait 1 second then rerun to update countdown
        time.sleep(1)
        st.rerun()

# Show cached data on page load if available
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

if not st.session_state.data_loaded and not auto_refresh:
    # Check if we have cached data to display
    if 'vessel_positions' in st.session_state and st.session_state.vessel_positions:
        cached_count = len([k for k in st.session_state.vessel_positions.keys() if k != '_last_update'])
        if cached_count > 0:
            st.info(f"📦 Found {cached_count} cached vessels. Click 'View Cached' to display, or 'Refresh Now' to get fresh data.")
        else:
            st.info("👆 Click 'Refresh Now' to start collecting AIS data")
    else:
        st.info("👆 Click 'Refresh Now' to start collecting AIS data")

# Legend
st.sidebar.markdown("---")
st.sidebar.markdown("### 🎨 Legend")
st.sidebar.markdown("""
**Vessel Colors & Indicators:**
- 🔴 Severe
- 🟡 Warning
- 🟢 Clear
- ❓ Not Checked (No IMO)

**Zone Colors:**
- 🔵 Anchorages
- 🟡 Channels
- 🟠 Fairways
""")

st.sidebar.markdown("---")
st.sidebar.caption("Data: AISStream.io + S&P Global Maritime")
