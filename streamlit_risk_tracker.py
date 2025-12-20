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


def load_cache() -> Tuple[Dict, Dict, Dict]:
    """Load cached ship, risk, and MMSI-to-IMO data from disk"""
    ship_cache = {}
    risk_cache = {}
    mmsi_imo_cache = {}
    
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
    
    return ship_cache, risk_cache, mmsi_imo_cache


def save_cache(ship_cache: Dict, risk_cache: Dict, mmsi_imo_cache: Dict = None):
    """Save ship, risk, and MMSI-to-IMO data to disk"""
    try:
        with open(STORAGE_FILE, 'wb') as f:
            pickle.dump(ship_cache, f)
        with open(RISK_DATA_FILE, 'wb') as f:
            pickle.dump(risk_cache, f)
        if mmsi_imo_cache is not None:
            with open(MMSI_IMO_CACHE_FILE, 'wb') as f:
                pickle.dump(mmsi_imo_cache, f)
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
    ship_cache, risk_cache, mmsi_imo_cache = load_cache()
    st.session_state.ship_static_cache = ship_cache
    st.session_state.risk_data_cache = risk_cache
    st.session_state.mmsi_to_imo_cache = mmsi_imo_cache
    st.session_state.last_save = time.time()

if 'selected_vessel' not in st.session_state:
    st.session_state.selected_vessel = None

if 'map_center' not in st.session_state:
    st.session_state.map_center = {"lat": 1.5, "lon": 104.0, "zoom": 8}


class SPShipsAPI:
    """S&P Ships API for MMSI to IMO lookup"""
    
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_url = "https://shipsapi.maritime.spglobal.com/MaritimeWCF/APSShipService.svc/RESTFul"
    
    def get_imo_by_mmsi(self, mmsi: str) -> Optional[str]:
        """Look up IMO number from MMSI using Ships API"""
        try:
            url = f"{self.base_url}/GetShipDataByMMSI?MMSI={mmsi}"
            
            response = requests.get(
                url,
                auth=(self.username, self.password),
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Based on actual API response structure:
                # Response has: apsStatus, shipCount, apsShipDetail
                # IMO is in apsShipDetail.ihslRorIMOShipNo (camelCase)
                
                if 'apsShipDetail' in data and data['apsShipDetail']:
                    detail = data['apsShipDetail']
                    # The field is ihslRorIMOShipNo (camelCase with lowercase 'ihs')
                    imo = detail.get('ihslRorIMOShipNo') or detail.get('lrno')
                    if imo and str(imo) != '0' and str(imo) != '':
                        return str(imo)
                
                # Also try alternate capitalizations just in case
                if 'ApsShipDetail' in data and data['ApsShipDetail']:
                    detail = data['ApsShipDetail']
                    imo = detail.get('ihslRorIMOShipNo') or detail.get('IhslRorIMOShipNo') or detail.get('lrno')
                    if imo and str(imo) != '0' and str(imo) != '':
                        return str(imo)
                
                # Check ShipResult wrapper (older API versions)
                if 'ShipResult' in data and data['ShipResult']:
                    ship_data = data['ShipResult']
                    if isinstance(ship_data, dict):
                        if 'apsShipDetail' in ship_data:
                            detail = ship_data['apsShipDetail']
                            imo = detail.get('ihslRorIMOShipNo') or detail.get('lrno')
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
        uncached_mmsis = [m for m in mmsi_list if m not in cache]
        
        # Return cached results for already looked up MMSIs
        for mmsi in mmsi_list:
            if mmsi in cache:
                results[mmsi] = cache[mmsi]
        
        if not uncached_mmsis:
            return results
        
        st.info(f"🔍 Looking up IMO for {len(uncached_mmsis)} vessels via MMSI (Ships API)...")
        
        # Look up each MMSI (with rate limiting)
        progress_bar = st.progress(0)
        for i, mmsi in enumerate(uncached_mmsis):
            imo = self.get_imo_by_mmsi(mmsi)
            
            if imo:
                cache[mmsi] = imo
                results[mmsi] = imo
            else:
                cache[mmsi] = None  # Cache the miss too to avoid re-lookup
            
            progress_bar.progress((i + 1) / len(uncached_mmsis))
            time.sleep(0.1)  # Rate limiting (10 req/sec max)
        
        progress_bar.empty()
        
        # Save cache to session state and disk
        st.session_state.mmsi_to_imo_cache = cache
        save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache, cache)
        
        found_count = len([v for v in results.values() if v])
        st.success(f"✅ Found IMO for {found_count}/{len(uncached_mmsis)} vessels")
        
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
            
            st.session_state.risk_data_cache = cache
            save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache, st.session_state.get('mmsi_to_imo_cache', {}))
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
            'cached_at': datetime.now(SGT).isoformat()
        }
        
        self.ships[mmsi]['static_data'] = static_info
        st.session_state.ship_static_cache[str(mmsi)] = static_info
        
        if time.time() - st.session_state.last_save > 60:
            save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache, st.session_state.get('mmsi_to_imo_cache', {}))
            st.session_state.last_save = time.time()
    
    def get_dataframe_with_compliance(self, sp_api: Optional[SPMaritimeAPI] = None, ships_api: Optional[SPShipsAPI] = None) -> pd.DataFrame:
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
        
        # Step 1: Get vessels WITH IMO from AIS static data
        valid_imos = [str(imo) for imo in df['imo'].unique() if imo and imo != '0']
        
        # Step 2: For vessels WITHOUT IMO, try to look up via MMSI using Ships API
        missing_imo_mask = (df['imo'] == '0') | (df['imo'] == '')
        missing_imo_mmsis = df.loc[missing_imo_mask, 'mmsi'].astype(str).unique().tolist()
        
        if missing_imo_mmsis and ships_api:
            # Look up IMO by MMSI
            mmsi_to_imo = ships_api.batch_get_imo_by_mmsi(missing_imo_mmsis)
            
            # Update dataframe with found IMOs
            for idx, row in df.iterrows():
                if str(row['mmsi']) in mmsi_to_imo and mmsi_to_imo[str(row['mmsi'])]:
                    found_imo = mmsi_to_imo[str(row['mmsi'])]
                    df.at[idx, 'imo'] = found_imo
                    if found_imo not in valid_imos:
                        valid_imos.append(found_imo)
        
        # Step 3: Get compliance data for all IMOs (from AIS + MMSI lookup)
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
        
        # Build tooltip text for vessel
        legal_emoji = '🔴' if row['legal_overall'] == 2 else ('🟡' if row['legal_overall'] == 1 else '✅')
        tooltip_text = f"<b>{row['name']}</b><br/>IMO: {row['imo']}<br/>MMSI: {row['mmsi']}<br/>Type: {row['type_name']}<br/>Status: {row['nav_status_name']}<br/>Speed: {row['speed']:.1f} kts<br/>Legal: {legal_emoji}<br/>Destination: {row['destination']}"
        
        vessel_polygons.append({
            'polygon': polygon,
            'name': row['name'],
            'tooltip': tooltip_text,
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
    default_sanctions = ["UN Sanctions", "OFAC Sanctions", "Dark Activity"]
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
compliance_options = ["All", "Severe (🔴)", "Warning (🟡)", "Clear (✅)"]
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
mmsi_imo_count = len(st.session_state.get('mmsi_to_imo_cache', {}))
mmsi_imo_found = len([v for v in st.session_state.get('mmsi_to_imo_cache', {}).values() if v])
st.sidebar.info(f"""
**Static Data Cache:** {len(st.session_state.ship_static_cache)} vessels

**Compliance Cache:** {len(st.session_state.risk_data_cache)} vessels

**MMSI→IMO Cache:** {mmsi_imo_found}/{mmsi_imo_count} found
""")

if st.sidebar.button("🗑️ Clear All Cache"):
    st.session_state.ship_static_cache = {}
    st.session_state.risk_data_cache = {}
    st.session_state.mmsi_to_imo_cache = {}
    save_cache({}, {}, {})
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
    
    # Compliance filters (skip if "All" is selected)
    if "All" not in selected_compliance and selected_compliance:
        compliance_map = {
            "Severe (🔴)": 2,
            "Warning (🟡)": 1,
            "Clear (✅)": 0
        }
        selected_levels = [compliance_map[c] for c in selected_compliance if c in compliance_map]
        if selected_levels:
            filtered_df = filtered_df[filtered_df['legal_overall'].isin(selected_levels)]
    
    # Sanctions & Dark Activity filter (skip if "All" is selected)
    if "All" not in selected_sanctions and selected_sanctions:
        sanction_mask = pd.Series([False] * len(filtered_df), index=filtered_df.index)
        if "UN Sanctions" in selected_sanctions:
            sanction_mask = sanction_mask | (filtered_df['un_sanction'] == 2)
        if "OFAC Sanctions" in selected_sanctions:
            sanction_mask = sanction_mask | (filtered_df['ofac_sanction'] == 2)
        if "Dark Activity" in selected_sanctions:
            sanction_mask = sanction_mask | (filtered_df['dark_activity'] >= 1)  # Include both warning (1) and severe (2)
        filtered_df = filtered_df[sanction_mask]
    
    # Vessel type filter (skip if "All" is selected)
    if "All" not in selected_types and selected_types:
        filtered_df = filtered_df[filtered_df['type_name'].isin(selected_types)]
    
    # Navigation status filter (skip if "All" is selected)
    if "All" not in selected_nav_statuses and selected_nav_statuses:
        filtered_df = filtered_df[filtered_df['nav_status_name'].isin(selected_nav_statuses)]
    
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
    
    # Initialize APIs
    sp_api = None
    ships_api = None
    if enable_compliance and sp_username and sp_password:
        sp_api = SPMaritimeAPI(sp_username, sp_password)
        ships_api = SPShipsAPI(sp_username, sp_password)  # Same credentials for Ships API
    
    # Collect AIS data
    with status_placeholder:
        with st.spinner(f'🔄 Collecting AIS data for {duration} seconds ({selected_coverage})...'):
            tracker = AISTracker()
            if ais_api_key:
                asyncio.run(tracker.collect_data(duration, ais_api_key, coverage_bbox))
            else:
                st.warning("⚠️ No AISStream API key provided. Please add it to secrets.")
                return
            
            df = tracker.get_dataframe_with_compliance(sp_api, ships_api)
    
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
        center_lat = 1.5
        center_lon = 104.0
        zoom = 8
    
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
            st.session_state.map_center = {"lat": 1.5, "lon": 104.0, "zoom": 8}
            st.rerun()
    
    with table_placeholder:
        # Sort by legal_overall (most severe first) then by speed
        display_df = df.sort_values(['legal_overall', 'speed'], ascending=[False, False]).copy()
        
        # Format boolean/status columns with emojis
        display_df['has_static_display'] = display_df['has_static'].map({True: '✅', False: '❌'})
        display_df['legal_display'] = display_df['legal_overall'].apply(format_compliance_value)
        display_df['un_display'] = display_df['un_sanction'].apply(format_compliance_value)
        display_df['ofac_display'] = display_df['ofac_sanction'].apply(format_compliance_value)
        display_df['dark_display'] = display_df['dark_activity'].apply(lambda x: '🌑' if x == 2 else ('⚠️' if x == 1 else '✅'))
        display_df['speed_fmt'] = display_df['speed'].apply(lambda x: f"{x:.1f}")
        
        # Select and rename columns for display - IMO first, then MMSI
        table_df = display_df[['name', 'imo', 'mmsi', 'type_name', 'nav_status_name', 'speed_fmt', 'destination', 'has_static_display', 'legal_display', 'un_display', 'ofac_display', 'dark_display']].copy()
        table_df.columns = ['Name', 'IMO', 'MMSI', 'Type', 'Nav Status', 'Speed', 'Destination', 'Static', 'Legal', 'UN', 'OFAC', 'Dark']
        
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
            
            col1, col2 = st.columns([4, 1])
            col1.info(f"Selected: **{table_df.iloc[selected_idx]['Name']}** (IMO: {table_df.iloc[selected_idx]['IMO']}, MMSI: {table_df.iloc[selected_idx]['MMSI']})")
            if col2.button("🗺️ View on Map"):
                st.session_state.selected_vessel = selected_mmsi
                st.rerun()
    
    # Save cache
    save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache, st.session_state.get('mmsi_to_imo_cache', {}))
    
    # Display timestamp in Singapore time (GMT+8)
    sgt_now = datetime.now(SGT)
    st.success(f"✅ Last updated: {sgt_now.strftime('%Y-%m-%d %H:%M:%S')} SGT (GMT+8)")


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
