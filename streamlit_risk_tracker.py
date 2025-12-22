"""
Singapore AIS Tracker with S&P Maritime Risk Intelligence
Real-time vessel tracking with compliance and risk indicators
Enhanced with persistent storage, vessel polygons, and maritime zones
v8.1 - Standardized legend, all compliance indicators, improved UI
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

# ============= STANDARDIZED LEGEND =============
# 🔴 Severe (2)
# 🟡 Warning (1)
# 🟢 Ok (0)
# ❓ Not checked / No IMO (-1)
# ===============================================

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

# API Compliance Fields Mapping (in PDF order)
# Only fields actually returned by the S&P API
API_COMPLIANCE_FIELDS = {
    # Field key: (API field name, Table header, Description)
    'legal_overall': ('legalOverall', 'Legal', 'Overall compliance status'),
    # Ship Sanctions (PDF Page 1)
    'ship_bes_sanction': ('shipBESSanctionList', 'UK', 'HM Treasury OFSI (UK)'),
    'ship_eu_sanction': ('shipEUSanctionList', 'EU', 'EU Sanction List'),
    'ship_ofac_sanction': ('shipOFACSanctionList', 'OFAC', 'US Treasury OFAC SDN'),
    'ship_ofac_non_sdn': ('shipOFACNonSDNSanctionList', 'OFAC-NS', 'OFAC Non-SDN'),
    'ship_swiss_sanction': ('shipSwissSanctionList', 'Swiss', 'Swiss Sanction List'),
    'ship_un_sanction': ('shipUNSanctionList', 'UN', 'UN Security Council'),
    # Page 2
    'ship_ofac_advisory': ('shipOFACAdvisoryList', 'OFACAdv', 'OFAC Advisory'),
    'port_call_3m': ('shipSanctionedCountryPortCallLast3m', 'Port3m', 'Sanctioned port call 3m'),
    'port_call_6m': ('shipSanctionedCountryPortCallLast6m', 'Port6m', 'Sanctioned port call 6m'),
    'port_call_12m': ('shipSanctionedCountryPortCallLast12m', 'Port12m', 'Sanctioned port call 12m'),
    'dark_activity': ('shipDarkActivityIndicator', 'Dark', 'Dark activity indicator'),
    # Page 3
    'sts_partner_non_compliance': ('shipSTSPartnerNonComplianceLast12m', 'STS', 'STS partner non-compliance'),
    'flag_disputed': ('shipFlagDisputed', 'FlagDisp', 'Flag disputed'),
    'flag_sanctioned': ('shipFlagSanctionedCountry', 'FlagSanc', 'Flag sanctioned country'),
    'flag_historical_sanctioned': ('shipHistoricalFlagSanctionedCountry', 'FlagHist', 'Historical flag sanctioned'),
    'security_legal_dispute': ('shipSecurityLegalDisputeEvent', 'SecEvt', 'Security/legal dispute'),
    'details_no_longer_maintained': ('shipDetailsNoLongerMaintained', 'NoMaint', 'Details no longer maintained'),
    # Owner Sanctions (Pages 3-5)
    'owner_australian': ('shipOwnerAustralianSanctionList', 'OwnAU', 'Owner Australian'),
    'owner_bes': ('shipOwnerBESSanctionList', 'OwnUK', 'Owner UK'),
    'owner_canadian': ('shipOwnerCanadianSanctionList', 'OwnCA', 'Owner Canadian'),
    'owner_eu': ('shipOwnerEUSanctionList', 'OwnEU', 'Owner EU'),
    'owner_fatf': ('shipOwnerFATFJurisdiction', 'OwnFATF', 'Owner FATF'),
    'owner_ofac_ssi': ('shipOwnerOFACSSIList', 'OwnSSI', 'Owner OFAC SSI'),
    'owner_ofac': ('shipOwnerOFACSanctionList', 'OwnOFAC', 'Owner OFAC'),
    'owner_swiss': ('shipOwnerSwissSanctionList', 'OwnSwiss', 'Owner Swiss'),
    'owner_uae': ('shipOwnerUAESanctionList', 'OwnUAE', 'Owner UAE'),
    'owner_un': ('shipOwnerUNSanctionList', 'OwnUN', 'Owner UN'),
    'owner_ofac_country': ('shipOwnerOFACSanctionedCountry', 'OwnOFACCty', 'Owner OFAC country'),
    'owner_historical_ofac_country': ('shipOwnerHistoricalOFACSanctionedCountry', 'OwnHistOFAC', 'Owner historical OFAC'),
    'owner_parent_non_compliance': ('shipOwnerParentCompanyNonCompliance', 'ParentNC', 'Parent non-compliance'),
    'owner_parent_ofac_country': ('shipOwnerParentOFACSanctionedCountry', 'ParentOFAC', 'Parent OFAC country'),
    'owner_parent_fatf': ('shipOwnerParentFATFJurisdiction', 'ParentFATF', 'Parent FATF'),
}


def format_datetime(dt_string: str) -> str:
    """Format ISO datetime string to readable format: '20 Dec 2025, 11:54 PM'"""
    if not dt_string or dt_string == 'Unknown' or dt_string == 'Never':
        return dt_string if dt_string else 'Never'
    
    try:
        dt = datetime.fromisoformat(dt_string)
        return dt.strftime('%d %b %Y, %I:%M %p')
    except Exception:
        return dt_string


def format_compliance_value(val) -> str:
    """Format compliance values with standardized emoji legend
    🔴 Severe (2)
    🟡 Warning (1)
    🟢 Ok (0)
    ❓ Not checked / No IMO (-1)
    """
    if val is None or val == -1 or val == '-1':
        return "❓"  # Not checked
    elif val == 2 or val == '2':
        return "🔴"  # Severe
    elif val == 1 or val == '1':
        return "🟡"  # Warning
    elif val == 0 or val == '0':
        return "🟢"  # Ok (green dot, not tick)
    else:
        return "❓"


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
    return "Other"


def get_ship_color(legal_overall: int = -1) -> List[int]:
    """Return color based on legal overall compliance status"""
    if legal_overall == 2:
        return [220, 53, 69, 200]  # Red - Severe
    elif legal_overall == 1:
        return [255, 193, 7, 200]  # Yellow - Warning
    elif legal_overall == 0:
        return [40, 167, 69, 200]  # Green - Ok
    else:
        return [128, 128, 128, 200]  # Gray - Not checked


def load_cache() -> Tuple[Dict, Dict, Dict, Dict]:
    """Load cached ship, risk, MMSI-to-IMO, and vessel position data from disk"""
    ship_cache, risk_cache, mmsi_imo_cache, vessel_positions = {}, {}, {}, {}
    
    for filepath, cache_ref in [(STORAGE_FILE, 'ship'), (RISK_DATA_FILE, 'risk'),
                                 (MMSI_IMO_CACHE_FILE, 'mmsi'), (VESSEL_POSITION_FILE, 'vessel')]:
        if os.path.exists(filepath):
            try:
                with open(filepath, 'rb') as f:
                    data = pickle.load(f)
                    if cache_ref == 'ship': ship_cache = data
                    elif cache_ref == 'risk': risk_cache = data
                    elif cache_ref == 'mmsi': mmsi_imo_cache = data
                    elif cache_ref == 'vessel': vessel_positions = data
            except: pass
    
    return ship_cache, risk_cache, mmsi_imo_cache, vessel_positions


def save_cache(ship_cache: Dict, risk_cache: Dict, mmsi_imo_cache: Dict = None, vessel_positions: Dict = None):
    """Save all caches to disk"""
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
            name_col = None
            for col in df.columns:
                if 'Name' in col:
                    name_col = col
                    break
            
            if name_col is None:
                continue
            
            for zone_name in df[name_col].unique():
                zone_df = df[df[name_col] == zone_name]
                
                if 'Decimal Latitude' in zone_df.columns and 'Decimal Longitude' in zone_df.columns:
                    coords = []
                    for _, row in zone_df.iterrows():
                        lat, lon = row['Decimal Latitude'], row['Decimal Longitude']
                        if pd.notna(lat) and pd.notna(lon):
                            coords.append([float(lon), float(lat)])
                    
                    if len(coords) >= 3:
                        if coords[0] != coords[-1]:
                            coords.append(coords[0])
                        zones[sheet_name].append({"name": zone_name, "polygon": coords})
        
        return zones
    except Exception as e:
        return zones


def create_vessel_polygon(lat: float, lon: float, heading: float, 
                         length: float = 60, width: float = 16,
                         scale_factor: float = 3.0) -> List[List[float]]:
    """Create a vessel-shaped polygon with pointed bow"""
    if lat is None or lon is None:
        return [[lon or 0, lat or 0]] * 7
    
    if heading is None or heading < 0 or heading >= 360:
        heading = 0
    
    heading_rad = math.radians(-heading)
    
    if length <= 0 or length > 500:
        length = 50
    if width <= 0 or width > 80:
        width = 10
    
    meters_per_deg_lat = 111320.0
    meters_per_deg_lon = 111320.0 * math.cos(math.radians(lat))
    
    scaled_length = length * scale_factor
    scaled_width = width * scale_factor
    
    half_length = scaled_length / 2.0 / meters_per_deg_lat
    half_width = scaled_width / 2.0 / meters_per_deg_lon
    
    bow_point = half_length
    bow_start = half_length * 0.5
    
    corners_local = [
        (-half_width, -half_length),
        (-half_width, bow_start),
        (0, bow_point),
        (half_width, bow_start),
        (half_width, -half_length),
        (-half_width, -half_length),
    ]
    
    cos_h = math.cos(heading_rad)
    sin_h = math.sin(heading_rad)
    
    rotated_corners = []
    for d_lon, d_lat in corners_local:
        rotated_lon = d_lon * cos_h - d_lat * sin_h
        rotated_lat = d_lon * sin_h + d_lat * cos_h
        final_lon = lon + rotated_lon
        final_lat = lat + rotated_lat
        rotated_corners.append([final_lon, final_lat])
    
    return rotated_corners


# Initialize session state
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
    """S&P Ships API for MMSI to IMO lookup"""
    
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_url = "https://shipsapi.maritime.spglobal.com/MaritimeWCF/APSShipService.svc/RESTFul"
    
    def get_imo_by_mmsi(self, mmsi: str) -> Optional[str]:
        """Look up IMO number from MMSI"""
        try:
            url = f"{self.base_url}/GetShipDataByMMSI?MMSI={mmsi}"
            response = requests.get(url, auth=(self.username, self.password), timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('shipCount', 0) == 0:
                    return None
                
                if 'APSShipDetail' in data and data['APSShipDetail']:
                    detail = data['APSShipDetail']
                    imo = detail.get('IHSLRorIMOShipNo')
                    if imo and str(imo) != '0' and str(imo) != '':
                        return str(imo)
            return None
        except:
            return None
    
    def batch_get_imo_by_mmsi(self, mmsi_list: List[str]) -> Dict[str, str]:
        """Look up IMO numbers for multiple MMSIs"""
        results = {}
        
        if 'mmsi_to_imo_cache' not in st.session_state:
            st.session_state.mmsi_to_imo_cache = {}
        
        cache = st.session_state.mmsi_to_imo_cache
        uncached_mmsis = [m for m in mmsi_list if m not in cache or cache.get(m) is None]
        
        for mmsi in mmsi_list:
            if mmsi in cache and cache[mmsi] is not None:
                results[mmsi] = cache[mmsi]
        
        if not uncached_mmsis:
            return results
        
        st.info(f"🔍 Looking up IMO for {len(uncached_mmsis)} vessels...")
        progress_bar = st.progress(0)
        
        for i, mmsi in enumerate(uncached_mmsis):
            imo = self.get_imo_by_mmsi(mmsi)
            if imo:
                cache[mmsi] = imo
                results[mmsi] = imo
            progress_bar.progress((i + 1) / len(uncached_mmsis))
            time.sleep(0.1)
        
        progress_bar.empty()
        st.session_state.mmsi_to_imo_cache = cache
        save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache, cache)
        
        return results


class SPMaritimeAPI:
    """S&P Maritime API for compliance screening"""
    
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_url = "https://webservices.maritime.spglobal.com/RiskAndCompliance/CompliancesByImos"
    
    def get_ship_compliance_data(self, imo_numbers: List[str]) -> Dict[str, Dict]:
        """Get compliance indicators for multiple IMO numbers"""
        if not imo_numbers:
            return {}
        
        cache = st.session_state.risk_data_cache
        uncached_imos = [imo for imo in imo_numbers if imo not in cache]
        
        if not uncached_imos:
            return {imo: cache[imo] for imo in imo_numbers}
        
        st.info(f"🔍 Fetching compliance data for {len(uncached_imos)} vessels...")
        
        try:
            batches = [uncached_imos[i:i+100] for i in range(0, len(uncached_imos), 100)]
            received_imos = set()
            
            for batch in batches:
                imo_string = ','.join(batch)
                url = f"{self.base_url}?imos={imo_string}"
                
                response = requests.get(url, auth=(self.username, self.password), timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if isinstance(data, list):
                        for ship in data:
                            imo = str(ship.get('lrimoShipNo', ''))
                            if not imo:
                                continue
                            
                            received_imos.add(imo)
                            
                            # Extract ALL fields from API response
                            cache_entry = {'cached_at': datetime.now(SGT).isoformat()}
                            for field_key, (api_field, _, _) in API_COMPLIANCE_FIELDS.items():
                                val = ship.get(api_field)
                                if val is not None:
                                    cache_entry[field_key] = val
                            
                            cache[imo] = cache_entry
                
                time.sleep(0.3)
            
            # Mark checked but not found
            for imo in uncached_imos:
                if imo not in received_imos:
                    cache[imo] = {
                        'legal_overall': 0,
                        'checked_but_not_found': True,
                        'cached_at': datetime.now(SGT).isoformat()
                    }
            
            st.session_state.risk_data_cache = cache
            save_cache(st.session_state.ship_static_cache, cache, st.session_state.get('mmsi_to_imo_cache', {}))
            
        except Exception as e:
            st.error(f"⚠️ S&P API error: {str(e)}")
        
        return {imo: cache.get(imo, {}) for imo in imo_numbers}


class AISTracker:
    """AIS data collection and vessel tracking"""
    
    def __init__(self, use_cached_positions: bool = True):
        if use_cached_positions and 'vessel_positions' in st.session_state:
            self.ships = defaultdict(lambda: {'latest_position': None, 'static_data': None})
            cached = st.session_state.vessel_positions
            for mmsi, data in cached.items():
                if mmsi != '_last_update':
                    self.ships[mmsi] = data
        else:
            self.ships = defaultdict(lambda: {'latest_position': None, 'static_data': None})
    
    def save_positions_to_cache(self):
        """Save current vessel positions to session state and disk"""
        positions_dict = dict(self.ships)
        positions_dict['_last_update'] = datetime.now(SGT).isoformat()
        
        st.session_state.vessel_positions = positions_dict
        st.session_state.last_data_update = positions_dict['_last_update']
        
        save_cache(
            st.session_state.ship_static_cache,
            st.session_state.risk_data_cache,
            st.session_state.get('mmsi_to_imo_cache', {}),
            positions_dict
        )
    
    async def collect_data(self, duration: int = 30, api_key: str = "", bounding_box: List = None):
        """Collect AIS data from AISStream.io"""
        if bounding_box is None:
            bounding_box = [[[0.5, 102.0], [2.5, 106.0]]]
        
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
        self.ships[mmsi]['last_seen'] = datetime.now(SGT).isoformat()
    
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
        
        existing_cached = st.session_state.ship_static_cache.get(str(mmsi), {})
        
        if dim_a == 0 and dim_b == 0:
            dim_a = existing_cached.get('dimension_a', 0) or 0
            dim_b = existing_cached.get('dimension_b', 0) or 0
        if dim_c == 0 and dim_d == 0:
            dim_c = existing_cached.get('dimension_c', 0) or 0
            dim_d = existing_cached.get('dimension_d', 0) or 0
        
        if imo == '0' and existing_cached.get('imo', '0') != '0':
            imo = existing_cached.get('imo')
        
        static_info = {
            'name': static_data.get('Name', 'Unknown') or existing_cached.get('name', 'Unknown'),
            'imo': imo,
            'type': static_data.get('Type') or existing_cached.get('type'),
            'dimension_a': dim_a, 'dimension_b': dim_b, 'dimension_c': dim_c, 'dimension_d': dim_d,
            'length': dim_a + dim_b, 'width': dim_c + dim_d,
            'destination': static_data.get('Destination', 'Unknown') or existing_cached.get('destination', 'Unknown'),
            'call_sign': static_data.get('CallSign', '') or existing_cached.get('call_sign', ''),
            'cached_at': datetime.now(SGT).isoformat()
        }
        
        self.ships[mmsi]['static_data'] = static_info
        st.session_state.ship_static_cache[str(mmsi)] = static_info
    
    def get_dataframe_with_compliance(self, sp_api: Optional[SPMaritimeAPI] = None, 
                                      ships_api: Optional[SPShipsAPI] = None, 
                                      expiry_hours: Optional[int] = None) -> pd.DataFrame:
        """Get dataframe with all compliance indicators"""
        data = []
        now = datetime.now(SGT)
        
        for mmsi, ship_data in self.ships.items():
            pos = ship_data.get('latest_position')
            
            # Check expiry
            if expiry_hours is not None:
                last_seen_str = ship_data.get('last_seen')
                if last_seen_str:
                    try:
                        last_seen = datetime.fromisoformat(last_seen_str)
                        hours_since_seen = (now - last_seen).total_seconds() / 3600
                        if hours_since_seen > expiry_hours:
                            continue
                    except: pass
            
            static = ship_data.get('static_data')
            if not static:
                static = st.session_state.ship_static_cache.get(str(mmsi), {})
            
            if not pos or pos.get('latitude') is None or pos.get('longitude') is None:
                continue
            
            name = static.get('name') or pos.get('ship_name') or 'Unknown'
            name = name.strip() if name else 'Unknown'
            
            true_heading = pos.get('true_heading', 511)
            heading = pos.get('cog', 0) if true_heading == 511 else true_heading
            
            length = static.get('length', 0) or 0
            width = static.get('width', 0) or 0
            has_real_dimensions = (length > 0 and width > 0)
            if not has_real_dimensions:
                length, width = 50, 10
            
            row = {
                'mmsi': mmsi,
                'name': name,
                'imo': str(static.get('imo', '0')),
                'latitude': pos.get('latitude'),
                'longitude': pos.get('longitude'),
                'speed': pos.get('sog', 0),
                'course': pos.get('cog', 0),
                'heading': heading,
                'nav_status': pos.get('nav_status', 15),
                'nav_status_name': NAV_STATUS_NAMES.get(pos.get('nav_status', 15), 'Unknown'),
                'type': static.get('type'),
                'type_name': get_vessel_type_category(static.get('type')),
                'length': length,
                'width': width,
                'has_dimensions': has_real_dimensions,
                'destination': (static.get('destination') or 'Unknown').strip(),
                'has_static': bool(static.get('name')),
                'last_seen': ship_data.get('last_seen', ''),
                'color': get_ship_color(-1)
            }
            
            # Initialize all compliance fields to -1 (not checked)
            for field_key in API_COMPLIANCE_FIELDS.keys():
                row[field_key] = -1
            
            data.append(row)
        
        df = pd.DataFrame(data)
        if len(df) == 0:
            return df
        
        # Get IMOs and compliance data
        valid_imos = [str(imo) for imo in df['imo'].unique() if imo and imo != '0']
        missing_imo_mask = (df['imo'] == '0') | (df['imo'] == '')
        missing_imo_mmsis = df.loc[missing_imo_mask, 'mmsi'].astype(str).unique().tolist()
        
        # MMSI -> IMO lookup
        if missing_imo_mmsis and ships_api:
            mmsi_to_imo = ships_api.batch_get_imo_by_mmsi(missing_imo_mmsis)
            for idx, row in df.iterrows():
                if str(row['mmsi']) in mmsi_to_imo:
                    found_imo = mmsi_to_imo[str(row['mmsi'])]
                    df.at[idx, 'imo'] = found_imo
                    if found_imo not in valid_imos:
                        valid_imos.append(found_imo)
        elif missing_imo_mmsis:
            cache = st.session_state.get('mmsi_to_imo_cache', {})
            for idx, row in df.iterrows():
                mmsi_str = str(row['mmsi'])
                if mmsi_str in cache and cache[mmsi_str]:
                    found_imo = cache[mmsi_str]
                    df.at[idx, 'imo'] = found_imo
                    if found_imo not in valid_imos:
                        valid_imos.append(found_imo)
        
        # Get compliance data
        compliance_cache = st.session_state.get('risk_data_cache', {})
        if valid_imos and sp_api:
            compliance_data = sp_api.get_ship_compliance_data(valid_imos)
        else:
            compliance_data = {imo: compliance_cache.get(imo, {}) for imo in valid_imos}
        
        # Apply compliance data to dataframe
        for idx, row in df.iterrows():
            imo = str(row['imo'])
            if imo in compliance_data and compliance_data[imo]:
                comp = compliance_data[imo]
                
                for field_key in API_COMPLIANCE_FIELDS.keys():
                    val = comp.get(field_key, -1)
                    if val is not None:
                        df.at[idx, field_key] = int(val) if val != -1 else -1
                
                legal = df.at[idx, 'legal_overall']
                df.at[idx, 'color'] = get_ship_color(legal)
        
        return df


def create_vessel_layers(df: pd.DataFrame, use_actual_shapes: bool = False) -> List[pdk.Layer]:
    """Create PyDeck layers for vessels - dots or actual shapes"""
    if len(df) == 0:
        return []
    
    vessel_data = []
    for _, row in df.iterrows():
        vessel_length = row['length'] if 0 < row['length'] < 500 else 50
        vessel_width = row['width'] if 0 < row['width'] < 80 else 10
        
        legal_emoji = format_compliance_value(row['legal_overall'])
        dim_text = f"{vessel_length:.0f}m x {vessel_width:.0f}m"
        if not row['has_dimensions']:
            dim_text += " (est.)"
        
        tooltip_text = (
            f"<b>{row['name']}</b><br/>"
            f"IMO: {row['imo']}<br/>"
            f"MMSI: {row['mmsi']}<br/>"
            f"Type: {row['type_name']}<br/>"
            f"Size: {dim_text}<br/>"
            f"Speed: {row['speed']:.1f} kts<br/>"
            f"Status: {row['nav_status_name']}<br/>"
            f"Compliance: {legal_emoji}"
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
        })
    
    if not use_actual_shapes:
        # Dot view
        return [pdk.Layer(
            'ScatterplotLayer',
            data=vessel_data,
            get_position=['longitude', 'latitude'],
            get_fill_color='color',
            get_radius=500,
            radius_min_pixels=4,
            radius_max_pixels=20,
            pickable=True,
        )]
    else:
        # Actual representation - ship shapes
        vessel_polygons = []
        for v in vessel_data:
            polygon = create_vessel_polygon(
                v['latitude'], v['longitude'], v['heading'],
                v['length'], v['width'], scale_factor=3.0
            )
            vessel_polygons.append({
                'polygon': polygon,
                'tooltip': v['tooltip'],
                'color': v['color']
            })
        
        return [pdk.Layer(
            'PolygonLayer',
            data=vessel_polygons,
            get_polygon='polygon',
            get_fill_color='color',
            get_line_color=[50, 50, 50, 100],
            line_width_min_pixels=1,
            pickable=True,
        )]


def create_zone_layer(zones: List[Dict], color: List[int], layer_id: str) -> pdk.Layer:
    """Create PyDeck polygon layer for maritime zones"""
    if not zones:
        return None
    
    zone_data = []
    for zone in zones:
        zone_data.append({
            'polygon': zone['polygon'],
            'name': zone['name'],
            'tooltip': f"<b>{zone['name']}</b>"
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
    )


# ============= STREAMLIT UI =============

st.title("🚢 Singapore Strait Ship Risk Tracker")
st.caption("Real-time vessel tracking with S&P Maritime compliance screening")

# Buttons and Last Updated - right after title
btn_col1, btn_col2, btn_col3, btn_col4 = st.columns([1, 1, 1, 2])

# Sidebar Configuration
st.sidebar.header("⚙️ Configuration")

# Try to load credentials from secrets
try:
    sp_username = st.secrets["sp_maritime"]["username"]
    sp_password = st.secrets["sp_maritime"]["password"]
    ais_api_key = st.secrets.get("aisstream", {}).get("api_key", "")
    st.sidebar.success("🔐 Credentials loaded")
except Exception:
    with st.sidebar.expander("🔐 API Credentials", expanded=False):
        sp_username = st.text_input("S&P Username", type="password")
        sp_password = st.text_input("S&P Password", type="password")
        ais_api_key = st.text_input("AISStream API Key", type="password")

# AIS Settings
st.sidebar.header("📡 AIS Settings")
duration = st.sidebar.slider("Collection time (seconds)", 10, 120, 60)
enable_compliance = st.sidebar.checkbox("Enable S&P compliance", value=True)

# Coverage Area
coverage_options = {
    "Singapore Strait Only": [[[1.15, 103.55], [1.50, 104.10]]],
    "Singapore + Approaches": [[[1.0, 103.3], [1.6, 104.3]]],
    "Malacca to SCS": [[[0.5, 102.0], [2.5, 106.0]]],
    "Extended Malacca": [[[-0.5, 100.0], [3.0, 106.0]]],
    "Full Regional": [[[-1.0, 99.0], [4.0, 108.0]]]
}
selected_coverage = st.sidebar.selectbox("Coverage area", list(coverage_options.keys()), index=2)
coverage_bbox = coverage_options[selected_coverage]

# Vessel Expiry
expiry_options = {"1 hour": 1, "2 hours": 2, "4 hours": 4, "8 hours": 8, "12 hours": 12, "24 hours": 24, "Never": None}
vessel_expiry_hours = expiry_options[st.sidebar.selectbox("Vessel expiry", list(expiry_options.keys()), index=2)]

# Maritime Zones
st.sidebar.header("🗺️ Maritime Zones")
show_anchorages = st.sidebar.checkbox("Anchorages", value=True)
show_channels = st.sidebar.checkbox("Channels", value=True)
show_fairways = st.sidebar.checkbox("Fairways", value=True)

# Map View - 2 zoom options only
st.sidebar.header("🔍 Map View")
zoom_mode = st.sidebar.radio("Vessel display", ["Dot", "Actual"], horizontal=True)
use_actual_shapes = (zoom_mode == "Actual")

# Load maritime zones
maritime_zones = {"Anchorages": [], "Channels": [], "Fairways": []}
excel_paths = [
    "/mnt/project/Anchorages_Channels_Fairways_Details.xlsx",
    "/mnt/user-data/uploads/Anchorages_Channels_Fairways_Details.xlsx",
    "Anchorages_Channels_Fairways_Details.xlsx"
]
for path in excel_paths:
    if os.path.exists(path):
        maritime_zones = load_maritime_zones(path)
        break

# Filters with renamed presets
st.sidebar.header("🔍 Filters")

# Preset options - renamed
quick_filter = st.sidebar.radio("Preset", ["All Vessels", "Dark Fleet", "Sanctioned"], horizontal=True)

# Set defaults based on preset
if quick_filter == "Dark Fleet":
    # Legal: severe or warning, Dark activity, Tanker or Cargo
    default_compliance = ["Severe (🔴)", "Warning (🟡)"]
    default_sanctions = ["Dark Activity"]
    default_types = ["Tanker", "Cargo"]
elif quick_filter == "Sanctioned":
    # Legal: severe, UN or OFAC sanctions
    default_compliance = ["Severe (🔴)"]
    default_sanctions = ["UN Sanctions", "OFAC Sanctions"]
    default_types = ["All"]
else:
    default_compliance = ["All"]
    default_sanctions = ["All"]
    default_types = ["All"]

# Compliance filters
compliance_options = ["All", "Severe (🔴)", "Warning (🟡)", "Ok (🟢)"]
selected_compliance = st.sidebar.multiselect("Legal Status", compliance_options, default=default_compliance)

sanction_options = ["All", "UN Sanctions", "OFAC Sanctions", "Dark Activity"]
selected_sanctions = st.sidebar.multiselect("Sanctions & Dark Activity", sanction_options, default=default_sanctions)

# Vessel type filter
vessel_types = ["All", "Cargo", "Tanker", "Passenger", "Tug", "Fishing", "High Speed Craft", "Pilot", "SAR", "Other", "Unknown"]
selected_types = st.sidebar.multiselect("Vessel Types", vessel_types, default=default_types)

# Navigation status filter
nav_status_options = ["All"] + list(NAV_STATUS_NAMES.values())
selected_nav_statuses = st.sidebar.multiselect("Nav Status", nav_status_options, default=["All"])

show_static_only = st.sidebar.checkbox("With static data only", value=False)

# Cache statistics
st.sidebar.header("💾 Cache")
vessel_count = len([k for k in st.session_state.get('vessel_positions', {}).keys() if k != '_last_update'])
st.sidebar.info(f"**Vessels:** {vessel_count} | **Compliance:** {len(st.session_state.risk_data_cache)}")

cache_col1, cache_col2 = st.sidebar.columns(2)
if cache_col1.button("🗑️ Clear"):
    st.session_state.ship_static_cache = {}
    st.session_state.risk_data_cache = {}
    st.session_state.mmsi_to_imo_cache = {}
    st.session_state.vessel_positions = {}
    save_cache({}, {}, {}, {})
    st.rerun()
if cache_col2.button("🔄 Retry IMO"):
    st.session_state.mmsi_to_imo_cache = {}
    st.rerun()

# Standardized Legend
st.sidebar.markdown("---")
st.sidebar.markdown("""
### 🎨 Legend
**Compliance Status:**
- 🔴 Severe (2)
- 🟡 Warning (1)
- 🟢 Ok (0)
- ❓ Not checked (No IMO)

**Zones:** 🔵 Anchorages | 🟡 Channels | 🟠 Fairways
""")

# Main content placeholders
status_placeholder = st.empty()
stats_placeholder = st.empty()
map_placeholder = st.empty()
table_placeholder = st.empty()


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all filters to the dataframe"""
    if len(df) == 0:
        return df
    
    filtered_df = df.copy()
    
    # Compliance filter
    if selected_compliance and "All" not in selected_compliance:
        compliance_map = {"Severe (🔴)": 2, "Warning (🟡)": 1, "Ok (🟢)": 0}
        selected_levels = [compliance_map[c] for c in selected_compliance if c in compliance_map]
        if selected_levels:
            filtered_df = filtered_df[filtered_df['legal_overall'].isin(selected_levels)]
    
    # Sanctions filter
    if selected_sanctions and "All" not in selected_sanctions:
        sanction_mask = pd.Series([False] * len(filtered_df), index=filtered_df.index)
        if "UN Sanctions" in selected_sanctions:
            sanction_mask = sanction_mask | (filtered_df['ship_un_sanction'] == 2)
        if "OFAC Sanctions" in selected_sanctions:
            sanction_mask = sanction_mask | (filtered_df['ship_ofac_sanction'] == 2)
        if "Dark Activity" in selected_sanctions:
            sanction_mask = sanction_mask | (filtered_df['dark_activity'] >= 1)
        filtered_df = filtered_df[sanction_mask]
    
    # Type filter
    if selected_types and "All" not in selected_types:
        filtered_df = filtered_df[filtered_df['type_name'].isin(selected_types)]
    
    # Nav status filter
    if selected_nav_statuses and "All" not in selected_nav_statuses:
        filtered_df = filtered_df[filtered_df['nav_status_name'].isin(selected_nav_statuses)]
    
    # Static data filter
    if show_static_only:
        filtered_df = filtered_df[filtered_df['has_static'] == True]
    
    return filtered_df


def display_vessel_data(df: pd.DataFrame, last_update: str, is_cached: bool = False):
    """Display map and table"""
    
    # Statistics
    with stats_placeholder:
        cols = st.columns(8)
        cols[0].metric("🚢 Total", len(df))
        cols[1].metric("⚡ Moving", len(df[df['speed'] > 1]) if len(df) else 0)
        cols[2].metric("📡 Static", int(df['has_static'].sum()) if len(df) else 0)
        cols[3].metric("🔴 Severe", len(df[df['legal_overall'] == 2]) if len(df) else 0)
        cols[4].metric("🟡 Warning", len(df[df['legal_overall'] == 1]) if len(df) else 0)
        cols[5].metric("🟢 Ok", len(df[df['legal_overall'] == 0]) if len(df) else 0)
        cols[6].metric("❓ Unknown", len(df[df['legal_overall'] < 0]) if len(df) else 0)
        cols[7].metric("📐 Dims", int(df['has_dimensions'].sum()) if len(df) else 0)
    
    # Map - initial zoom level 3
    center_lat, center_lon = 1.28, 103.85
    map_zoom = 3  # Initial zoom level
    
    if st.session_state.selected_vessel and len(df):
        vessel = df[df['mmsi'] == st.session_state.selected_vessel]
        if len(vessel) > 0:
            center_lat = vessel.iloc[0]['latitude']
            center_lon = vessel.iloc[0]['longitude']
            map_zoom = 12
    
    # Create layers
    layers = []
    
    if show_anchorages and maritime_zones['Anchorages']:
        layer = create_zone_layer(maritime_zones['Anchorages'], [0, 255, 255, 50], "anchorages")
        if layer: layers.append(layer)
    
    if show_channels and maritime_zones['Channels']:
        layer = create_zone_layer(maritime_zones['Channels'], [255, 255, 0, 50], "channels")
        if layer: layers.append(layer)
    
    if show_fairways and maritime_zones['Fairways']:
        layer = create_zone_layer(maritime_zones['Fairways'], [255, 165, 0, 50], "fairways")
        if layer: layers.append(layer)
    
    vessel_layers = create_vessel_layers(df, use_actual_shapes)
    layers.extend(vessel_layers)
    
    with map_placeholder:
        st.pydeck_chart(pdk.Deck(
            map_style='mapbox://styles/mapbox/dark-v10',
            initial_view_state=pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=map_zoom),
            layers=layers,
            tooltip={'html': '{tooltip}', 'style': {'backgroundColor': 'steelblue', 'color': 'white'}}
        ), use_container_width=True)
    
    # Table
    with table_placeholder:
        if len(df) == 0:
            st.info("No vessels to display. Adjust filters or refresh data.")
        else:
            # Sort: Not checked (-1) → Ok (0) → Warning (1) → Severe (2) for ascending
            # We want Severe first, so use descending
            def compliance_sort_key(val):
                if val == 2: return 3
                elif val == 1: return 2
                elif val == -1: return 1
                else: return 0
            
            display_df = df.copy()
            display_df['_sort_key'] = display_df['legal_overall'].apply(compliance_sort_key)
            display_df = display_df.sort_values(['_sort_key', 'name'], ascending=[False, True])
            display_df = display_df.drop(columns=['_sort_key'])
            
            # Build table columns - only include fields that have data
            table_cols = ['name', 'imo', 'mmsi', 'type_name', 'nav_status_name']
            col_names = ['Name', 'IMO', 'MMSI', 'Type', 'Nav Status']
            
            # Add compliance columns in PDF order - only if they have non -1 values
            for field_key, (_, header, _) in API_COMPLIANCE_FIELDS.items():
                if field_key in display_df.columns:
                    if (display_df[field_key] != -1).any():
                        display_df[f'{field_key}_fmt'] = display_df[field_key].apply(format_compliance_value)
                        table_cols.append(f'{field_key}_fmt')
                        col_names.append(header)
            
            table_df = display_df[table_cols].copy()
            table_df.columns = col_names
            
            selected_rows = st.dataframe(
                table_df,
                use_container_width=True,
                height=400,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row"
            )
            
            if selected_rows and selected_rows.selection and selected_rows.selection.rows:
                selected_idx = selected_rows.selection.rows[0]
                selected_mmsi = display_df.iloc[selected_idx]['mmsi']
                
                st.info(f"Selected: **{table_df.iloc[selected_idx]['Name']}** (IMO: {table_df.iloc[selected_idx]['IMO']})")
                if st.button("🗺️ View on Map"):
                    st.session_state.selected_vessel = selected_mmsi
                    st.rerun()
    
    # Last updated
    cache_indicator = " 📦 (cached)" if is_cached else ""
    formatted_time = format_datetime(last_update) if last_update else datetime.now(SGT).strftime('%d %b %Y, %I:%M %p')
    st.success(f"✅ Last updated: {formatted_time} SGT{cache_indicator}")


def update_display():
    """Collect data and update display"""
    sp_api = None
    ships_api = None
    if enable_compliance and sp_username and sp_password:
        sp_api = SPMaritimeAPI(sp_username, sp_password)
        ships_api = SPShipsAPI(sp_username, sp_password)
    
    with status_placeholder:
        with st.spinner(f'🔄 Collecting AIS data for {duration} seconds...'):
            tracker = AISTracker(use_cached_positions=True)
            if ais_api_key:
                asyncio.run(tracker.collect_data(duration, ais_api_key, coverage_bbox))
            else:
                st.warning("⚠️ No AISStream API key")
                return
            
            df = tracker.get_dataframe_with_compliance(sp_api, ships_api, expiry_hours=vessel_expiry_hours)
    
    status_placeholder.empty()
    
    if df.empty:
        st.warning("⚠️ No ships detected.")
        return
    
    df = apply_filters(df)
    last_update = st.session_state.get('last_data_update', datetime.now(SGT).isoformat())
    display_vessel_data(df, last_update, is_cached=False)


def display_cached_data():
    """Display cached vessel data"""
    if 'vessel_positions' not in st.session_state or not st.session_state.vessel_positions:
        st.info("ℹ️ No cached data. Click 'Refresh Now' first.")
        return
    
    tracker = AISTracker(use_cached_positions=True)
    df = tracker.get_dataframe_with_compliance(sp_api=None, ships_api=None, expiry_hours=vessel_expiry_hours)
    
    if df.empty:
        st.info("ℹ️ No cached vessel data.")
        return
    
    df = apply_filters(df)
    
    if df.empty:
        st.warning("⚠️ No vessels match filters.")
        return
    
    last_update = st.session_state.vessel_positions.get('_last_update', '')
    display_vessel_data(df, last_update, is_cached=True)


# Action buttons in header row
with btn_col1:
    if st.button("🔄 Refresh Now", type="primary", use_container_width=True):
        update_display()
        st.session_state.data_loaded = True

with btn_col2:
    if st.button("📦 View Cached", use_container_width=True):
        display_cached_data()
        st.session_state.data_loaded = True

with btn_col3:
    if st.button("🔄 Reset View", use_container_width=True):
        st.session_state.selected_vessel = None
        st.rerun()

with btn_col4:
    last_update_time = st.session_state.get('last_data_update', 'Never')
    if last_update_time and last_update_time != 'Never':
        st.caption(f"📅 Last Updated: {format_datetime(last_update_time)} SGT")
    else:
        st.caption("📅 Last Updated: Never")

# Show cached data on page load
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

if not st.session_state.data_loaded:
    if 'vessel_positions' in st.session_state and st.session_state.vessel_positions:
        cached_count = len([k for k in st.session_state.vessel_positions.keys() if k != '_last_update'])
        if cached_count > 0:
            st.info(f"📦 Found {cached_count} cached vessels. Click 'View Cached' to display, or 'Refresh Now' for fresh data.")
        else:
            st.info("👆 Click 'Refresh Now' to start collecting AIS data")
    else:
        st.info("👆 Click 'Refresh Now' to start collecting AIS data")
