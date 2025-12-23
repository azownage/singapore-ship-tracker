"""
Singapore AIS Tracker with S&P Maritime Risk Intelligence
Real-time vessel tracking with compliance and risk indicators
v9 - Streamlined: Display mode toggle, improved sorting, persistent map view
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
from typing import List, Dict, Optional, Tuple
import pickle
import os
import math

st.set_page_config(page_title="Singapore Ship Risk Tracker", page_icon="🚢", layout="wide")

# Constants
SGT = timezone(timedelta(hours=8))
STORAGE_FILE = "ship_data_cache.pkl"
RISK_DATA_FILE = "risk_data_cache.pkl"
MMSI_IMO_CACHE_FILE = "mmsi_imo_cache.pkl"
VESSEL_POSITION_FILE = "vessel_positions_cache.pkl"

# AIS vessel type codes
VESSEL_TYPE_NAMES = {
    0: "Not available", 20: "Wing in ground", 30: "Fishing", 31: "Towing", 32: "Towing - Large",
    33: "Dredging", 34: "Diving ops", 35: "Military ops", 36: "Sailing", 37: "Pleasure Craft",
    40: "High Speed Craft", 50: "Pilot Vessel", 51: "Search and Rescue", 52: "Tug",
    53: "Port Tender", 54: "Anti-pollution", 55: "Law Enforcement", 58: "Medical Transport",
    60: "Passenger", 70: "Cargo", 80: "Tanker", 90: "Other"
}

NAV_STATUS_NAMES = {
    0: "Under way using engine", 1: "At anchor", 2: "Not under command",
    3: "Restricted maneuverability", 4: "Constrained by draught", 5: "Moored",
    6: "Aground", 7: "Engaged in fishing", 8: "Under way sailing", 15: "Not defined"
}

# Helper Functions
def format_datetime(dt_string: str) -> str:
    """Format ISO datetime string to readable format"""
    if not dt_string or dt_string in ['Unknown', 'Never']:
        return dt_string or 'Never'
    try:
        return datetime.fromisoformat(dt_string).strftime('%d %b %Y, %I:%M %p')
    except:
        return dt_string

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
    return "Other"

def load_cache() -> Tuple[Dict, Dict, Dict, Dict]:
    """Load all cached data from disk"""
    caches = [{}, {}, {}, {}]
    files = [STORAGE_FILE, RISK_DATA_FILE, MMSI_IMO_CACHE_FILE, VESSEL_POSITION_FILE]
    for i, file in enumerate(files):
        if os.path.exists(file):
            try:
                with open(file, 'rb') as f:
                    caches[i] = pickle.load(f)
            except:
                pass
    return tuple(caches)

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
            name_col = next((col for col in df.columns if 'Name' in col), None)
            if name_col is None:
                continue
            for zone_name in df[name_col].unique():
                zone_df = df[df[name_col] == zone_name]
                if 'Decimal Latitude' in zone_df.columns and 'Decimal Longitude' in zone_df.columns:
                    coords = [[float(row['Decimal Longitude']), float(row['Decimal Latitude'])]
                             for _, row in zone_df.iterrows()
                             if pd.notna(row['Decimal Latitude']) and pd.notna(row['Decimal Longitude'])]
                    if len(coords) >= 3:
                        if coords[0] != coords[-1]:
                            coords.append(coords[0])
                        zones[sheet_name].append({"name": zone_name, "polygon": coords})
        return zones
    except Exception as e:
        st.warning(f"Could not load maritime zones: {e}")
        return zones

def create_vessel_polygon(lat: float, lon: float, heading: float, length: float = 60, 
                         width: float = 16, dim_a: float = 0, dim_b: float = 0,
                         dim_c: float = 0, dim_d: float = 0) -> List[List[float]]:
    """Create vessel-shaped polygon at actual scale"""
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
    
    offset_forward = ((dim_a - dim_b) / 2.0) if (dim_a > 0 or dim_b > 0) else 0
    offset_port = ((dim_c - dim_d) / 2.0) if (dim_c > 0 or dim_d > 0) else 0
    
    half_length = length / 2.0 / meters_per_deg_lat
    half_width = width / 2.0 / meters_per_deg_lon
    offset_fwd_deg = offset_forward / meters_per_deg_lat
    offset_port_deg = offset_port / meters_per_deg_lon
    
    bow_point = half_length
    bow_start = half_length * 0.5
    
    corners_local = [
        (-half_width, -half_length), (-half_width, bow_start), (0, bow_point),
        (half_width, bow_start), (half_width, -half_length), (-half_width, -half_length)
    ]
    
    cos_h, sin_h = math.cos(heading_rad), math.sin(heading_rad)
    rotated_corners = []
    for d_lon, d_lat in corners_local:
        d_lat_adj = d_lat - offset_fwd_deg
        d_lon_adj = d_lon + offset_port_deg
        rotated_lon = d_lon_adj * cos_h - d_lat_adj * sin_h
        rotated_lat = d_lon_adj * sin_h + d_lat_adj * cos_h
        rotated_corners.append([lon + rotated_lon, lat + rotated_lat])
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
    st.session_state.collection_in_progress = False

if 'selected_vessel' not in st.session_state:
    st.session_state.selected_vessel = None
if 'show_details_imo' not in st.session_state:
    st.session_state.show_details_imo = None
    st.session_state.show_details_name = None

# API Classes
class SPShipsComplianceAPI:
    """S&P Ships API for compliance data and ship details"""
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_url_imo = "https://shipsapi.maritime.spglobal.com/MaritimeWCF/APSShipService.svc/RESTFul/GetShipsByIHSLRorIMONumbersAll"
        self.base_url_mmsi = "https://shipsapi.maritime.spglobal.com/MaritimeWCF/APSShipService.svc/RESTFul/GetShipDataByMMSI"
    
    def get_ship_details_by_imo(self, imo: str) -> Optional[Dict]:
        """Get full ship details including dark activity by IMO"""
        try:
            url = f"{self.base_url_imo}?imoNumbers={imo}"
            response = requests.get(url, auth=(self.username, self.password), timeout=30)
            if response.status_code == 200:
                data = response.json()
                if 'ShipResult' in data and data['ShipResult']:
                    ship_result = data['ShipResult'][0] if isinstance(data['ShipResult'], list) else data['ShipResult']
                    if 'APSShipDetail' in ship_result:
                        detail = ship_result['APSShipDetail']
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
                            'registered_owner': detail.get('RegisteredOwner', ''),
                            'group_beneficial_owner': detail.get('GroupBeneficialOwner', ''),
                            'operator': detail.get('Operator', ''),
                            'ship_manager': detail.get('ShipManager', ''),
                            'technical_manager': detail.get('TechnicalManager', ''),
                            'doc_company': detail.get('DOCCompany', ''),
                            'legal_overall': int(detail.get('LegalOverall', 0) or 0),
                            'dark_activity_indicator': int(detail.get('ShipDarkActivityIndicator', 0) or 0),
                            'flag_disputed': int(detail.get('ShipFlagDisputed', 0) or 0),
                            'flag_sanctioned': int(detail.get('ShipFlagSanctionedCountry', 0) or 0),
                            'dark_activity_events': []
                        }
                        if 'DarkActivityConfirmed' in detail and detail['DarkActivityConfirmed']:
                            for event in detail['DarkActivityConfirmed']:
                                result['dark_activity_events'].append({
                                    'dark_time': event.get('Dark_Time', ''),
                                    'next_seen': event.get('NextSeen', ''),
                                    'dark_hours': event.get('Dark_Hours', ''),
                                    'dark_activity_type': event.get('Dark_Activity', ''),
                                    'area_name': event.get('Area_Name', ''),
                                    'dark_lat': float(event.get('Dark_Latitude', 0) or 0),
                                    'dark_lon': float(event.get('Dark_Longitude', 0) or 0),
                                    'next_lat': float(event.get('NextSeen_Latitude', 0) or 0),
                                    'next_lon': float(event.get('NextSeen_Longitude', 0) or 0),
                                })
                        return result
            return None
        except Exception as e:
            st.error(f"Error fetching ship details: {e}")
            return None
    
    def get_imo_by_mmsi(self, mmsi: str) -> Optional[str]:
        """Look up IMO number from MMSI - also caches compliance data"""
        compliance = self.get_ship_compliance_by_mmsi(mmsi)
        if compliance:
            # IMO was cached during compliance lookup
            mmsi_cache = st.session_state.mmsi_to_imo_cache
            return mmsi_cache.get(mmsi)
        return None
    
    def batch_get_imo_by_mmsi(self, mmsi_list: List[str]) -> Dict[str, str]:
        """Look up IMO numbers for multiple MMSIs - also fetches compliance data in same call"""
        results = {}
        cache = st.session_state.mmsi_to_imo_cache
        uncached_mmsis = [m for m in mmsi_list if m not in cache or cache.get(m) is None]
        
        # Return cached IMOs
        for mmsi in mmsi_list:
            if mmsi in cache and cache[mmsi] is not None:
                results[mmsi] = cache[mmsi]
        
        if not uncached_mmsis:
            return results
        
        # Fetch compliance data for uncached MMSIs (this also caches IMOs)
        # This happens silently as it's part of the overall compliance fetching process
        for mmsi in uncached_mmsis:
            imo = self.get_imo_by_mmsi(mmsi)
            if imo:
                results[mmsi] = imo
        
        return results
    
    def parse_compliance_from_ship_detail(self, ship_detail: Dict) -> Dict:
        """Extract compliance indicators from APSShipDetail"""
        return {
            'legal_overall': int(ship_detail.get('LegalOverall', -1)),
            'ship_bes_sanction': int(ship_detail.get('ShipBESSanctionList', -1)),
            'ship_eu_sanction': int(ship_detail.get('ShipEUSanctionList', -1)),
            'ship_ofac_sanction': int(ship_detail.get('ShipOFACSanctionList', -1)),
            'ship_un_sanction': int(ship_detail.get('ShipUNSanctionList', -1)),
            'dark_activity': int(ship_detail.get('ShipDarkActivityIndicator', -1)),
            'flag_disputed': int(ship_detail.get('ShipFlagDisputed', -1)),
            'flag_sanctioned': int(ship_detail.get('ShipFlagSanctionedCountry', -1)),
            'port_call_3m': int(ship_detail.get('ShipSanctionedCountryPortCallLast3m', -1)),
            'port_call_6m': int(ship_detail.get('ShipSanctionedCountryPortCallLast6m', -1)),
            'port_call_12m': int(ship_detail.get('ShipSanctionedCountryPortCallLast12m', -1)),
            'owner_ofac': int(ship_detail.get('ShipOwnerOFACSanctionList', -1)),
            'owner_un': int(ship_detail.get('ShipOwnerUNSanctionList', -1)),
            'owner_eu': int(ship_detail.get('ShipOwnerEUSanctionList', -1)),
            'owner_bes': int(ship_detail.get('ShipOwnerBESSanctionList', -1)),
            'sts_partner_non_compliance': int(ship_detail.get('ShipSTSPartnerNonComplianceLast12m', -1)),
            'cached_at': datetime.now(SGT).isoformat()
        }
    
    def get_ship_compliance_by_imo_batch(self, imo_numbers: List[str], status_placeholder=None) -> Dict[str, Dict]:
        """Get compliance data for multiple IMOs (up to 100) in one call"""
        if not imo_numbers:
            return {}
        
        cache = st.session_state.risk_data_cache
        uncached_imos = [imo for imo in imo_numbers if imo not in cache]
        
        # Use provided status_placeholder or create new one
        info_placeholder = status_placeholder if status_placeholder else st.empty()
        
        # Always show message, even if all are cached
        total_vessels = len(imo_numbers)
        if not uncached_imos:
            info_placeholder.info(f"🔍 Fetching compliance data for {total_vessels} vessels... 100% (all cached)")
            time.sleep(0.5)
            info_placeholder.empty()
            return {imo: cache[imo] for imo in imo_numbers}
        
        info_placeholder.info(f"🔍 Fetching compliance data for {total_vessels} vessels ({len(uncached_imos)} new)... 0%")
        try:
            # Batch up to 100 IMOs per call
            batches = [uncached_imos[i:i+100] for i in range(0, len(uncached_imos), 100)]
            received_imos = set()
            
            for batch_idx, batch in enumerate(batches):
                imo_string = ','.join(batch)
                url = f"{self.base_url_imo}?imoNumbers={imo_string}"
                response = requests.get(url, auth=(self.username, self.password), timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    # Response structure: {"shipCount": 3, "ShipResult": [...]}
                    if 'ShipResult' in data and data['ShipResult']:
                        ship_results = data['ShipResult'] if isinstance(data['ShipResult'], list) else [data['ShipResult']]
                        
                        for ship_result in ship_results:
                            if 'APSShipDetail' in ship_result:
                                detail = ship_result['APSShipDetail']
                                imo = str(detail.get('IHSLRorIMOShipNo', ''))
                                if imo:
                                    received_imos.add(imo)
                                    cache[imo] = self.parse_compliance_from_ship_detail(detail)
                
                progress_pct = int(((batch_idx + 1) / len(batches)) * 100)
                info_placeholder.info(f"🔍 Fetching compliance data for {total_vessels} vessels ({len(uncached_imos)} new)... {progress_pct}%")
                
                # Force Streamlit to update the UI
                if batch_idx < len(batches) - 1:  
                    time.sleep(0.1)
                else:
                    time.sleep(0.5)
            # Mark IMOs that weren't returned as checked but not found
            for imo in uncached_imos:
                if imo not in received_imos:
                    cache[imo] = {
                        'legal_overall': -1,
                        'checked_but_not_found': True,
                        'cached_at': datetime.now(SGT).isoformat()
                    }
            
            st.session_state.risk_data_cache = cache
            save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache)
        except Exception as e:
            st.error(f"⚠️ S&P Ships API error: {str(e)}")
        # Don't clear the placeholder here - let it stay visible until new data is displayed
        
        return {imo: cache.get(imo, {}) for imo in imo_numbers}
    
    def get_ship_compliance_by_mmsi(self, mmsi: str) -> Dict:
        """Get compliance data for single MMSI"""
        if not mmsi:
            return {}
        
        cache = st.session_state.risk_data_cache
        mmsi_cache = st.session_state.mmsi_to_imo_cache
        
        if mmsi in mmsi_cache and mmsi_cache[mmsi] and mmsi_cache[mmsi] in cache:
            return cache[mmsi_cache[mmsi]]
        
        try:
            url = f"{self.base_url_mmsi}?mmsi={mmsi}"
            response = requests.get(url, auth=(self.username, self.password), timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                # Response structure: {"APSShipDetail": {...}, "APSStatus": {...}}
                if 'APSShipDetail' in data:
                    detail = data['APSShipDetail']
                    imo = str(detail.get('IHSLRorIMOShipNo', ''))
                    
                    if imo:
                        # Cache the IMO mapping
                        mmsi_cache[mmsi] = imo
                        st.session_state.mmsi_to_imo_cache = mmsi_cache
                        
                        # Parse and cache compliance data
                        compliance = self.parse_compliance_from_ship_detail(detail)
                        cache[imo] = compliance
                        st.session_state.risk_data_cache = cache
                        save_cache(st.session_state.ship_static_cache, cache, mmsi_cache)
                        return compliance
            
            time.sleep(0.1)
        except Exception as e:
            st.error(f"⚠️ S&P Ships API error for MMSI {mmsi}: {str(e)}")
        
        return {}

class AISTracker:
    """AIS data collection and vessel tracking"""
    def __init__(self, use_cached_positions: bool = True):
        self.ships = defaultdict(lambda: {'latest_position': None, 'static_data': None})
        if use_cached_positions and 'vessel_positions' in st.session_state:
            cached = st.session_state.vessel_positions
            for mmsi, data in cached.items():
                if mmsi != '_last_update':
                    self.ships[mmsi] = data
    
    def get_ship_color(self, legal_overall: int = -1) -> List[int]:
        """Return color based on compliance status"""
        colors = {2: [220, 53, 69, 200], 1: [255, 193, 7, 200], 0: [40, 167, 69, 200]}
        return colors.get(legal_overall, [128, 128, 128, 200])
    
    def save_positions_to_cache(self):
        """Save current vessel positions"""
        positions_dict = dict(self.ships)
        positions_dict['_last_update'] = datetime.now(SGT).isoformat()
        st.session_state.vessel_positions = positions_dict
        st.session_state.last_data_update = positions_dict['_last_update']
        save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache,
                  st.session_state.get('mmsi_to_imo_cache', {}), positions_dict)
    
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
                
                if 'collection_status_placeholder' in st.session_state:
                    st.session_state.collection_start_time = start_time
                    st.session_state.collection_duration_seconds = duration
                
                async for message_json in ws:
                    if not st.session_state.get('collection_in_progress', True):
                        break
                    elapsed = time.time() - start_time
                    progress = min(elapsed / duration, 1.0)
                    
                    if 'collection_status_placeholder' in st.session_state:
                        placeholder = st.session_state.collection_status_placeholder
                        placeholder.info(f'🔄 Collecting AIS data... {int(progress*100)}%')
                    
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
            'ship_name': ais_message.get('MetaData', {}).get('ShipName', 'Unknown'),
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
        dim_a, dim_b = dimension.get('A', 0) or 0, dimension.get('B', 0) or 0
        dim_c, dim_d = dimension.get('C', 0) or 0, dimension.get('D', 0) or 0
        
        existing_cached = st.session_state.ship_static_cache.get(str(mmsi), {})
        if dim_a == 0 and dim_b == 0:
            dim_a, dim_b = existing_cached.get('dimension_a', 0) or 0, existing_cached.get('dimension_b', 0) or 0
        if dim_c == 0 and dim_d == 0:
            dim_c, dim_d = existing_cached.get('dimension_c', 0) or 0, existing_cached.get('dimension_d', 0) or 0
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
        if time.time() - st.session_state.last_save > 60:
            save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache)
            st.session_state.last_save = time.time()
    
    def get_dataframe_with_compliance(self, sp_api: Optional[SPShipsComplianceAPI] = None, 
                                     expiry_hours: Optional[int] = None, status_placeholder=None) -> pd.DataFrame:
        """Get dataframe with compliance indicators"""
        data = []
        now = datetime.now(SGT)
        
        for mmsi, ship_data in self.ships.items():
            pos = ship_data.get('latest_position')
            if expiry_hours is not None:
                last_seen_str = ship_data.get('last_seen')
                if last_seen_str:
                    try:
                        last_seen = datetime.fromisoformat(last_seen_str)
                        if (now - last_seen).total_seconds() / 3600 > expiry_hours:
                            continue
                    except:
                        pass
            
            static = ship_data.get('static_data') or st.session_state.ship_static_cache.get(str(mmsi), {})
            if not pos or pos.get('latitude') is None or pos.get('longitude') is None:
                continue
            
            name = (static.get('name') or pos.get('ship_name') or 'Unknown').strip()
            ship_type = static.get('type')
            imo = str(static.get('imo', '0'))
            true_heading = pos.get('true_heading', 511)
            heading = pos.get('cog', 0) if true_heading == 511 else true_heading
            
            dim_a, dim_b = static.get('dimension_a', 0) or 0, static.get('dimension_b', 0) or 0
            dim_c, dim_d = static.get('dimension_c', 0) or 0, static.get('dimension_d', 0) or 0
            length, width = dim_a + dim_b, dim_c + dim_d
            has_real_dimensions = (length > 0 and width > 0)
            if not has_real_dimensions:
                length, width = 50, 10
            
            data.append({
                'mmsi': mmsi, 'name': name, 'imo': imo,
                'latitude': pos.get('latitude'), 'longitude': pos.get('longitude'),
                'speed': pos.get('sog', 0), 'course': pos.get('cog', 0), 'heading': heading,
                'nav_status': pos.get('nav_status', 15),
                'nav_status_name': NAV_STATUS_NAMES.get(pos.get('nav_status', 15), 'Unknown'),
                'type': ship_type, 'type_name': get_vessel_type_category(ship_type),
                'length': length, 'width': width,
                'dim_a': dim_a, 'dim_b': dim_b, 'dim_c': dim_c, 'dim_d': dim_d,
                'has_dimensions': has_real_dimensions,
                'destination': (static.get('destination') or 'Unknown').strip(),
                'call_sign': static.get('call_sign', ''),
                'has_static': bool(static.get('name')),
                'last_seen': ship_data.get('last_seen', pos.get('timestamp', '')),
                'legal_overall': -1, 'un_sanction': -1, 'ofac_sanction': -1, 'dark_activity': -1,
                'bes_sanction': -1, 'eu_sanction': -1, 'flag_disputed': -1, 'flag_sanctioned': -1,
                'port_call_3m': -1, 'port_call_6m': -1, 'port_call_12m': -1,
                'owner_ofac': -1, 'owner_un': -1, 'owner_eu': -1, 'owner_bes': -1,
                'sts_partner_non_compliance': -1, 'compliance_checked': False,
                'color': self.get_ship_color(-1)
            })
        
        df = pd.DataFrame(data)
        if len(df) == 0:
            return df
        
        valid_imos = [str(imo) for imo in df['imo'].unique() if imo and imo != '0']
        missing_imo_mask = (df['imo'] == '0') | (df['imo'] == '')
        missing_imo_mmsis = df.loc[missing_imo_mask, 'mmsi'].astype(str).unique().tolist()
        
        if missing_imo_mmsis and sp_api:
            mmsi_to_imo = sp_api.batch_get_imo_by_mmsi(missing_imo_mmsis)
            for idx, row in df.iterrows():
                if str(row['mmsi']) in mmsi_to_imo and mmsi_to_imo[str(row['mmsi'])]:
                    found_imo = mmsi_to_imo[str(row['mmsi'])]
                    df.at[idx, 'imo'] = found_imo
                    if found_imo not in valid_imos:
                        valid_imos.append(found_imo)
        elif missing_imo_mmsis and not sp_api:
            cache = st.session_state.get('mmsi_to_imo_cache', {})
            for idx, row in df.iterrows():
                mmsi_str = str(row['mmsi'])
                if mmsi_str in cache and cache[mmsi_str]:
                    found_imo = cache[mmsi_str]
                    df.at[idx, 'imo'] = found_imo
                    if found_imo not in valid_imos:
                        valid_imos.append(found_imo)
        
        compliance_cache = st.session_state.get('risk_data_cache', {})
        if valid_imos and sp_api:
            # Use batch IMO lookup - pass status_placeholder for progress updates
            compliance_data = sp_api.get_ship_compliance_by_imo_batch(valid_imos, status_placeholder)
        else:
            compliance_data = {imo: compliance_cache.get(imo, {}) for imo in valid_imos}
        
        for idx, row in df.iterrows():
            imo = str(row['imo'])
            if imo in compliance_data and compliance_data[imo]:
                comp = compliance_data[imo]
                legal_overall = comp.get('legal_overall', -1)
                if isinstance(legal_overall, str):
                    legal_overall = int(legal_overall) if legal_overall.isdigit() else -1
                
                df.at[idx, 'legal_overall'] = legal_overall
                df.at[idx, 'un_sanction'] = int(comp.get('ship_un_sanction', 0) or 0)
                df.at[idx, 'ofac_sanction'] = int(comp.get('ship_ofac_sanction', 0) or 0)
                df.at[idx, 'dark_activity'] = int(comp.get('dark_activity', 0) or 0)
                df.at[idx, 'bes_sanction'] = int(comp.get('ship_bes_sanction', 0) or 0)
                df.at[idx, 'eu_sanction'] = int(comp.get('ship_eu_sanction', 0) or 0)
                df.at[idx, 'flag_disputed'] = int(comp.get('flag_disputed', 0) or 0)
                df.at[idx, 'flag_sanctioned'] = int(comp.get('flag_sanctioned', 0) or 0)
                df.at[idx, 'port_call_3m'] = int(comp.get('port_call_3m', 0) or 0)
                df.at[idx, 'port_call_6m'] = int(comp.get('port_call_6m', 0) or 0)
                df.at[idx, 'port_call_12m'] = int(comp.get('port_call_12m', 0) or 0)
                df.at[idx, 'owner_ofac'] = int(comp.get('owner_ofac', 0) or 0)
                df.at[idx, 'owner_un'] = int(comp.get('owner_un', 0) or 0)
                df.at[idx, 'owner_eu'] = int(comp.get('owner_eu', 0) or 0)
                df.at[idx, 'owner_bes'] = int(comp.get('owner_bes', 0) or 0)
                df.at[idx, 'sts_partner_non_compliance'] = int(comp.get('sts_partner_non_compliance', 0) or 0)
                df.at[idx, 'compliance_checked'] = True
                df.at[idx, 'color'] = self.get_ship_color(legal_overall)
        
        return df

def create_vessel_layers(df: pd.DataFrame, zoom: float = 10, display_mode: str = "Dots") -> List[pdk.Layer]:
    """Create PyDeck layers for vessels - user-selectable display mode"""
    if len(df) == 0:
        return []
    
    layers = []
    vessel_data = []
    
    for _, row in df.iterrows():
        vessel_length = row['length'] if 0 < row['length'] < 500 else 50
        vessel_width = row['width'] if 0 < row['width'] < 80 else 10
        
        legal_emoji = {2: '🔴', 1: '🟡', 0: '🟢'}.get(row['legal_overall'], '❓')
        dim_text = f"{vessel_length:.0f}m x {vessel_width:.0f}m" + ("" if row['has_dimensions'] else " (est.)")
        
        # Format last seen time
        last_seen_text = format_datetime(row['last_seen']) if row['last_seen'] else 'Unknown'
        
        tooltip_text = (
            f"<b>{row['name']}</b><br/>"
            f"IMO: {row['imo']}<br/>MMSI: {row['mmsi']}<br/>"
            f"Type: {row['type_name']}<br/>Size: {dim_text}<br/>"
            f"Heading: {row['heading']:.0f}°<br/>Speed: {row['speed']:.1f} kts<br/>"
            f"Nav Status: {row['nav_status_name']}<br/>"
            f"Dest: {row['destination']}<br/>"
            f"Last Seen: {last_seen_text}<br/>Legal Overall: {legal_emoji}"
        )
        
        vessel_data.append({
            'latitude': row['latitude'], 'longitude': row['longitude'],
            'name': row['name'], 'tooltip': tooltip_text, 'color': row['color'],
            'heading': row['heading'], 'length': vessel_length, 'width': vessel_width,
            'dim_a': row.get('dim_a', 0) or 0, 'dim_b': row.get('dim_b', 0) or 0,
            'dim_c': row.get('dim_c', 0) or 0, 'dim_d': row.get('dim_d', 0) or 0,
        })
    
    if display_mode == "Dots":
        layers.append(pdk.Layer(
            'ScatterplotLayer', data=vessel_data,
            get_position=['longitude', 'latitude'], get_fill_color='color',
            get_radius=150, radius_min_pixels=3, radius_max_pixels=8,
            pickable=True, auto_highlight=True
        ))
    else:  # "Shapes"
        vessel_polygons = []
        for v in vessel_data:
            polygon = create_vessel_polygon(
                lat=v['latitude'], lon=v['longitude'], heading=v['heading'],
                length=v['length'], width=v['width'],
                dim_a=v['dim_a'], dim_b=v['dim_b'], dim_c=v['dim_c'], dim_d=v['dim_d']
            )
            vessel_polygons.append({
                'polygon': polygon, 'name': v['name'], 
                'tooltip': v['tooltip'], 'color': v['color']
            })
        layers.append(pdk.Layer(
            'PolygonLayer', data=vessel_polygons,
            get_polygon='polygon', get_fill_color='color',
            get_line_color=[50, 50, 50, 100], line_width_min_pixels=1,
            pickable=True, auto_highlight=True, extruded=False
        ))
    return layers

def create_zone_layer(zones: List[Dict], color: List[int], layer_id: str) -> pdk.Layer:
    """Create PyDeck polygon layer for maritime zones"""
    if not zones:
        return None
    zone_type_name = layer_id.replace('_', ' ').title()
    zone_data = [{'polygon': zone['polygon'], 'name': zone['name'], 
                  'tooltip': f"<b>{zone['name']}</b><br/>Type: {zone_type_name}"} 
                 for zone in zones]
    return pdk.Layer(
        'PolygonLayer', data=zone_data, id=layer_id,
        get_polygon='polygon', get_fill_color=color,
        get_line_color=[100, 100, 100, 150], line_width_min_pixels=1,
        pickable=True, auto_highlight=True, extruded=False
    )

def show_vessel_details_panel(imo: str, vessel_name: str, sp_username: str, sp_password: str):
    """Display detailed vessel information including dark activity events"""
    if not imo or imo == '0':
        st.warning("⚠️ No IMO number available for this vessel.")
        return
    
    with st.expander(f"📋 Vessel Details: {vessel_name} (IMO: {imo})", expanded=True):
        if st.button("❌ Close Details", key="close_details"):
            st.session_state.show_details_imo = None
            st.session_state.show_details_name = None
            st.rerun()
        
        ships_api = SPShipsComplianceAPI(sp_username, sp_password)
        with st.spinner("🔍 Fetching vessel details..."):
            details = ships_api.get_ship_details_by_imo(imo)
        
        if not details:
            st.error("❌ Could not fetch vessel details.")
            return
        
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
            st.markdown(f"**Nav Status:** {details.get('status', 'N/A')}")
        with col3:
            st.markdown(f"**Class:** {details.get('classification', 'N/A')}")
            legal = details.get('legal_overall', 0)
            legal_emoji = {2: '🔴 Severe', 1: '🟡 Warning', 0: '🟢 Clear'}.get(legal, '❓ Unknown')
            st.markdown(f"**Legal Overall:** {legal_emoji}")
            dark_ind = details.get('dark_activity_indicator', 0)
            dark_emoji = {2: '🔴 Severe', 1: '🟡 Warning', 0: '🟢 Clear'}.get(dark_ind, '❓ Unknown')
            st.markdown(f"**Dark Activity:** {dark_emoji}")
        
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
        
        dark_events = details.get('dark_activity_events', [])
        if dark_events:
            st.subheader(f"🌑 Dark Activity Events ({len(dark_events)} recorded)")
            for i, event in enumerate(dark_events):
                with st.container():
                    st.markdown(f"---\n**Event {i+1}: {event.get('dark_activity_type', 'Unknown')}**")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.markdown("**🔴 When Dark:**")
                        st.markdown(f"Time: {event.get('dark_time', 'N/A')}")
                        st.markdown(f"Area: {event.get('area_name', 'N/A')}")
                        st.markdown(f"Position: {event.get('dark_lat', 0):.4f}, {event.get('dark_lon', 0):.4f}")
                    with col2:
                        st.markdown("**🟢 When Seen Again:**")
                        st.markdown(f"Time: {event.get('next_seen', 'N/A')}")
                        st.markdown(f"Position: {event.get('next_lat', 0):.4f}, {event.get('next_lon', 0):.4f}")
        else:
            st.info("✅ No dark activity events recorded for this vessel.")

def apply_filters(df: pd.DataFrame, selected_compliance, selected_sanctions, 
                 selected_types, selected_nav_statuses) -> pd.DataFrame:
    """Apply all filters to the dataframe"""
    if len(df) == 0:
        return df
    filtered_df = df.copy()
    
    if selected_compliance and "All" not in selected_compliance:
        compliance_map = {"Severe (🔴)": 2, "Warning (🟡)": 1, "Clear (🟢)": 0}
        selected_levels = [compliance_map[c] for c in selected_compliance if c in compliance_map]
        if selected_levels:
            filtered_df = filtered_df[filtered_df['legal_overall'].isin(selected_levels)]
    
    if selected_sanctions and "All" not in selected_sanctions:
        sanction_mask = pd.Series([False] * len(filtered_df), index=filtered_df.index)
        if "UN Sanctions" in selected_sanctions:
            sanction_mask = sanction_mask | (filtered_df['un_sanction'] == 2)
        if "OFAC Sanctions" in selected_sanctions:
            sanction_mask = sanction_mask | (filtered_df['ofac_sanction'] == 2)
        if "Dark Activity" in selected_sanctions:
            sanction_mask = sanction_mask | (filtered_df['dark_activity'] >= 1)
        filtered_df = filtered_df[sanction_mask]
    
    if selected_types and "All" not in selected_types:
        filtered_df = filtered_df[filtered_df['type_name'].isin(selected_types)]
    
    if selected_nav_statuses and "All" not in selected_nav_statuses:
        filtered_df = filtered_df[filtered_df['nav_status_name'].isin(selected_nav_statuses)]
    
    return filtered_df

def format_compliance_value(val) -> str:
    """Format compliance values with emoji"""
    emoji_map = {2: "🔴", 1: "🟡", 0: "🟢"}
    return emoji_map.get(val, "❓") if val is not None and val != -1 else "❓"

def display_vessel_data(df: pd.DataFrame, last_update: str, vessel_display_mode: str, 
                       maritime_zones: Dict, show_anchorages: bool, show_channels: bool, 
                       show_fairways: bool, is_cached: bool = False):
    """Display vessel data on map and table with persistent view"""
    
    # Display statistics
    cols = st.columns(8)
    cols[0].metric("🚢 Total Ships", len(df))
    cols[1].metric("⚡ Moving", len(df[df['speed'] > 1]) if len(df) > 0 else 0)
    cols[2].metric("📡 Has Static", int(df['has_static'].sum()) if len(df) > 0 else 0)
    
    if len(df) > 0:
        real_dims = int(df['has_dimensions'].sum())
        severe_count = len(df[df['legal_overall'] == 2])
        warning_count = len(df[df['legal_overall'] == 1])
        clear_count = len(df[df['legal_overall'] == 0])
        unknown_count = len(df[df['legal_overall'] < 0])
    else:
        severe_count = warning_count = clear_count = unknown_count = real_dims = 0
    
    cols[3].metric("📐 Real Dims", real_dims)
    cols[4].metric("🔴 Severe", severe_count)
    cols[5].metric("🟡 Warning", warning_count)
    cols[6].metric("🟢 Clear", clear_count)
    cols[7].metric("❓ Unknown", unknown_count)
    
    # Determine map view - preserve current view from session state
    user_zoom = st.session_state.get('user_zoom', 10)
    
    # Initialize map center only on first load
    if 'map_center_initialized' not in st.session_state:
        st.session_state.map_center_initialized = True
        st.session_state.map_center = {"lat": 1.28, "lon": 103.85, "zoom": user_zoom}
    
    # Always use stored map center if available (unless viewing a specific vessel)
    if st.session_state.selected_vessel and len(df) > 0:
        vessel = df[df['mmsi'] == st.session_state.selected_vessel]
        if len(vessel) > 0:
            center_lat = vessel.iloc[0]['latitude']
            center_lon = vessel.iloc[0]['longitude']
            zoom = max(user_zoom, 14)
            # Update stored position when viewing specific vessel
            st.session_state.map_center = {"lat": center_lat, "lon": center_lon, "zoom": zoom}
        else:
            # Vessel not in filtered results, use stored position
            center_lat = st.session_state.map_center.get('lat', 1.28)
            center_lon = st.session_state.map_center.get('lon', 103.85)
            zoom = st.session_state.map_center.get('zoom', user_zoom)
    else:
        # Use stored map position (preserves across filter changes)
        center_lat = st.session_state.map_center.get('lat', 1.28)
        center_lon = st.session_state.map_center.get('lon', 103.85)
        zoom = st.session_state.map_center.get('zoom', user_zoom)
    
    # Create map layers
    layers = []
    if show_anchorages and maritime_zones['Anchorages']:
        layer = create_zone_layer(maritime_zones['Anchorages'], [0, 255, 255, 50], "anchorages")
        if layer:
            layers.append(layer)
    if show_channels and maritime_zones['Channels']:
        layer = create_zone_layer(maritime_zones['Channels'], [255, 255, 0, 50], "channels")
        if layer:
            layers.append(layer)
    if show_fairways and maritime_zones['Fairways']:
        layer = create_zone_layer(maritime_zones['Fairways'], [255, 165, 0, 50], "fairways")
        if layer:
            layers.append(layer)
    
    vessel_layers = create_vessel_layers(df, zoom=zoom, display_mode=vessel_display_mode)
    layers.extend(vessel_layers)
    
    # Render map - use static key to maintain state across filter changes
    view_state = pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=zoom, pitch=0)
    deck = pdk.Deck(
        map_style='https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
        initial_view_state=view_state, layers=layers,
        tooltip={'html': '{tooltip}', 'style': {'backgroundColor': 'steelblue', 'color': 'white'}}
    )
    
    # Use static key to preserve map state even when vessels change
    st.pydeck_chart(deck, use_container_width=True, key="main_vessel_map")
    
    # Vessel table
    st.subheader("📋 Vessel Details")
    
    if len(df) == 0:
        st.info("No vessels to display. Adjust filters or refresh data.")
    else:
        # Sort by legal_overall: default descending order (2, 1, 0, -1) from top to bottom
        display_df = df.copy().sort_values(['legal_overall', 'name'], ascending=[False, True])
        
        # Create display columns with emojis (simple, no sorting tricks)
        display_df['legal_display'] = display_df['legal_overall'].apply(format_compliance_value)
        display_df['un_display'] = display_df['un_sanction'].apply(format_compliance_value)
        display_df['ofac_display'] = display_df['ofac_sanction'].apply(format_compliance_value)
        display_df['eu_display'] = display_df['eu_sanction'].apply(format_compliance_value)
        display_df['bes_display'] = display_df['bes_sanction'].apply(format_compliance_value)
        display_df['owner_un_display'] = display_df['owner_un'].apply(format_compliance_value)
        display_df['owner_ofac_display'] = display_df['owner_ofac'].apply(format_compliance_value)
        display_df['dark_display'] = display_df['dark_activity'].apply(format_compliance_value)
        display_df['sts_display'] = display_df['sts_partner_non_compliance'].apply(format_compliance_value)
        display_df['port3m_display'] = display_df['port_call_3m'].apply(format_compliance_value)
        display_df['port6m_display'] = display_df['port_call_6m'].apply(format_compliance_value)
        display_df['port12m_display'] = display_df['port_call_12m'].apply(format_compliance_value)
        display_df['flag_sanc_display'] = display_df['flag_sanctioned'].apply(format_compliance_value)
        display_df['flag_disp_display'] = display_df['flag_disputed'].apply(format_compliance_value)
        
        # Create table with display columns only
        table_df = display_df[[
            'name', 'imo', 'mmsi', 'type_name', 'nav_status_name',
            'legal_display', 'un_display', 'ofac_display', 'eu_display', 'bes_display',
            'owner_un_display', 'owner_ofac_display', 'dark_display', 'sts_display',
            'port3m_display', 'port6m_display', 'port12m_display',
            'flag_sanc_display', 'flag_disp_display'
        ]].copy()
        
        # Rename columns for display
        table_df.columns = [
            'Name', 'IMO', 'MMSI', 'Type', 'Nav Status',
            'Legal Overall', 'UN', 'OFAC', 'EU', 'UK',
            'Own UN', 'Own OFAC', 'Dark', 'STS',
            'Port 3m', 'Port 6m', 'Port 12m',
            'Flag Sanc', 'Flag Disp'
        ]
        
        # Configure columns
        column_config = {}
        
        # Calculate dynamic height based on number of rows
        row_count = len(table_df)
        header_height = 38
        row_height = 35
        dynamic_height = header_height + (row_height * min(row_count, 20))  # Max 20 rows visible
        
        # Use session-based counter for stable unique keys across reruns
        if 'table_render_count' not in st.session_state:
            st.session_state.table_render_count = 0
        st.session_state.table_render_count += 1
        table_key = f"vessel_table_{st.session_state.table_render_count}"
        
        selected_rows = st.dataframe(
            table_df, use_container_width=True, height=dynamic_height,
            hide_index=True, on_select="rerun", selection_mode="single-row",
            column_config=column_config, key=table_key
        )
        
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

def display_cached_data(vessel_expiry_hours, vessel_display_mode, maritime_zones, 
                       show_anchorages, show_channels, show_fairways, 
                       selected_compliance, selected_sanctions, selected_types, 
                       selected_nav_statuses):
    """Display cached vessel data without collecting new AIS data"""
    if 'vessel_positions' not in st.session_state or not st.session_state.vessel_positions:
        return
    
    cached_positions = st.session_state.vessel_positions
    last_update = cached_positions.get('_last_update', 'Unknown')
    tracker = AISTracker(use_cached_positions=True)
    df = tracker.get_dataframe_with_compliance(sp_api=None, expiry_hours=vessel_expiry_hours)
    
    if df.empty:
        return
    
    df = apply_filters(df, selected_compliance, selected_sanctions, selected_types, 
                      selected_nav_statuses)
    
    display_vessel_data(df, last_update, vessel_display_mode, maritime_zones, 
                       show_anchorages, show_channels, show_fairways, is_cached=True)

def update_display(duration, ais_api_key, coverage_bbox, enable_compliance, sp_username, sp_password,
                  vessel_expiry_hours, vessel_display_mode, maritime_zones, show_anchorages, 
                  show_channels, show_fairways, selected_compliance, selected_sanctions, 
                  selected_types, selected_nav_statuses, status_placeholder):
    """Collect data and update display"""
    sp_api = SPShipsComplianceAPI(sp_username, sp_password) if enable_compliance and sp_username and sp_password else None
    
    status_placeholder.empty()
    
    status_placeholder.info(f'🔄 Collecting AIS data... 0%')
    
    st.session_state.collection_duration = duration
    st.session_state.collection_status_placeholder = status_placeholder
    
    # Collect data without spinner
    tracker = AISTracker(use_cached_positions=True)
    if ais_api_key:
        try:
            asyncio.run(tracker.collect_data(duration, ais_api_key, coverage_bbox))
        except Exception as e:
            st.session_state.collection_in_progress = False
            if 'collection_status_placeholder' in st.session_state:
                del st.session_state.collection_status_placeholder
            status_placeholder.error(f"⚠️ Error collecting AIS data: {e}")
            return
    else:
        st.session_state.collection_in_progress = False
        if 'collection_status_placeholder' in st.session_state:
            del st.session_state.collection_status_placeholder
        status_placeholder.warning("⚠️ No AISStream API key provided.")
        return
    
    if 'collection_status_placeholder' in st.session_state:
        del st.session_state.collection_status_placeholder
    
    # get_dataframe_with_compliance will show its own detailed progress message with vessel count and percentage
    df = tracker.get_dataframe_with_compliance(sp_api, expiry_hours=vessel_expiry_hours, status_placeholder=status_placeholder)
    
    st.session_state.collection_in_progress = False
    
    if df.empty:
        status_placeholder.warning("⚠️ No ships detected. Try increasing collection time or check API key.")
        return
    
    df = apply_filters(df, selected_compliance, selected_sanctions, selected_types, 
                      selected_nav_statuses)
    
    if df.empty:
        status_placeholder.warning("⚠️ No vessels match filters. Adjust filters to see vessels.")
    
    last_update = st.session_state.get('last_data_update', datetime.now(SGT).isoformat())
    display_vessel_data(df, last_update, vessel_display_mode, maritime_zones, 
                       show_anchorages, show_channels, show_fairways, is_cached=False)
    
    # Clear the status message after data is displayed
    status_placeholder.empty()
    
    save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache,
              st.session_state.get('mmsi_to_imo_cache', {}), st.session_state.get('vessel_positions', {}))

# ============= STREAMLIT UI =============
st.title("🚢 Singapore Ship Risk Tracker")
st.markdown("Real-time vessel tracking with S&P Maritime compliance screening")

# Sidebar Configuration

try:
    sp_username = st.secrets["sp_maritime"]["username"]
    sp_password = st.secrets["sp_maritime"]["password"]
    ais_api_key = st.secrets.get("aisstream", {}).get("api_key", "")
except:
    with st.sidebar.expander("🔐 API Credentials", expanded=True):
        sp_username = st.text_input("S&P Username", type="password")
        sp_password = st.text_input("S&P Password", type="password")
        ais_api_key = st.text_input("AISStream API Key", type="password")

st.sidebar.header("📡 AIS Settings")

collection_active = st.session_state.get('collection_in_progress', False) or st.session_state.get('sp_api_in_progress', False)

duration = st.sidebar.slider("AIS collection time (seconds)", 10, 300, 60, disabled=collection_active)
enable_compliance = st.sidebar.checkbox("Enable S&P compliance screening", value=True, disabled=collection_active)

# Refresh Now / Stop button in sidebar
if not collection_active:
    refresh_button = st.sidebar.button("🔄 Refresh Now", type="primary", use_container_width=True)
    stop_button = False
else:
    refresh_button = False
    stop_button = st.sidebar.button("⏹️ Stop Collection", type="secondary", use_container_width=True)

st.sidebar.subheader("Coverage Area")
coverage_options = {
    "Singapore Strait Only": [[[1.15, 103.55], [1.50, 104.10]]],
    "Singapore + Approaches": [[[1.0, 103.3], [1.6, 104.3]]],
    "Malacca to South China Sea (Dark Fleet)": [[[0.5, 102.0], [2.5, 106.0]]],
    "Extended Malacca Strait": [[[-0.5, 100.0], [3.0, 106.0]]],
    "Full Regional (Max Coverage)": [[[-1.0, 99.0], [4.0, 108.0]]]
}
selected_coverage = st.sidebar.selectbox("Select coverage area", list(coverage_options.keys()), index=2)
coverage_bbox = coverage_options[selected_coverage]

st.sidebar.subheader("⏱️ Vessel Expiry")
expiry_options = {"30 minutes": 0.5, "1 hour": 1, "2 hours": 2, "4 hours": 4, "8 hours": 8, "12 hours": 12, "24 hours": 24, "Never (keep forever)": None}
selected_expiry = st.sidebar.selectbox("Remove vessels not seen in:", list(expiry_options.keys()), index=0)
vessel_expiry_hours = expiry_options[selected_expiry]

st.sidebar.header("🗺️ Maritime Zones")
show_anchorages = st.sidebar.checkbox("Show Anchorages", value=True)
show_channels = st.sidebar.checkbox("Show Channels", value=True)
show_fairways = st.sidebar.checkbox("Show Fairways", value=True)

st.sidebar.header("🔍 Map View")

vessel_display_mode = st.sidebar.radio(
    "Vessel Display Mode",
    options=["Dots", "Shapes"],
    index=st.session_state.get('vessel_display_mode_index', 0)
)
st.session_state.vessel_display_mode_index = 0 if vessel_display_mode == "Dots" else 1

zoom_level = 10  # Fixed zoom
st.session_state.user_zoom = zoom_level

maritime_zones = {"Anchorages": [], "Channels": [], "Fairways": []}
excel_paths = ["/mnt/project/Anchorages_Channels_Fairways_Details.xlsx",
               "/mnt/user-data/uploads/Anchorages_Channels_Fairways_Details.xlsx",
               "Anchorages_Channels_Fairways_Details.xlsx"]
if show_anchorages or show_channels or show_fairways:
    for path in excel_paths:
        if os.path.exists(path):
            maritime_zones = load_maritime_zones(path)
            break

st.sidebar.header("🔍 Filters")
st.sidebar.subheader("Quick Filters")

if 'prev_quick_filter' not in st.session_state:
    st.session_state.prev_quick_filter = "All Vessels"

if 'refresh_in_progress' not in st.session_state:
    st.session_state.refresh_in_progress = False

quick_filter = st.sidebar.radio("Preset", ["All Vessels", "Dark Vessels", "Sanctioned Vessels", "Custom"], 
                                index=0, horizontal=True, key="quick_filter_radio")

if quick_filter == "Dark Vessels":
    default_compliance = ["Severe (🔴)", "Warning (🟡)"]
    default_sanctions = ["Dark Activity"]
    default_types = ["Tanker", "Cargo"]
elif quick_filter == "Sanctioned Vessels":
    default_compliance = ["Severe (🔴)"]
    default_sanctions = ["UN Sanctions", "OFAC Sanctions"]
    default_types = ["All"]
else:  # All Vessels or Custom
    default_compliance = ["All"]
    default_sanctions = ["All"]
    default_types = ["All"]

if st.session_state.prev_quick_filter != quick_filter and quick_filter != "Custom":
    st.session_state.compliance_filter = default_compliance
    st.session_state.sanctions_filter = default_sanctions
    st.session_state.types_filter = default_types
    st.session_state.prev_quick_filter = quick_filter
    # Only rerun if refresh is not in progress
    if not st.session_state.refresh_in_progress:
        st.rerun()

st.sidebar.subheader("Compliance")
compliance_options = ["All", "Severe (🔴)", "Warning (🟡)", "Clear (🟢)"]
selected_compliance = st.sidebar.multiselect("Legal Overall", compliance_options, 
                                             default=st.session_state.get('compliance_filter', default_compliance),
                                             key="compliance_filter")

sanction_options = ["All", "UN Sanctions", "OFAC Sanctions", "Dark Activity"]
selected_sanctions = st.sidebar.multiselect("Sanctions & Dark Activity", sanction_options, 
                                            default=st.session_state.get('sanctions_filter', default_sanctions),
                                            key="sanctions_filter")

st.sidebar.subheader("Vessel Type")
vessel_types = ["All", "Cargo", "Tanker", "Passenger", "Tug", "Fishing", "High Speed Craft", "Pilot", "SAR", "Port Tender", "Law Enforcement", "Other", "Unknown"]
selected_types = st.sidebar.multiselect("Types", vessel_types, 
                                       default=st.session_state.get('types_filter', default_types),
                                       key="types_filter")

st.sidebar.subheader("Navigation Status")
nav_status_options = ["All"] + list(NAV_STATUS_NAMES.values())
selected_nav_statuses = st.sidebar.multiselect("Status", nav_status_options, default=["All"],
                                               key="nav_status_filter")

st.sidebar.header("💾 Cache Statistics")
vessel_count = len([k for k in st.session_state.get('vessel_positions', {}).keys() if k != '_last_update'])
last_update_fmt = format_datetime(st.session_state.get('last_data_update', 'Never'))

st.sidebar.info(f"""**Cached Vessels:** {vessel_count}

**Static Data:** {len(st.session_state.ship_static_cache)} vessels

**Compliance:** {len(st.session_state.risk_data_cache)} vessels

**Last Update:** {last_update_fmt}""")

if st.sidebar.button("🗑️ Clear All Cache"):
    st.session_state.ship_static_cache = {}
    st.session_state.risk_data_cache = {}
    st.session_state.mmsi_to_imo_cache = {}
    st.session_state.vessel_positions = {}
    st.session_state.last_data_update = None
    save_cache({}, {}, {}, {})
    st.sidebar.success("Cache cleared!")
    st.rerun()

# Auto-refresh (place before buttons so it always displays)
st.sidebar.markdown("---")
st.sidebar.markdown("### ⏱️ Auto-Refresh")
auto_refresh = st.sidebar.checkbox("Enable auto-refresh", value=st.session_state.get('auto_refresh_enabled', False))
st.session_state.auto_refresh_enabled = auto_refresh

if auto_refresh:
    refresh_options = {"30 seconds": 30, "1 minute": 60, "2 minutes": 120, "5 minutes": 300, "10 minutes": 600}
    selected_interval = st.sidebar.selectbox("Refresh interval:", list(refresh_options.keys()), index=1)
    st.session_state.refresh_interval = refresh_options[selected_interval]
    
    if 'last_refresh_time' in st.session_state:
        elapsed = time.time() - st.session_state.last_refresh_time
        remaining = max(0, st.session_state.refresh_interval - elapsed)
        st.sidebar.info(f"⏳ Next refresh in {int(remaining)}s")
        # Only rerun every 5 seconds to update countdown, not every second
        if remaining > 5:
            time.sleep(5)
            st.rerun()
        elif remaining > 0:
            time.sleep(remaining)
            st.rerun()

# Legend (place before buttons so it always displays)
st.sidebar.markdown("---")
st.sidebar.markdown("### 🎨 Legend")
st.sidebar.markdown("""
**Vessel Compliance Status:**
- 🔴 Severe
- 🟡 Warning
- 🟢 Clear
- ❓ Unknown

**Maritime Zones:**
- 🔵 Anchorages
- 🟡 Channels
- 🟠 Fairways
""")
st.sidebar.markdown("---")
st.sidebar.caption("Data: AISStream.io + S&P Global Maritime")

status_placeholder = st.empty()

displayed_in_this_run = False

auto_refresh_triggered = False
if 'auto_refresh_enabled' in st.session_state and st.session_state.auto_refresh_enabled:
    if 'last_refresh_time' not in st.session_state:
        st.session_state.last_refresh_time = 0
    
    refresh_interval = st.session_state.get('refresh_interval', 60)
    elapsed = time.time() - st.session_state.last_refresh_time
    
    if elapsed >= refresh_interval:
        auto_refresh_triggered = True
        st.session_state.last_refresh_time = time.time()

if stop_button:
    st.session_state.collection_in_progress = False
    st.session_state.sp_api_in_progress = False
    
    for key in ['collection_status_placeholder', 'collection_duration', 'sp_api_state']:
        st.session_state.pop(key, None)
    
    status_placeholder.empty()
    st.rerun()

if not st.session_state.get('collection_in_progress', False):
    if refresh_button or auto_refresh_triggered:
        st.session_state.collection_in_progress = True
        st.session_state.refresh_in_progress = True
        st.session_state.last_refresh_time = time.time()
        
        status_placeholder.empty()
        
        st.rerun()
else:
    # Collection is in progress - actually collect data on this rerun
    if st.session_state.get('refresh_in_progress', False):
        update_display(duration, ais_api_key, coverage_bbox, enable_compliance, sp_username, sp_password,
                      vessel_expiry_hours, vessel_display_mode, maritime_zones, show_anchorages, 
                      show_channels, show_fairways, selected_compliance, selected_sanctions, 
                      selected_types, selected_nav_statuses, status_placeholder)
        st.session_state.data_loaded = True
        st.session_state.refresh_in_progress = False
        st.rerun()
    else:
        if 'vessel_positions' in st.session_state and st.session_state.vessel_positions:
            display_cached_data(vessel_expiry_hours, vessel_display_mode, maritime_zones, show_anchorages, 
                               show_channels, show_fairways, selected_compliance, selected_sanctions, 
                               selected_types, selected_nav_statuses)
            displayed_in_this_run = True

if st.session_state.get('show_details_imo') and sp_username and sp_password:
    show_vessel_details_panel(st.session_state.show_details_imo, 
                            st.session_state.get('show_details_name', ''),
                            sp_username, sp_password)

# Auto-display cached data on initial load or when filters change
if not displayed_in_this_run and not st.session_state.get('collection_in_progress', False):
    if 'vessel_positions' in st.session_state and st.session_state.vessel_positions:
        display_cached_data(vessel_expiry_hours, vessel_display_mode, maritime_zones, show_anchorages, 
                           show_channels, show_fairways, selected_compliance, selected_sanctions, 
                           selected_types, selected_nav_statuses)
    else:
        status_placeholder.info("ℹ️ No cached vessel data. Click 'Refresh Now' to collect AIS data.")
