"""
Singapore AIS Tracker with S&P Maritime Risk Intelligence
Real-time vessel tracking with compliance and risk indicators
v9 - Streamlined code, fixed sorting, all compliance indicators
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

# Navigation status codes
NAV_STATUS_NAMES = {
    0: "Under way using engine", 1: "At anchor", 2: "Not under command",
    3: "Restricted manoeuvrability", 4: "Constrained by draught", 5: "Moored",
    6: "Aground", 7: "Engaged in fishing", 8: "Under way sailing",
    9: "Reserved for HSC", 10: "Reserved for WIG", 11: "Reserved",
    12: "Reserved", 13: "Reserved", 14: "AIS-SART", 15: "Not defined"
}

# Vessel type categories
def get_vessel_type_category(type_code: int) -> str:
    if type_code is None: return "Unknown"
    if 70 <= type_code <= 79: return "Cargo"
    elif 80 <= type_code <= 89: return "Tanker"
    elif 60 <= type_code <= 69: return "Passenger"
    elif type_code in [31, 32, 52]: return "Tug"
    elif type_code == 30: return "Fishing"
    elif 40 <= type_code <= 49: return "High Speed Craft"
    elif type_code == 50: return "Pilot"
    elif type_code == 51: return "SAR"
    elif type_code == 53: return "Port Tender"
    elif type_code == 55: return "Law Enforcement"
    return "Other"


# Cache management
def load_cache() -> Tuple[Dict, Dict, Dict, Dict]:
    """Load all cached data from disk"""
    caches = [{}, {}, {}, {}]
    files = [STORAGE_FILE, RISK_DATA_FILE, MMSI_IMO_CACHE_FILE, VESSEL_POSITION_FILE]
    for i, f in enumerate(files):
        if os.path.exists(f):
            try:
                with open(f, 'rb') as file:
                    caches[i] = pickle.load(file)
            except: pass
    return tuple(caches)


def save_cache(ship_cache: Dict, risk_cache: Dict, mmsi_imo_cache: Dict, vessel_positions: Dict = None):
    """Save all cached data to disk"""
    data = [(STORAGE_FILE, ship_cache), (RISK_DATA_FILE, risk_cache), 
            (MMSI_IMO_CACHE_FILE, mmsi_imo_cache)]
    if vessel_positions:
        data.append((VESSEL_POSITION_FILE, vessel_positions))
    for f, d in data:
        try:
            with open(f, 'wb') as file:
                pickle.dump(d, file)
        except: pass


def format_datetime(dt_string: str) -> str:
    """Format datetime string to readable format in SGT"""
    if not dt_string or dt_string == 'Unknown': return 'Unknown'
    try:
        if isinstance(dt_string, str):
            dt = datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
        else:
            dt = dt_string
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=SGT)
        else:
            dt = dt.astimezone(SGT)
        return dt.strftime('%d %b %Y, %I:%M %p')
    except:
        return dt_string


def format_compliance_value(val) -> str:
    """Format compliance values with emoji"""
    if val is None or val == -1: return "❓"
    elif val == 2: return "🔴"
    elif val == 1: return "🟡"
    elif val == 0: return "✅"
    return "❓"


# Initialize session state
if 'ship_static_cache' not in st.session_state:
    ship_cache, risk_cache, mmsi_imo_cache, vessel_positions = load_cache()
    st.session_state.ship_static_cache = ship_cache
    st.session_state.risk_data_cache = risk_cache
    st.session_state.mmsi_to_imo_cache = mmsi_imo_cache
    st.session_state.vessel_positions = vessel_positions
    st.session_state.last_save = time.time()

for key, default in [('selected_vessel', None), ('map_center', {"lat": 1.28, "lon": 103.85, "zoom": 10}),
                     ('show_details_imo', None), ('show_details_name', '')]:
    if key not in st.session_state:
        st.session_state[key] = default


# Maritime zones loader
@st.cache_data
def load_maritime_zones(excel_path: str) -> Dict[str, List]:
    """Load maritime zones from Excel file"""
    zones = {"Anchorages": [], "Channels": [], "Fairways": []}
    try:
        import openpyxl
        wb = openpyxl.load_workbook(excel_path, data_only=True)
        for sheet_name in wb.sheetnames:
            zone_type = None
            if 'anchorage' in sheet_name.lower(): zone_type = 'Anchorages'
            elif 'channel' in sheet_name.lower(): zone_type = 'Channels'
            elif 'fairway' in sheet_name.lower(): zone_type = 'Fairways'
            if not zone_type: continue
            
            ws = wb[sheet_name]
            rows = list(ws.iter_rows(min_row=2, values_only=True))
            current_zone = None
            
            for row in rows:
                if len(row) >= 4:
                    zone_name, point_num, lat, lon = row[0], row[1], row[2], row[3]
                    if zone_name and str(zone_name).strip():
                        if current_zone and current_zone['coordinates']:
                            zones[zone_type].append(current_zone)
                        current_zone = {'name': str(zone_name).strip(), 'coordinates': []}
                    if current_zone and lat and lon:
                        try:
                            current_zone['coordinates'].append([float(lon), float(lat)])
                        except: pass
            if current_zone and current_zone['coordinates']:
                zones[zone_type].append(current_zone)
    except Exception as e:
        st.warning(f"Could not load maritime zones: {e}")
    return zones


def create_zone_layer(zones: List[Dict], color: List[int], layer_id: str) -> pdk.Layer:
    """Create a polygon layer for maritime zones"""
    polygons = []
    for zone in zones:
        if zone['coordinates'] and len(zone['coordinates']) >= 3:
            coords = zone['coordinates']
            if coords[0] != coords[-1]:
                coords = coords + [coords[0]]
            polygons.append({'polygon': coords, 'name': zone['name']})
    if not polygons: return None
    return pdk.Layer('PolygonLayer', data=polygons, get_polygon='polygon',
                     get_fill_color=color, get_line_color=[100, 100, 100, 200],
                     line_width_min_pixels=1, pickable=True)


# S&P API Classes
class SPShipsAPI:
    """S&P Ships API for MMSI to IMO lookup"""
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_url = "https://shipsapi.maritime.spglobal.com/MaritimeWCF/APSShipService.svc/RESTFul"
    
    def get_imo_by_mmsi(self, mmsi: str) -> Optional[str]:
        try:
            url = f"{self.base_url}/GetShipDataByMMSI?MMSI={mmsi}"
            response = requests.get(url, auth=(self.username, self.password), timeout=15)
            if response.status_code == 200:
                data = response.json()
                ships = data.get('Ships', [])
                if ships:
                    imo = ships[0].get('LRIMOShipNo')
                    if imo and str(imo) != '0':
                        return str(imo)
        except: pass
        return None
    
    def batch_get_imo_by_mmsi(self, mmsi_list: List[str]) -> Dict[str, str]:
        results = {}
        cache = st.session_state.mmsi_to_imo_cache
        uncached = [m for m in mmsi_list if m not in cache or cache.get(m) is None]
        
        for mmsi in mmsi_list:
            if mmsi in cache and cache[mmsi]:
                results[mmsi] = cache[mmsi]
        
        if not uncached: return results
        
        st.info(f"🔍 Looking up IMO for {len(uncached)} vessels...")
        progress = st.progress(0)
        for i, mmsi in enumerate(uncached):
            imo = self.get_imo_by_mmsi(mmsi)
            if imo:
                cache[mmsi] = imo
                results[mmsi] = imo
            progress.progress((i + 1) / len(uncached))
            time.sleep(0.1)
        progress.empty()
        
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
        if not imo_numbers: return {}
        
        cache = st.session_state.risk_data_cache
        uncached = [imo for imo in imo_numbers if imo not in cache]
        
        if not uncached:
            return {imo: cache[imo] for imo in imo_numbers}
        
        st.info(f"🔍 Fetching compliance data for {len(uncached)} vessels...")
        
        try:
            batches = [uncached[i:i+100] for i in range(0, len(uncached), 100)]
            received = set()
            
            for batch in batches:
                url = f"{self.base_url}?imos={','.join(batch)}"
                response = requests.get(url, auth=(self.username, self.password), timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list):
                        for ship in data:
                            imo = str(ship.get('lrimoShipNo', ''))
                            if not imo: continue
                            received.add(imo)
                            # Extract ALL compliance fields in PDF order
                            cache[imo] = {
                                # Overall
                                'legal_overall': ship.get('legalOverall', 0),
                                # Ship Sanctions (Page 1)
                                'ship_bes_sanction': ship.get('shipBESSanctionList', 0),
                                'ship_eu_sanction': ship.get('shipEUSanctionList', 0),
                                'ship_ofac_sanction': ship.get('shipOFACSanctionList', 0),
                                'ship_ofac_non_sdn': ship.get('shipOFACNonSDNSanctionList', 0),
                                'ship_swiss_sanction': ship.get('shipSwissSanctionList', 0),
                                'ship_un_sanction': ship.get('shipUNSanctionList', 0),
                                # Page 2
                                'ship_ofac_advisory': ship.get('shipOFACAdvisoryList', 0),
                                'port_call_3m': ship.get('shipSanctionedCountryPortCallLast3m', 0),
                                'port_call_6m': ship.get('shipSanctionedCountryPortCallLast6m', 0),
                                'port_call_12m': ship.get('shipSanctionedCountryPortCallLast12m', 0),
                                'dark_activity': ship.get('shipDarkActivityIndicator', 0),
                                # Page 3
                                'sts_partner': ship.get('shipSTSPartnerNonComplianceLast12m', 0),
                                'flag_disputed': ship.get('shipFlagDisputed', 0),
                                'flag_sanctioned': ship.get('shipFlagSanctionedCountry', 0),
                                'flag_historical': ship.get('shipHistoricalFlagSanctionedCountry', 0),
                                'security_event': ship.get('shipSecurityLegalDisputeEvent', 0),
                                'not_maintained': ship.get('shipDetailsNoLongerMaintained', 0),
                                # Owner Sanctions (Pages 3-5)
                                'owner_australian': ship.get('shipOwnerAustralianSanctionList', 0),
                                'owner_bes': ship.get('shipOwnerBESSanctionList', 0),
                                'owner_canadian': ship.get('shipOwnerCanadianSanctionList', 0),
                                'owner_eu': ship.get('shipOwnerEUSanctionList', 0),
                                'owner_fatf': ship.get('shipOwnerFATFJurisdiction', 0),
                                'owner_ofac_ssi': ship.get('shipOwnerOFACSSIList', 0),
                                'owner_ofac': ship.get('shipOwnerOFACSanctionList', 0),
                                'owner_swiss': ship.get('shipOwnerSwissSanctionList', 0),
                                'owner_uae': ship.get('shipOwnerUAESanctionList', 0),
                                'owner_un': ship.get('shipOwnerUNSanctionList', 0),
                                'owner_ofac_country': ship.get('shipOwnerOFACSanctionedCountry', 0),
                                'owner_historical_ofac': ship.get('shipOwnerHistoricalOFACSanctionedCountry', 0),
                                'owner_parent_non_compliance': ship.get('shipOwnerParentCompanyNonCompliance', 0),
                                'owner_parent_ofac_country': ship.get('shipOwnerParentOFACSanctionedCountry', 0),
                                'owner_parent_fatf': ship.get('shipOwnerParentFATFJurisdiction', 0),
                                'cached_at': datetime.now(SGT).isoformat()
                            }
                time.sleep(0.5)
            
            # Mark checked but not found
            for imo in uncached:
                if imo not in received:
                    cache[imo] = {'legal_overall': 0, 'checked_but_not_found': True,
                                  'cached_at': datetime.now(SGT).isoformat()}
            
            st.session_state.risk_data_cache = cache
            save_cache(st.session_state.ship_static_cache, cache, st.session_state.get('mmsi_to_imo_cache', {}))
            
        except Exception as e:
            st.error(f"⚠️ S&P API error: {e}")
        
        return {imo: cache.get(imo, {}) for imo in imo_numbers}


class AISTracker:
    """AIS data collection and vessel tracking"""
    
    def __init__(self, use_cached_positions: bool = True):
        self.ships = defaultdict(lambda: {'latest_position': None, 'static_data': None})
        if use_cached_positions and 'vessel_positions' in st.session_state:
            for mmsi, data in st.session_state.vessel_positions.items():
                if mmsi != '_last_update':
                    self.ships[mmsi] = data
    
    def get_ship_color(self, legal_overall: int = -1) -> List[int]:
        colors = {2: [220, 53, 69, 200], 1: [255, 193, 7, 200], 0: [40, 167, 69, 200]}
        return colors.get(legal_overall, [128, 128, 128, 200])
    
    def save_positions_to_cache(self):
        positions = dict(self.ships)
        positions['_last_update'] = datetime.now(SGT).isoformat()
        st.session_state.vessel_positions = positions
        st.session_state.last_data_update = positions['_last_update']
        save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache,
                   st.session_state.get('mmsi_to_imo_cache', {}), positions)
    
    async def collect_data(self, duration: int = 30, api_key: str = "", bounding_box: List = None):
        if bounding_box is None:
            bounding_box = [[[0.5, 102.0], [2.5, 106.0]]]
        
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
                await ws.send(json.dumps({
                    "APIKey": api_key,
                    "BoundingBoxes": bounding_box,
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                }))
                start_time = time.time()
                
                async for message_json in ws:
                    if time.time() - start_time > duration: break
                    
                    msg = json.loads(message_json)
                    msg_type = msg.get("MessageType")
                    
                    if msg_type == "PositionReport":
                        self._process_position(msg)
                    elif msg_type == "ShipStaticData":
                        self._process_static(msg)
                
                self.save_positions_to_cache()
        except Exception as e:
            st.error(f"AIS connection error: {e}")
    
    def _process_position(self, msg: Dict):
        meta = msg.get('MetaData', {})
        pos = msg.get('Message', {}).get('PositionReport', {})
        mmsi = pos.get('UserID')
        if not mmsi: return
        
        self.ships[mmsi]['latest_position'] = {
            'latitude': pos.get('Latitude'), 'longitude': pos.get('Longitude'),
            'sog': pos.get('Sog', 0), 'cog': pos.get('Cog', 0),
            'true_heading': pos.get('TrueHeading', 511),
            'nav_status': pos.get('NavigationalStatus', 15),
            'ship_name': meta.get('ShipName', 'Unknown'),
            'timestamp': datetime.now(SGT).isoformat()
        }
        self.ships[mmsi]['last_seen'] = datetime.now(SGT).isoformat()
    
    def _process_static(self, msg: Dict):
        static = msg.get('Message', {}).get('ShipStaticData', {})
        mmsi = static.get('UserID')
        if not mmsi: return
        
        dim = static.get('Dimension', {})
        imo = str(static.get('ImoNumber', 0))
        existing = st.session_state.ship_static_cache.get(str(mmsi), {})
        
        dim_a = dim.get('A', 0) or existing.get('dimension_a', 0) or 0
        dim_b = dim.get('B', 0) or existing.get('dimension_b', 0) or 0
        dim_c = dim.get('C', 0) or existing.get('dimension_c', 0) or 0
        dim_d = dim.get('D', 0) or existing.get('dimension_d', 0) or 0
        
        if imo == '0' and existing.get('imo', '0') != '0':
            imo = existing.get('imo')
        
        info = {
            'name': static.get('Name') or existing.get('name', 'Unknown'),
            'imo': imo, 'type': static.get('Type') or existing.get('type'),
            'dimension_a': dim_a, 'dimension_b': dim_b, 'dimension_c': dim_c, 'dimension_d': dim_d,
            'length': dim_a + dim_b, 'width': dim_c + dim_d,
            'destination': static.get('Destination') or existing.get('destination', 'Unknown'),
            'call_sign': static.get('CallSign') or existing.get('call_sign', ''),
            'cached_at': datetime.now(SGT).isoformat()
        }
        
        self.ships[mmsi]['static_data'] = info
        st.session_state.ship_static_cache[str(mmsi)] = info
    
    def get_dataframe(self, sp_api=None, ships_api=None, expiry_hours=None) -> pd.DataFrame:
        """Get vessel dataframe with compliance data"""
        data = []
        now = datetime.now(SGT)
        
        for mmsi, ship in self.ships.items():
            pos = ship.get('latest_position')
            if not pos or not pos.get('latitude'): continue
            
            # Check expiry
            if expiry_hours:
                last_seen = ship.get('last_seen')
                if last_seen:
                    try:
                        hours = (now - datetime.fromisoformat(last_seen)).total_seconds() / 3600
                        if hours > expiry_hours: continue
                    except: pass
            
            static = ship.get('static_data') or st.session_state.ship_static_cache.get(str(mmsi), {})
            
            heading = pos.get('true_heading', 511)
            if heading == 511: heading = pos.get('cog', 0)
            
            length = static.get('length', 0) or 0
            width = static.get('width', 0) or 0
            has_dims = length > 0 and width > 0
            if not has_dims: length, width = 50, 10
            
            data.append({
                'mmsi': mmsi, 'name': (static.get('name') or pos.get('ship_name') or 'Unknown').strip(),
                'imo': str(static.get('imo', '0')),
                'latitude': pos.get('latitude'), 'longitude': pos.get('longitude'),
                'speed': pos.get('sog', 0), 'course': pos.get('cog', 0), 'heading': heading,
                'nav_status': pos.get('nav_status', 15),
                'nav_status_name': NAV_STATUS_NAMES.get(pos.get('nav_status', 15), 'Unknown'),
                'type': static.get('type'), 'type_name': get_vessel_type_category(static.get('type')),
                'length': length, 'width': width, 'has_dimensions': has_dims,
                'destination': (static.get('destination') or 'Unknown').strip(),
                'has_static': bool(static.get('name')),
                'last_seen': ship.get('last_seen', ''),
                # All compliance fields initialized to -1 (not checked)
                'legal_overall': -1,
                # Ship Sanctions
                'ship_bes': -1, 'ship_eu': -1, 'ship_ofac': -1, 'ship_ofac_non_sdn': -1,
                'ship_swiss': -1, 'ship_un': -1, 'ship_ofac_advisory': -1,
                # Port Calls & Dark Activity
                'port_3m': -1, 'port_6m': -1, 'port_12m': -1, 'dark_activity': -1,
                # STS, Flag, Security
                'sts_partner': -1, 'flag_disputed': -1, 'flag_sanctioned': -1,
                'flag_historical': -1, 'security_event': -1, 'not_maintained': -1,
                # Owner Sanctions
                'owner_australian': -1, 'owner_bes': -1, 'owner_canadian': -1, 'owner_eu': -1,
                'owner_fatf': -1, 'owner_ofac_ssi': -1, 'owner_ofac': -1, 'owner_swiss': -1,
                'owner_uae': -1, 'owner_un': -1, 'owner_ofac_country': -1, 'owner_historical_ofac': -1,
                'owner_parent_nc': -1, 'owner_parent_ofac': -1, 'owner_parent_fatf': -1,
                'color': self.get_ship_color(-1)
            })
        
        df = pd.DataFrame(data)
        if len(df) == 0: return df
        
        # Get IMOs and compliance data
        valid_imos = [imo for imo in df['imo'].unique() if imo and imo != '0']
        missing_imos = df[df['imo'] == '0']['mmsi'].astype(str).unique().tolist()
        
        # MMSI -> IMO lookup
        if missing_imos and ships_api:
            mmsi_imos = ships_api.batch_get_imo_by_mmsi(missing_imos)
            for idx, row in df.iterrows():
                found = mmsi_imos.get(str(row['mmsi']))
                if found:
                    df.at[idx, 'imo'] = found
                    if found not in valid_imos: valid_imos.append(found)
        elif missing_imos and not ships_api:
            # Use cache only
            cache = st.session_state.get('mmsi_to_imo_cache', {})
            for idx, row in df.iterrows():
                found = cache.get(str(row['mmsi']))
                if found:
                    df.at[idx, 'imo'] = found
                    if found not in valid_imos: valid_imos.append(found)
        
        # Get compliance data
        compliance_cache = st.session_state.get('risk_data_cache', {})
        compliance_data = sp_api.get_ship_compliance_data(valid_imos) if sp_api and valid_imos else {
            imo: compliance_cache.get(imo, {}) for imo in valid_imos
        }
        
        # Apply compliance to dataframe - map all fields from API response
        field_mapping = {
            # Ship Sanctions
            'ship_bes': 'ship_bes_sanction', 'ship_eu': 'ship_eu_sanction',
            'ship_ofac': 'ship_ofac_sanction', 'ship_ofac_non_sdn': 'ship_ofac_non_sdn',
            'ship_swiss': 'ship_swiss_sanction', 'ship_un': 'ship_un_sanction',
            'ship_ofac_advisory': 'ship_ofac_advisory',
            # Port Calls & Dark Activity
            'port_3m': 'port_call_3m', 'port_6m': 'port_call_6m', 'port_12m': 'port_call_12m',
            'dark_activity': 'dark_activity',
            # STS, Flag, Security
            'sts_partner': 'sts_partner', 'flag_disputed': 'flag_disputed',
            'flag_sanctioned': 'flag_sanctioned', 'flag_historical': 'flag_historical',
            'security_event': 'security_event', 'not_maintained': 'not_maintained',
            # Owner Sanctions
            'owner_australian': 'owner_australian', 'owner_bes': 'owner_bes',
            'owner_canadian': 'owner_canadian', 'owner_eu': 'owner_eu',
            'owner_fatf': 'owner_fatf', 'owner_ofac_ssi': 'owner_ofac_ssi',
            'owner_ofac': 'owner_ofac', 'owner_swiss': 'owner_swiss',
            'owner_uae': 'owner_uae', 'owner_un': 'owner_un',
            'owner_ofac_country': 'owner_ofac_country', 'owner_historical_ofac': 'owner_historical_ofac',
            'owner_parent_nc': 'owner_parent_non_compliance',
            'owner_parent_ofac': 'owner_parent_ofac_country', 'owner_parent_fatf': 'owner_parent_fatf',
        }
        
        for idx, row in df.iterrows():
            comp = compliance_data.get(str(row['imo']), {})
            if comp:
                legal = comp.get('legal_overall', -1)
                if isinstance(legal, str): legal = int(legal) if legal.isdigit() else -1
                df.at[idx, 'legal_overall'] = legal
                
                for df_field, api_field in field_mapping.items():
                    val = comp.get(api_field, -1)
                    df.at[idx, df_field] = int(val) if val is not None else -1
                
                df.at[idx, 'color'] = self.get_ship_color(legal)
        
        return df


def create_vessel_polygon(lat, lon, heading, length, width, zoom, dim_a=0, dim_b=0, dim_c=0, dim_d=0):
    """Create ship polygon with pointed bow"""
    scale = max(1, 16 - zoom) * 1.5 if zoom < 16 else 1
    
    if dim_a > 0 and dim_b > 0:
        bow_offset = (dim_a - length/2) * scale
        stern_offset = (dim_b - length/2) * scale
    else:
        bow_offset = length * 0.5 * scale
        stern_offset = -length * 0.5 * scale
    
    half_width = width / 2 * scale
    bow_taper = length * 0.15 * scale
    
    # Ship shape points (bow at top)
    points = [
        (bow_offset, 0),  # Bow point
        (bow_offset - bow_taper, half_width * 0.7),
        (stern_offset + length * 0.1 * scale, half_width),
        (stern_offset, half_width * 0.9),
        (stern_offset, -half_width * 0.9),
        (stern_offset + length * 0.1 * scale, -half_width),
        (bow_offset - bow_taper, -half_width * 0.7),
    ]
    
    # Convert to lat/lon
    heading_rad = math.radians(heading)
    m_per_deg_lat = 111320
    m_per_deg_lon = m_per_deg_lat * math.cos(math.radians(lat))
    
    polygon = []
    for dx, dy in points:
        rot_x = dx * math.cos(heading_rad) - dy * math.sin(heading_rad)
        rot_y = dx * math.sin(heading_rad) + dy * math.cos(heading_rad)
        new_lat = lat + (rot_x / m_per_deg_lat)
        new_lon = lon + (rot_y / m_per_deg_lon)
        polygon.append([new_lon, new_lat])
    polygon.append(polygon[0])
    return polygon


def create_vessel_layers(df: pd.DataFrame, zoom: float = 10) -> List[pdk.Layer]:
    """Create vessel map layers"""
    if len(df) == 0: return []
    
    vessel_data = []
    for _, row in df.iterrows():
        length = row['length'] if 0 < row['length'] < 500 else 50
        width = row['width'] if 0 < row['width'] < 80 else 10
        
        legal_emoji = {2: '🔴', 1: '🟡', 0: '✅'}.get(row['legal_overall'], '❓')
        dim_text = f"{length:.0f}m x {width:.0f}m" + ("" if row['has_dimensions'] else " (est.)")
        
        tooltip = (f"<b>{row['name']}</b><br/>IMO: {row['imo']}<br/>MMSI: {row['mmsi']}<br/>"
                   f"Type: {row['type_name']}<br/>Size: {dim_text}<br/>"
                   f"Speed: {row['speed']:.1f} kts<br/>Status: {row['nav_status_name']}<br/>"
                   f"Compliance: {legal_emoji}")
        
        vessel_data.append({
            'latitude': row['latitude'], 'longitude': row['longitude'],
            'name': row['name'], 'tooltip': tooltip, 'color': row['color'],
            'heading': row['heading'], 'length': length, 'width': width
        })
    
    if zoom < 12:
        return [pdk.Layer('ScatterplotLayer', data=vessel_data,
                          get_position=['longitude', 'latitude'], get_fill_color='color',
                          get_radius=300, radius_min_pixels=4, radius_max_pixels=15, pickable=True)]
    else:
        polygons = []
        for v in vessel_data:
            poly = create_vessel_polygon(v['latitude'], v['longitude'], v['heading'],
                                         v['length'], v['width'], zoom)
            polygons.append({'polygon': poly, 'tooltip': v['tooltip'], 'color': v['color']})
        return [pdk.Layer('PolygonLayer', data=polygons, get_polygon='polygon',
                          get_fill_color='color', get_line_color=[50, 50, 50, 100],
                          line_width_min_pixels=1, pickable=True)]


# ==================== MAIN APP ====================

st.title("🚢 Singapore Strait Ship Risk Tracker")

# Sidebar
st.sidebar.title("⚙️ Settings")

# API Credentials
try:
    sp_username = st.secrets["sp_maritime"]["username"]
    sp_password = st.secrets["sp_maritime"]["password"]
    ais_api_key = st.secrets.get("aisstream", {}).get("api_key", "")
    st.sidebar.success("🔐 Using credentials from secrets")
except:
    with st.sidebar.expander("🔐 API Credentials"):
        sp_username = st.text_input("S&P Username", type="password")
        sp_password = st.text_input("S&P Password", type="password")
        ais_api_key = st.text_input("AISStream API Key", type="password")

# AIS Settings
st.sidebar.header("📡 AIS Settings")
duration = st.sidebar.slider("Collection time (seconds)", 10, 120, 60)
enable_compliance = st.sidebar.checkbox("Enable S&P compliance", value=True)

coverage_options = {
    "Singapore Strait Only": [[[1.15, 103.55], [1.50, 104.10]]],
    "Singapore + Approaches": [[[1.0, 103.3], [1.6, 104.3]]],
    "Malacca to SCS (Dark Fleet)": [[[0.5, 102.0], [2.5, 106.0]]],
    "Extended Malacca": [[[-0.5, 100.0], [3.0, 106.0]]],
    "Full Regional": [[[-1.0, 99.0], [4.0, 108.0]]]
}
selected_coverage = st.sidebar.selectbox("Coverage area", list(coverage_options.keys()), index=2)
coverage_bbox = coverage_options[selected_coverage]

expiry_options = {"1 hour": 1, "2 hours": 2, "4 hours": 4, "8 hours": 8, "12 hours": 12, "24 hours": 24, "Never": None}
vessel_expiry_hours = expiry_options[st.sidebar.selectbox("Vessel expiry", list(expiry_options.keys()), index=2)]
st.sidebar.caption("💡 Run multiple refreshes to accumulate vessels. Old vessels auto-expire.")

# Maritime Zones
st.sidebar.header("🗺️ Maritime Zones")
show_anchorages = st.sidebar.checkbox("Anchorages", value=True)
show_channels = st.sidebar.checkbox("Channels", value=True)
show_fairways = st.sidebar.checkbox("Fairways", value=True)

zoom_level = st.sidebar.slider("Zoom Level", 6, 18, st.session_state.get('user_zoom', 10))
st.session_state.user_zoom = zoom_level

# Load zones
maritime_zones = {"Anchorages": [], "Channels": [], "Fairways": []}
for path in ["/mnt/project/Anchorages_Channels_Fairways_Details.xlsx",
             "/mnt/user-data/uploads/Anchorages_Channels_Fairways_Details.xlsx"]:
    if os.path.exists(path):
        maritime_zones = load_maritime_zones(path)
        break

# Filters
st.sidebar.header("🔍 Filters")

quick_filter = st.sidebar.radio("Quick Filter", ["All Vessels", "Dark Fleet Focus", "Sanctioned Only", "Custom"], horizontal=True)
defaults = {
    "Dark Fleet Focus": (["Severe (🔴)", "Caution (🟡)"], ["UN Sanctions", "OFAC Sanctions", "Dark Activity"], ["Tanker", "Cargo"]),
    "Sanctioned Only": (["Severe (🔴)"], ["UN Sanctions", "OFAC Sanctions"], ["All"]),
}.get(quick_filter, (["All"], ["All"], ["All"]))

selected_compliance = st.sidebar.multiselect("Compliance", ["All", "Severe (🔴)", "Caution (🟡)", "Clear (✅)"], default=defaults[0])
selected_sanctions = st.sidebar.multiselect("Sanctions", ["All", "UN Sanctions", "OFAC Sanctions", "Dark Activity"], default=defaults[1])
vessel_types = ["All", "Cargo", "Tanker", "Passenger", "Tug", "Fishing", "High Speed Craft", "Pilot", "SAR", "Other", "Unknown"]
selected_types = st.sidebar.multiselect("Vessel Types", vessel_types, default=defaults[2])
selected_nav = st.sidebar.multiselect("Nav Status", ["All"] + list(NAV_STATUS_NAMES.values()), default=["All"])
show_static_only = st.sidebar.checkbox("With static data only")

# Cache stats
st.sidebar.header("💾 Cache")
vessel_count = len([k for k in st.session_state.get('vessel_positions', {}).keys() if k != '_last_update'])
st.sidebar.info(f"**Vessels:** {vessel_count} | **Compliance:** {len(st.session_state.risk_data_cache)}")

col1, col2 = st.sidebar.columns(2)
if col1.button("🗑️ Clear"):
    st.session_state.ship_static_cache = {}
    st.session_state.risk_data_cache = {}
    st.session_state.mmsi_to_imo_cache = {}
    st.session_state.vessel_positions = {}
    save_cache({}, {}, {}, {})
    st.rerun()
if col2.button("🔄 Retry IMO"):
    st.session_state.mmsi_to_imo_cache = {}
    st.rerun()

# Legend
st.sidebar.markdown("---")
st.sidebar.markdown("""
### 🎨 Legend
**Vessels:** 🔴 Severe | 🟡 Caution | 🟢 Clear | ⬜ Unknown  
**Zones:** 🔵 Anchorages | 🟡 Channels | 🟠 Fairways
""")

# Main content
status_placeholder = st.empty()
stats_placeholder = st.empty()
map_placeholder = st.empty()
table_placeholder = st.empty()


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    if len(df) == 0: return df
    filtered = df.copy()
    
    if selected_compliance and "All" not in selected_compliance:
        levels = {"Severe (🔴)": 2, "Caution (🟡)": 1, "Clear (✅)": 0}
        filtered = filtered[filtered['legal_overall'].isin([levels[c] for c in selected_compliance if c in levels])]
    
    if selected_sanctions and "All" not in selected_sanctions:
        mask = pd.Series([False] * len(filtered), index=filtered.index)
        if "UN Sanctions" in selected_sanctions: mask |= (filtered['ship_un'] == 2)
        if "OFAC Sanctions" in selected_sanctions: mask |= (filtered['ship_ofac'] == 2)
        if "Dark Activity" in selected_sanctions: mask |= (filtered['dark_activity'] >= 1)
        filtered = filtered[mask]
    
    if selected_types and "All" not in selected_types:
        filtered = filtered[filtered['type_name'].isin(selected_types)]
    
    if selected_nav and "All" not in selected_nav:
        filtered = filtered[filtered['nav_status_name'].isin(selected_nav)]
    
    if show_static_only:
        filtered = filtered[filtered['has_static']]
    
    return filtered


def display_data(df: pd.DataFrame, last_update: str, is_cached: bool = False):
    """Display map and table"""
    # Stats
    with stats_placeholder:
        cols = st.columns(8)
        cols[0].metric("🚢 Total", len(df))
        cols[1].metric("⚡ Moving", len(df[df['speed'] > 1]) if len(df) else 0)
        cols[2].metric("📡 Static", int(df['has_static'].sum()) if len(df) else 0)
        cols[3].metric("🔴 Severe", len(df[df['legal_overall'] == 2]) if len(df) else 0)
        cols[4].metric("🟡 Caution", len(df[df['legal_overall'] == 1]) if len(df) else 0)
        cols[5].metric("✅ Clear", len(df[df['legal_overall'] == 0]) if len(df) else 0)
        cols[6].metric("❓ Unknown", len(df[df['legal_overall'] == -1]) if len(df) else 0)
        cols[7].metric("📐 Dims", int(df['has_dimensions'].sum()) if len(df) else 0)
    
    # Map
    center_lat, center_lon = 1.28, 103.85
    if st.session_state.selected_vessel and len(df):
        vessel = df[df['mmsi'] == st.session_state.selected_vessel]
        if len(vessel):
            center_lat, center_lon = vessel.iloc[0]['latitude'], vessel.iloc[0]['longitude']
    
    layers = []
    if show_anchorages and maritime_zones['Anchorages']:
        layer = create_zone_layer(maritime_zones['Anchorages'], [0, 255, 255, 50], "anc")
        if layer: layers.append(layer)
    if show_channels and maritime_zones['Channels']:
        layer = create_zone_layer(maritime_zones['Channels'], [255, 255, 0, 50], "ch")
        if layer: layers.append(layer)
    if show_fairways and maritime_zones['Fairways']:
        layer = create_zone_layer(maritime_zones['Fairways'], [255, 165, 0, 50], "fw")
        if layer: layers.append(layer)
    
    layers.extend(create_vessel_layers(df, zoom_level))
    
    with map_placeholder:
        st.pydeck_chart(pdk.Deck(
            map_style='', layers=layers,
            initial_view_state=pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=zoom_level),
            tooltip={'html': '{tooltip}', 'style': {'backgroundColor': 'steelblue', 'color': 'white'}}
        ), use_container_width=True)
    
    # Table
    with table_placeholder:
        if len(df) == 0:
            st.info("No vessels to display.")
        else:
            # Sort: Severe > Caution > Unknown > Clear
            df['_sort'] = df['legal_overall'].map({2: 3, 1: 2, -1: 1, 0: 0}).fillna(1)
            display_df = df.sort_values(['_sort', 'name'], ascending=[False, True]).drop(columns=['_sort'])
            
            # Format all compliance columns with emojis
            compliance_cols = [
                'legal_overall',
                # Ship Sanctions (PDF Page 1)
                'ship_bes', 'ship_eu', 'ship_ofac', 'ship_ofac_non_sdn', 'ship_swiss', 'ship_un',
                # Page 2
                'ship_ofac_advisory', 'port_3m', 'port_6m', 'port_12m', 'dark_activity',
                # Page 3
                'sts_partner', 'flag_disputed', 'flag_sanctioned', 'flag_historical',
                'security_event', 'not_maintained',
                # Owner Sanctions (Pages 3-5)
                'owner_australian', 'owner_bes', 'owner_canadian', 'owner_eu', 'owner_fatf',
                'owner_ofac_ssi', 'owner_ofac', 'owner_swiss', 'owner_uae', 'owner_un',
                'owner_ofac_country', 'owner_historical_ofac',
                'owner_parent_nc', 'owner_parent_ofac', 'owner_parent_fatf'
            ]
            
            for col in compliance_cols:
                if col in display_df.columns:
                    display_df[f'{col}_fmt'] = display_df[col].apply(format_compliance_value)
            
            # Build table with columns in PDF order
            # Core info + Ship Sanctions + Port/Dark + STS/Flag + Owner Sanctions
            table_df = display_df[[
                'name', 'imo', 'mmsi', 'type_name', 'nav_status_name',
                'legal_overall_fmt',
                # Ship Sanctions
                'ship_bes_fmt', 'ship_eu_fmt', 'ship_ofac_fmt', 'ship_ofac_non_sdn_fmt', 
                'ship_swiss_fmt', 'ship_un_fmt', 'ship_ofac_advisory_fmt',
                # Port Calls & Dark
                'port_3m_fmt', 'port_6m_fmt', 'port_12m_fmt', 'dark_activity_fmt',
                # STS & Flag
                'sts_partner_fmt', 'flag_disputed_fmt', 'flag_sanctioned_fmt', 'flag_historical_fmt',
                'security_event_fmt', 'not_maintained_fmt',
                # Owner Sanctions
                'owner_australian_fmt', 'owner_bes_fmt', 'owner_canadian_fmt', 'owner_eu_fmt',
                'owner_fatf_fmt', 'owner_ofac_ssi_fmt', 'owner_ofac_fmt', 'owner_swiss_fmt',
                'owner_uae_fmt', 'owner_un_fmt', 'owner_ofac_country_fmt', 'owner_historical_ofac_fmt',
                'owner_parent_nc_fmt', 'owner_parent_ofac_fmt', 'owner_parent_fatf_fmt'
            ]].copy()
            
            table_df.columns = [
                'Name', 'IMO', 'MMSI', 'Type', 'Nav Status',
                'Legal',
                # Ship Sanctions
                'UK', 'EU', 'OFAC', 'OFAC NS', 'Swiss', 'UN', 'OFAC Adv',
                # Port Calls & Dark
                'Port3m', 'Port6m', 'Port12m', 'Dark',
                # STS & Flag
                'STS', 'FlagDisp', 'FlagSanc', 'FlagHist', 'SecEvt', 'NoMaint',
                # Owner Sanctions
                'OwnAU', 'OwnUK', 'OwnCA', 'OwnEU', 'OwnFATF', 'OwnSSI', 'OwnOFAC',
                'OwnSwiss', 'OwnUAE', 'OwnUN', 'OwnOFACCty', 'OwnHistOFAC',
                'ParentNC', 'ParentOFAC', 'ParentFATF'
            ]
            
            selected = st.dataframe(table_df, use_container_width=True, height=400, hide_index=True,
                                    on_select="rerun", selection_mode="single-row")
            
            if selected and selected.selection and selected.selection.rows:
                idx = selected.selection.rows[0]
                mmsi = display_df.iloc[idx]['mmsi']
                st.info(f"Selected: **{table_df.iloc[idx]['Name']}** (IMO: {table_df.iloc[idx]['IMO']})")
                if st.button("🗺️ View on Map"):
                    st.session_state.selected_vessel = mmsi
                    st.rerun()
    
    st.success(f"✅ Last updated: {format_datetime(last_update)} SGT{' 📦 (cached)' if is_cached else ''}")


# Action buttons
col1, col2 = st.columns(2)

if col1.button("🔄 Refresh Now", type="primary", use_container_width=True):
    sp_api = SPMaritimeAPI(sp_username, sp_password) if enable_compliance and sp_username else None
    ships_api = SPShipsAPI(sp_username, sp_password) if enable_compliance and sp_username else None
    
    with status_placeholder:
        with st.spinner(f'Collecting AIS data for {duration}s...'):
            tracker = AISTracker(use_cached_positions=True)
            if ais_api_key:
                asyncio.run(tracker.collect_data(duration, ais_api_key, coverage_bbox))
            else:
                st.warning("No AISStream API key")
            df = tracker.get_dataframe(sp_api, ships_api, vessel_expiry_hours)
    
    status_placeholder.empty()
    if not df.empty:
        df = apply_filters(df)
        display_data(df, st.session_state.get('last_data_update', ''), is_cached=False)
    else:
        st.warning("No ships detected.")

elif col2.button("📋 View Cached", use_container_width=True):
    if 'vessel_positions' in st.session_state and st.session_state.vessel_positions:
        tracker = AISTracker(use_cached_positions=True)
        df = tracker.get_dataframe(sp_api=None, ships_api=None, expiry_hours=vessel_expiry_hours)
        df = apply_filters(df)
        display_data(df, st.session_state.vessel_positions.get('_last_update', ''), is_cached=True)
    else:
        st.info("No cached data. Click 'Refresh Now' first.")
else:
    # Auto-display cached on load
    if 'vessel_positions' in st.session_state and st.session_state.vessel_positions:
        tracker = AISTracker(use_cached_positions=True)
        df = tracker.get_dataframe(sp_api=None, ships_api=None, expiry_hours=vessel_expiry_hours)
        df = apply_filters(df)
        if not df.empty:
            display_data(df, st.session_state.vessel_positions.get('_last_update', ''), is_cached=True)
        else:
            st.info("No vessels match filters. Adjust filters or click 'Refresh Now'.")
    else:
        st.info("👆 Click 'Refresh Now' to start collecting AIS data")
