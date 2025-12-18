"""
Singapore AIS Tracker with S&P Maritime Risk Intelligence
STABLE VERSION - Before maritime zones addition
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
from typing import List, Dict, Any
import pickle
import os

st.set_page_config(
    page_title="Singapore Ship Risk Tracker",
    page_icon="🚢",
    layout="wide"
)

# File-based persistent storage
STORAGE_FILE = "ship_data_cache.pkl"
RISK_DATA_FILE = "risk_data_cache.pkl"

def load_cache():
    """Load cached ship and risk data from disk"""
    ship_cache = {}
    risk_cache = {}
    
    if os.path.exists(STORAGE_FILE):
        try:
            with open(STORAGE_FILE, 'rb') as f:
                ship_cache = pickle.load(f)
        except:
            pass
    
    if os.path.exists(RISK_DATA_FILE):
        try:
            with open(RISK_DATA_FILE, 'rb') as f:
                risk_cache = pickle.load(f)
        except:
            pass
    
    return ship_cache, risk_cache

def save_cache(ship_cache, risk_cache):
    """Save ship and risk data to disk"""
    try:
        with open(STORAGE_FILE, 'wb') as f:
            pickle.dump(ship_cache, f)
        with open(RISK_DATA_FILE, 'wb') as f:
            pickle.dump(risk_cache, f)
    except Exception as e:
        st.warning(f"Could not save cache: {e}")

# Initialize session state
if 'ship_static_cache' not in st.session_state:
    ship_cache, risk_cache = load_cache()
    st.session_state.ship_static_cache = ship_cache
    st.session_state.risk_data_cache = risk_cache
    st.session_state.last_save = time.time()
    st.session_state.last_collection = None
    st.session_state.current_df = None
    st.session_state.selected_vessel = None

# S&P Maritime API Integration
class SPMaritimeAPI:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_url = "https://maritimewebservices.ihs.com/MaritimeWCF/APSShipService.svc/RESTFul/GetShipsByIHSLRorIMONumbersAll"
    
    def get_ship_risk_data(self, imo_numbers: List[str]) -> Dict[str, Dict]:
        """Get complete compliance screening for multiple IMO numbers"""
        if not imo_numbers:
            return {}
        
        cache = st.session_state.risk_data_cache
        uncached_imos = [imo for imo in imo_numbers if imo not in cache]
        
        if not uncached_imos:
            return {imo: cache[imo] for imo in imo_numbers}
        
        st.info(f"🔍 Fetching S&P compliance data for {len(uncached_imos)} vessels...")
        
        try:
            batches = [uncached_imos[i:i+100] for i in range(0, len(uncached_imos), 100)]
            
            for batch in batches:
                imo_string = ','.join(batch)
                encoded_imo = quote(imo_string)
                url = f"{self.base_url}?imoNumbers={encoded_imo}"
                
                response = requests.get(
                    url,
                    auth=(self.username, self.password),
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'ShipResult' in data:
                        for ship in data['ShipResult']:
                            if 'APSShipDetail' in ship:
                                detail = ship['APSShipDetail']
                                imo = str(detail.get('IHSLRorIMOShipNo', ''))
                                
                                compliance_data = self._extract_complete_compliance(detail)
                                cache[imo] = compliance_data
                
                time.sleep(0.5)
            
            st.session_state.risk_data_cache = cache
            save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache)
            st.success(f"✅ Cached compliance data for {len(uncached_imos)} vessels")
                
        except Exception as e:
            st.error(f"⚠️ S&P API error: {str(e)}")
        
        return {imo: cache.get(imo, {}) for imo in imo_numbers}
    
    def _extract_complete_compliance(self, detail: Dict) -> Dict:
        """Extract complete S&P compliance screening"""
        
        def parse_value(val):
            if val == '2' or val == 2:
                return 2
            elif val == '1' or val == 1:
                return 1
            else:
                return 0
        
        ship_un = parse_value(detail.get('ShipUNSanctionList', '0'))
        ship_ofac = parse_value(detail.get('ShipOFACSanctionList', '0'))
        dark_activity = parse_value(detail.get('ShipDarkActivityIndicator', '0'))
        flag_disputed = parse_value(detail.get('ShipFlagDisputed', '0'))
        port_call_3m = parse_value(detail.get('ShipSanctionedCountryPortCallLast3m', '0'))
        owner_un = parse_value(detail.get('ShipOwnerUNSanctionList', '0'))
        owner_ofac = parse_value(detail.get('ShipOwnerOFACSanctionList', '0'))
        
        all_scores = [ship_un, ship_ofac, dark_activity, flag_disputed, port_call_3m, owner_un, owner_ofac]
        legal_overall = max(all_scores)
        
        return {
            'ship_name': detail.get('ShipName', ''),
            'flag_name': detail.get('FlagName', ''),
            'registered_owner': detail.get('RegisteredOwner', ''),
            'legal_overall': legal_overall,
            'ship_un_sanction': ship_un,
            'ship_ofac_sanction': ship_ofac,
            'dark_activity': dark_activity,
            'flag_disputed': flag_disputed,
            'port_call_3m': port_call_3m,
            'owner_un': owner_un,
            'owner_ofac': owner_ofac,
            'cached_at': datetime.now().isoformat()
        }

# AIS Tracker
class AISTracker:
    def __init__(self):
        self.ships = defaultdict(lambda: {
            'latest_position': None,
            'static_data': None
        })
    
    def get_ship_color(self, type_code, legal_overall=0):
        """Return color based on S&P Legal Overall status"""
        if legal_overall == 2:
            return [255, 0, 0, 220]
        elif legal_overall == 1:
            return [255, 165, 0, 220]
        elif type_code:
            if 60 <= type_code <= 69:
                return [0, 0, 255, 180]
            elif 70 <= type_code <= 79:
                return [100, 200, 100, 180]
            elif 80 <= type_code <= 89:
                return [139, 0, 139, 180]
            elif type_code == 52:
                return [128, 128, 0, 180]
        
        return [0, 255, 0, 180]
    
    async def collect_data(self, duration=30, api_key="e38db7cbbfbd792829696a346f41a6630d74c53d"):
        async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
            subscription = {
                "APIKey": api_key,
                "BoundingBoxes": [[[1.15, 103.55], [1.50, 104.10]]],
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
    
    def process_position(self, ais_message):
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
    
    def process_static(self, ais_message):
        static_data = ais_message.get('Message', {}).get('ShipStaticData', {})
        
        mmsi = static_data.get('UserID')
        if not mmsi:
            return
        
        dimension = static_data.get('Dimension', {})
        imo = str(static_data.get('ImoNumber', 0))
        
        static_info = {
            'name': static_data.get('Name', 'Unknown'),
            'imo': imo,
            'type': static_data.get('Type'),
            'length': dimension.get('A', 0) + dimension.get('B', 0),
            'dimension_a': dimension.get('A', 0),
            'dimension_b': dimension.get('B', 0),
            'dimension_c': dimension.get('C', 0),
            'dimension_d': dimension.get('D', 0),
            'destination': static_data.get('Destination', 'Unknown'),
            'call_sign': static_data.get('CallSign', ''),
            'cached_at': datetime.now().isoformat()
        }
        
        self.ships[mmsi]['static_data'] = static_info
        st.session_state.ship_static_cache[str(mmsi)] = static_info
        
        if time.time() - st.session_state.last_save > 60:
            save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache)
            st.session_state.last_save = time.time()
    
    def get_dataframe_with_risk(self, sp_api: SPMaritimeAPI) -> pd.DataFrame:
        """Get dataframe with complete S&P compliance data"""
        data = []
        
        for mmsi, ship_data in self.ships.items():
            pos = ship_data.get('latest_position')
            static = ship_data.get('static_data') or st.session_state.ship_static_cache.get(str(mmsi), {})
            
            if not pos or pos.get('latitude') is None or pos.get('longitude') is None:
                continue
            
            name = static.get('name') or pos.get('ship_name') or 'Unknown'
            name = name.strip() if name else 'Unknown'
            
            ship_type = static.get('type')
            imo = str(static.get('imo', '0'))
            
            dimension_a = static.get('dimension_a', 0) if static.get('dimension_a') else 0
            dimension_b = static.get('dimension_b', 0) if static.get('dimension_b') else 0
            dimension_c = static.get('dimension_c', 0) if static.get('dimension_c') else 0
            dimension_d = static.get('dimension_d', 0) if static.get('dimension_d') else 0
            
            true_heading = pos.get('true_heading', 511)
            heading = true_heading if true_heading != 511 else pos.get('cog', 0)
            
            data.append({
                'mmsi': mmsi,
                'name': name,
                'imo': imo,
                'latitude': pos.get('latitude'),
                'longitude': pos.get('longitude'),
                'speed': pos.get('sog', 0),
                'course': pos.get('cog', 0),
                'heading': heading,
                'type': ship_type,
                'length': dimension_a + dimension_b,
                'width': dimension_c + dimension_d,
                'dimension_a': dimension_a,
                'dimension_b': dimension_b,
                'dimension_c': dimension_c,
                'dimension_d': dimension_d,
                'destination': (static.get('destination') or 'Unknown').strip(),
                'call_sign': static.get('call_sign', ''),
                'nav_status': pos.get('nav_status', 15),
                'legal_overall': 0,
                'has_static': bool(static.get('name')),
                'color': self.get_ship_color(ship_type, 0)
            })
        
        df = pd.DataFrame(data)
        
        if len(df) == 0:
            return df
        
        valid_imos = [str(imo) for imo in df['imo'].unique() if imo and imo != '0']
        
        if valid_imos and sp_api:
            risk_data = sp_api.get_ship_risk_data(valid_imos)
            
            for idx, row in df.iterrows():
                imo = str(row['imo'])
                if imo in risk_data and risk_data[imo]:
                    risk_info = risk_data[imo]
                    legal_overall = risk_info.get('legal_overall', 0)
                    
                    df.at[idx, 'legal_overall'] = legal_overall
                    df.at[idx, 'flag_name'] = risk_info.get('flag_name', '')
                    df.at[idx, 'registered_owner'] = risk_info.get('registered_owner', '')
                    df.at[idx, 'ship_un_sanction'] = risk_info.get('ship_un_sanction', 0)
                    df.at[idx, 'ship_ofac_sanction'] = risk_info.get('ship_ofac_sanction', 0)
                    df.at[idx, 'dark_activity'] = risk_info.get('dark_activity', 0)
                    df.at[idx, 'flag_disputed'] = risk_info.get('flag_disputed', 0)
                    df.at[idx, 'port_call_3m'] = risk_info.get('port_call_3m', 0)
                    df.at[idx, 'owner_un'] = risk_info.get('owner_un', 0)
                    df.at[idx, 'owner_ofac'] = risk_info.get('owner_ofac', 0)
                    df.at[idx, 'color'] = self.get_ship_color(row['type'], legal_overall)
        
        df['type_name'] = df['type'].apply(self._get_vessel_type_name)
        df['nav_status_name'] = df['nav_status'].apply(self._get_nav_status_name)
        df['vessel_polygon'] = df.apply(self._create_vessel_polygon, axis=1)
        
        return df
    
    def _create_vessel_polygon(self, row):
        """Create vessel polygon using dimensions"""
        try:
            lat = row['latitude']
            lon = row['longitude']
            heading = row['heading']
            
            dim_a = row['dimension_a'] if row['dimension_a'] > 0 else 30
            dim_b = row['dimension_b'] if row['dimension_b'] > 0 else 30
            dim_c = row['dimension_c'] if row['dimension_c'] > 0 else 8
            dim_d = row['dimension_d'] if row['dimension_d'] > 0 else 8
            
            import math
            heading_rad = math.radians(heading if heading != 511 else 0)
            
            corners = [
                (-dim_c, dim_a),
                (dim_d, dim_a),
                (dim_d, -dim_b),
                (-dim_c, -dim_b),
            ]
            
            polygon = []
            for x, y in corners:
                rotated_x = x * math.cos(heading_rad) - y * math.sin(heading_rad)
                rotated_y = x * math.sin(heading_rad) + y * math.cos(heading_rad)
                
                lat_offset = rotated_y / 111111.0
                lon_offset = rotated_x / (111111.0 * math.cos(math.radians(lat)))
                
                polygon.append([lon + lon_offset, lat + lat_offset])
            
            return polygon
        except:
            return []
    
    def _get_vessel_type_name(self, type_code):
        """Convert AIS type code to readable name"""
        if pd.isna(type_code) or type_code == 0:
            return 'Unknown'
        
        type_code = int(type_code)
        
        if type_code == 30:
            return 'Fishing'
        elif type_code in [31, 32]:
            return 'Towing'
        elif 40 <= type_code <= 49:
            return 'High Speed Craft'
        elif type_code == 50:
            return 'Pilot'
        elif type_code == 52:
            return 'Tug'
        elif 60 <= type_code <= 69:
            return 'Passenger'
        elif 70 <= type_code <= 79:
            return 'Cargo'
        elif 80 <= type_code <= 89:
            return 'Tanker'
        else:
            return 'Other'
    
    def _get_nav_status_name(self, nav_status_code):
        """Convert navigational status code to readable name"""
        if pd.isna(nav_status_code):
            return 'Unknown'
        
        status_map = {
            0: 'Under way using engine',
            1: 'At anchor',
            2: 'Not under command',
            3: 'Restricted maneuverability',
            4: 'Constrained by draught',
            5: 'Moored',
            6: 'Aground',
            7: 'Engaged in fishing',
            8: 'Under way sailing',
            15: 'Unknown'
        }
        
        return status_map.get(int(nav_status_code), 'Unknown')


# Streamlit UI
st.title("🚢 Singapore Ship Risk Tracker")
st.markdown("Real-time vessel tracking with **S&P Global Compliance Screening**")

# Sidebar
st.sidebar.header("⚙️ Configuration")

try:
    sp_username = st.secrets["sp_maritime"]["username"]
    sp_password = st.secrets["sp_maritime"]["password"]
    ais_api_key = st.secrets.get("aisstream", {}).get("api_key", "e38db7cbbfbd792829696a346f41a6630d74c53d")
    st.sidebar.success("🔐 Using credentials from secrets")
except:
    with st.sidebar.expander("🔐 S&P Maritime API", expanded=False):
        st.warning("⚠️ Credentials should be in Streamlit Secrets")
        sp_username = st.text_input("Username", type="password")
        sp_password = st.text_input("Password", type="password")
    ais_api_key = "e38db7cbbfbd792829696a346f41a6630d74c53d"

duration = st.sidebar.slider("AIS collection time (seconds)", 10, 60, 30)
enable_risk_check = st.sidebar.checkbox("Enable S&P compliance screening", value=True)
auto_refresh = st.sidebar.checkbox("Auto-refresh every 60s", value=False)

st.sidebar.header("💾 Cache Statistics")
st.sidebar.info(f"""
**Static Data:** {len(st.session_state.ship_static_cache)} vessels
**Compliance Data:** {len(st.session_state.risk_data_cache)} vessels
""")

if st.sidebar.button("🗑️ Clear All Cache"):
    st.session_state.ship_static_cache = {}
    st.session_state.risk_data_cache = {}
    save_cache({}, {})
    st.sidebar.success("Cache cleared!")
    st.rerun()

st.sidebar.header("🚨 Compliance Filters")
show_severe = st.sidebar.checkbox("Severe only (Legal Overall = 2)", value=False)
show_warning = st.sidebar.checkbox("Warning+ (Legal Overall ≥ 1)", value=False)
show_sanctioned = st.sidebar.checkbox("UN/OFAC sanctions only", value=False)

st.sidebar.header("🚢 Vessel Type Filter")
vessel_types = st.sidebar.multiselect(
    "Filter by vessel type:",
    options=['All', 'Cargo', 'Tanker', 'Passenger', 'Tug', 'Fishing', 'High Speed Craft', 
             'Pilot', 'SAR', 'Port Tender', 'Law Enforcement', 'Other', 'Unknown'],
    default=['All']
)

st.sidebar.header("⚓ Navigational Status")
nav_status = st.sidebar.multiselect(
    "Filter by status:",
    options=['All', 'Under way using engine', 'At anchor', 'Not under command', 
             'Restricted maneuverability', 'Constrained by draught', 'Moored',
             'Aground', 'Engaged in fishing', 'Under way sailing'],
    default=['All']
)

# Main content
status_placeholder = st.empty()
map_placeholder = st.empty()
stats_placeholder = st.empty()
table_placeholder = st.empty()

def collect_new_data():
    """Collect new AIS data and get S&P compliance"""
    sp_api = None
    if enable_risk_check and sp_username and sp_password:
        sp_api = SPMaritimeAPI(sp_username, sp_password)
    
    with status_placeholder:
        with st.spinner(f'🔄 Collecting AIS data for {duration} seconds...'):
            tracker = AISTracker()
            asyncio.run(tracker.collect_data(duration, ais_api_key))
            df = tracker.get_dataframe_with_risk(sp_api)
            st.session_state.current_df = df
            st.session_state.last_collection = time.time()
    
    return df

def display_data(df):
    """Display data with current filter settings"""
    if df.empty:
        st.warning("⚠️ No ships detected. Try increasing collection time.")
        return
    
    # Apply filters
    df_filtered = df.copy()
    
    if 'type_name' in df_filtered.columns and 'All' not in vessel_types:
        df_filtered = df_filtered[df_filtered['type_name'].isin(vessel_types)]
    
    if 'nav_status_name' in df_filtered.columns and 'All' not in nav_status:
        df_filtered = df_filtered[df_filtered['nav_status_name'].isin(nav_status)]
    
    if show_severe and 'legal_overall' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['legal_overall'] == 2]
    
    if show_warning and 'legal_overall' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['legal_overall'] >= 1]
    
    if show_sanctioned and 'ship_un_sanction' in df_filtered.columns:
        df_filtered = df_filtered[(df_filtered['ship_un_sanction'] == 2) | (df_filtered['ship_ofac_sanction'] == 2)]
    
    if df_filtered.empty:
        st.info("ℹ️ No ships match the selected filters.")
        return
    
    # Display stats
    with stats_placeholder:
        cols = st.columns(7)
        cols[0].metric("🚢 Total Ships", len(df_filtered))
        cols[1].metric("⚡ Moving", len(df_filtered[df_filtered['speed'] > 1]))
        
        if 'has_static' in df_filtered.columns:
            cols[2].metric("📡 Has Static", int(df_filtered['has_static'].sum()))
        
        ships_real_dims = len(df_filtered[(df_filtered['dimension_a'] > 0) | (df_filtered['dimension_b'] > 0)])
        ships_estimated = len(df_filtered) - ships_real_dims
        cols[3].metric("📐 Real Dims", ships_real_dims)
        cols[4].metric("📏 Estimated", ships_estimated)
        
        if 'legal_overall' in df_filtered.columns:
            severe = len(df_filtered[df_filtered['legal_overall'] == 2])
            warning = len(df_filtered[df_filtered['legal_overall'] == 1])
            cols[5].metric("🔴 Severe", severe)
            cols[6].metric("🟡 Warning", warning)
    
    # Create map
    with map_placeholder:
        if st.session_state.selected_vessel is not None:
            selected = df_filtered[df_filtered['mmsi'] == st.session_state.selected_vessel]
            if not selected.empty:
                center_lat = float(selected.iloc[0]['latitude'])
                center_lon = float(selected.iloc[0]['longitude'])
                zoom_level = 15
            else:
                center_lat = 1.27
                center_lon = 103.85
                zoom_level = 11
        else:
            center_lat = 1.27
            center_lon = 103.85
            zoom_level = 11
        
        view_state = pdk.ViewState(
            latitude=center_lat,
            longitude=center_lon,
            zoom=zoom_level,
            pitch=0,
        )
        
        df_filtered['is_estimated'] = (df_filtered['dimension_a'] == 0) & (df_filtered['dimension_b'] == 0)
        df_filtered['is_estimated'] = df_filtered['is_estimated'].apply(
            lambda x: '⚠️ Estimated size' if x else 'Real dimensions'
        )
        
        polygon_layer = pdk.Layer(
            'PolygonLayer',
            data=df_filtered,
            get_polygon='vessel_polygon',
            get_fill_color='color',
            get_line_color=[0, 0, 0, 0],
            line_width_min_pixels=0,
            pickable=True,
            auto_highlight=True,
            filled=True,
            extruded=False,
        )
        
        deck = pdk.Deck(
            map_style='',
            initial_view_state=view_state,
            layers=[polygon_layer],
            tooltip={
                'html': '<b>{name}</b><br/>Type: {type_name}<br/>Status: {nav_status_name}<br/>Speed: {speed} kts<br/>Legal: {legal_overall}',
                'style': {'backgroundColor': 'steelblue', 'color': 'white'}
            }
        )
        
        st.pydeck_chart(deck)
    
    # Show table
    with table_placeholder:
        st.subheader("📋 Vessel Details")
        
        def format_sp_value(val):
            if pd.isna(val):
                return '-'
            elif val == 2:
                return '🔴'
            elif val == 1:
                return '🟡'
            else:
                return '✅'
        
        header_cols = st.columns([2, 1.5, 2, 0.8, 1, 0.8, 0.8, 0.6])
        header_cols[0].markdown("**Name**")
        header_cols[1].markdown("**Type**")
        header_cols[2].markdown("**Status**")
        header_cols[3].markdown("**Speed**")
        header_cols[4].markdown("**Legal**")
        header_cols[5].markdown("**UN**")
        header_cols[6].markdown("**OFAC**")
        header_cols[7].markdown("**View**")
        
        st.markdown("---")
        
        container = st.container(height=500)
        
        with container:
            for idx, row in df_filtered.iterrows():
                cols = st.columns([2, 1.5, 2, 0.8, 1, 0.8, 0.8, 0.6])
                
                cols[0].text(row['name'][:25] + '...' if len(str(row['name'])) > 25 else row['name'])
                cols[1].text(row.get('type_name', 'Unknown'))
                cols[2].text(row.get('nav_status_name', 'Unknown')[:20])
                cols[3].text(f"{row['speed']:.1f}")
                cols[4].text(format_sp_value(row.get('legal_overall', 0)))
                cols[5].text(format_sp_value(row.get('ship_un_sanction', 0)))
                cols[6].text(format_sp_value(row.get('ship_ofac_sanction', 0)))
                
                if cols[7].button("🗺️", key=f"map_{row['mmsi']}", help=f"View {row['name']}"):
                    st.session_state.selected_vessel = row['mmsi']
                    st.rerun()
        
        st.markdown("---")
        
        if st.session_state.selected_vessel is not None:
            selected = df_filtered[df_filtered['mmsi'] == st.session_state.selected_vessel]
            if not selected.empty:
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.success(f"🎯 Map centered on: **{selected.iloc[0]['name']}**")
                with col2:
                    if st.button("↩️ Reset View", type="secondary"):
                        st.session_state.selected_vessel = None
                        st.rerun()
    
    save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache)
    
    if st.session_state.last_collection:
        st.success(f"✅ Last updated: {datetime.fromtimestamp(st.session_state.last_collection).strftime('%Y-%m-%d %H:%M:%S')}")

# Main execution
if st.session_state.current_df is None:
    df = collect_new_data()
    display_data(df)
else:
    display_data(st.session_state.current_df)

if st.sidebar.button("🔄 Refresh Now", type="primary"):
    df = collect_new_data()
    display_data(df)

if auto_refresh:
    time.sleep(60)
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("### 🎨 Legend")
st.sidebar.markdown("""
**Colors:**
- 🔴 Red: Severe (2)
- 🟠 Orange: Warning (1)
- 🟢 Green: Clear (0)
""")

st.sidebar.caption("Data: AISStream.io + S&P Global Maritime")
