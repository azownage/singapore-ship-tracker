"""
Singapore AIS Tracker with S&P Maritime Risk Intelligence
Real-time vessel tracking with compliance and risk indicators
Enhanced with persistent storage and caching
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

# Initialize session state for persistent storage
if 'ship_static_cache' not in st.session_state:
    ship_cache, risk_cache = load_cache()
    st.session_state.ship_static_cache = ship_cache
    st.session_state.risk_data_cache = risk_cache
    st.session_state.last_save = time.time()

# S&P Maritime API Integration
class SPMaritimeAPI:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_url = "https://maritimewebservices.ihs.com/MaritimeWCF/APSShipService.svc/RESTFul/GetShipsByIHSLRorIMONumbersAll"
    
    def get_ship_risk_data(self, imo_numbers: List[str]) -> Dict[str, Dict]:
        """Get risk indicators for multiple IMO numbers (with caching)"""
        if not imo_numbers:
            return {}
        
        # Use session state cache
        cache = st.session_state.risk_data_cache
        
        # Check which IMOs we already have
        uncached_imos = [imo for imo in imo_numbers if imo not in cache]
        
        if not uncached_imos:
            # All data in cache, no API call needed!
            return {imo: cache[imo] for imo in imo_numbers}
        
        st.info(f"🔍 Fetching risk data for {len(uncached_imos)} new vessels from S&P API...")
        
        try:
            # Batch API call (max 100 per request)
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
                                
                                # Cache this result permanently
                                cache[imo] = {
                                    'ship_name': detail.get('ShipName', ''),
                                    'ship_status': detail.get('ShipStatus', ''),
                                    'flag_disputed': detail.get('ShipFlagDisputed', '0') == '1',
                                    'un_sanction': detail.get('ShipUNSanctionList', '0') == '1',
                                    'owner_un_sanction': detail.get('ShipOwnerUNSanctionList', '0') == '1',
                                    'dark_activity': detail.get('ShipDarkActivityIndicator', '0') == '1',
                                    'ofac_sanction': detail.get('ShipOFACSanctionList', '0') == '1',
                                    'owner_ofac_sanction': detail.get('ShipOwnerOFACSanctionList', '0') == '1',
                                    'flag_name': detail.get('FlagName', ''),
                                    'ship_manager': detail.get('ShipManager', ''),
                                    'registered_owner': detail.get('RegisteredOwner', ''),
                                    'technical_manager': detail.get('TechnicalManager', ''),
                                    'risk_score': self._calculate_risk_score(detail),
                                    'cached_at': datetime.now().isoformat()
                                }
                
                time.sleep(0.5)  # Rate limiting
            
            # Save cache to disk
            st.session_state.risk_data_cache = cache
            save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache)
            
            st.success(f"✅ Cached risk data for {len(uncached_imos)} vessels")
                
        except Exception as e:
            st.error(f"⚠️ S&P API error: {str(e)}")
        
        # Return combined cache (old + new)
        return {imo: cache.get(imo, {}) for imo in imo_numbers}
    
    def _calculate_risk_score(self, detail: Dict) -> int:
        """Calculate risk score (0-100)"""
        score = 0
        
        if detail.get('ShipFlagDisputed') == '1':
            score += 25
        if detail.get('ShipUNSanctionList') == '1':
            score += 30
        if detail.get('ShipOwnerUNSanctionList') == '1':
            score += 20
        if detail.get('ShipDarkActivityIndicator') == '1':
            score += 15
        if detail.get('ShipOFACSanctionList') == '1':
            score += 30
        if detail.get('ShipOwnerOFACSanctionList') == '1':
            score += 20
        
        return min(score, 100)

# AIS Tracker with persistent storage
class AISTracker:
    def __init__(self):
        self.ships = defaultdict(lambda: {
            'latest_position': None,
            'static_data': None
        })
    
    def get_ship_color(self, type_code, risk_score=0):
        """Return color based on risk score"""
        if risk_score >= 50:
            return [255, 0, 0, 200]  # Red - High risk
        elif risk_score >= 25:
            return [255, 165, 0, 200]  # Orange - Medium risk
        elif type_code:
            if 60 <= type_code <= 69:
                return [0, 0, 255, 180]  # Blue - Passenger
            elif 70 <= type_code <= 79:
                return [255, 100, 100, 180]  # Light Red - Cargo
            elif 80 <= type_code <= 89:
                return [139, 0, 0, 180]  # Dark Red - Tanker
            elif type_code == 52:
                return [128, 0, 128, 180]  # Purple - Tug
        
        return [0, 255, 0, 180]  # Green - Low/No risk
    
    async def collect_data(self, duration=30, api_key="e38db7cbbfbd792829696a346f41a6630d74c53d"):
        async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
            subscription = {
                "APIKey": api_key,
                "BoundingBoxes": [[[1.22, 103.80], [1.32, 103.92]]],
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
        
        # Store in session state for persistence
        static_info = {
            'name': static_data.get('Name', 'Unknown'),
            'imo': imo,
            'type': static_data.get('Type'),
            'length': dimension.get('A', 0) + dimension.get('B', 0),
            'destination': static_data.get('Destination', 'Unknown'),
            'call_sign': static_data.get('CallSign', ''),
            'cached_at': datetime.now().isoformat()
        }
        
        self.ships[mmsi]['static_data'] = static_info
        
        # Store permanently in session state
        st.session_state.ship_static_cache[str(mmsi)] = static_info
        
        # Periodic save to disk
        if time.time() - st.session_state.last_save > 60:  # Save every minute
            save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache)
            st.session_state.last_save = time.time()
    
    def get_dataframe_with_risk(self, sp_api: SPMaritimeAPI) -> pd.DataFrame:
        """Get dataframe with risk indicators"""
        data = []
        
        # Collect all valid ships
        for mmsi, ship_data in self.ships.items():
            pos = ship_data.get('latest_position')
            
            # Try current static data, fall back to cached
            static = ship_data.get('static_data')
            if not static:
                static = st.session_state.ship_static_cache.get(str(mmsi), {})
            
            # Skip if no position data or invalid coordinates
            if not pos or pos.get('latitude') is None or pos.get('longitude') is None:
                continue
            
            # Safely get ship name
            name = static.get('name') or pos.get('ship_name') or 'Unknown'
            name = name.strip() if name else 'Unknown'
            
            ship_type = static.get('type')
            imo = str(static.get('imo', '0'))
            
            data.append({
                'mmsi': mmsi,
                'name': name,
                'imo': imo,
                'latitude': pos.get('latitude'),
                'longitude': pos.get('longitude'),
                'speed': pos.get('sog', 0),
                'course': pos.get('cog', 0),
                'type': ship_type,
                'length': static.get('length', 0),
                'destination': (static.get('destination') or 'Unknown').strip(),
                'call_sign': static.get('call_sign', ''),
                'risk_score': 0,
                'has_static': bool(static.get('name')),
                'color': self.get_ship_color(ship_type, 0)
            })
        
        df = pd.DataFrame(data)
        
        if len(df) == 0:
            return df
        
        # Get IMO numbers for risk checking
        valid_imos = [str(imo) for imo in df['imo'].unique() if imo and imo != '0']
        
        if valid_imos and sp_api:
            # Get risk data (uses cache, minimal API calls)
            risk_data = sp_api.get_ship_risk_data(valid_imos)
            
            # Merge risk data
            for idx, row in df.iterrows():
                imo = str(row['imo'])
                if imo in risk_data and risk_data[imo]:
                    risk_info = risk_data[imo]
                    df.at[idx, 'risk_score'] = risk_info.get('risk_score', 0)
                    df.at[idx, 'flag_disputed'] = risk_info.get('flag_disputed', False)
                    df.at[idx, 'un_sanction'] = risk_info.get('un_sanction', False)
                    df.at[idx, 'ofac_sanction'] = risk_info.get('ofac_sanction', False)
                    df.at[idx, 'dark_activity'] = risk_info.get('dark_activity', False)
                    df.at[idx, 'flag_name'] = risk_info.get('flag_name', '')
                    df.at[idx, 'registered_owner'] = risk_info.get('registered_owner', '')
                    
                    # Update color based on risk
                    df.at[idx, 'color'] = self.get_ship_color(row['type'], risk_info.get('risk_score', 0))
        
        return df


# Streamlit UI
st.title("🚢 Singapore Ship Risk Tracker")
st.markdown("Real-time vessel tracking with S&P Maritime compliance indicators")

# Sidebar - Get credentials from secrets
st.sidebar.header("⚙️ Configuration")

# Try to load from Streamlit secrets first, otherwise use input
try:
    sp_username = st.secrets["sp_maritime"]["username"]
    sp_password = st.secrets["sp_maritime"]["password"]
    ais_api_key = st.secrets.get("aisstream", {}).get("api_key", "e38db7cbbfbd792829696a346f41a6630d74c53d")
    st.sidebar.success("🔐 Using credentials from secrets")
except:
    # Fallback to manual input (hidden by default)
    with st.sidebar.expander("🔐 S&P Maritime API (Admin Only)", expanded=False):
        st.warning("⚠️ Credentials should be in Streamlit Secrets")
        sp_username = st.text_input("Username", type="password", help="Enter S&P username")
        sp_password = st.text_input("Password", type="password", help="Enter S&P password")
    ais_api_key = "e38db7cbbfbd792829696a346f41a6630d74c53d"

# Tracking settings
duration = st.sidebar.slider("AIS collection time (seconds)", 10, 60, 30)
enable_risk_check = st.sidebar.checkbox("Enable S&P risk checking", value=True)
auto_refresh = st.sidebar.checkbox("Auto-refresh every 60s", value=False)

# Cache stats
st.sidebar.header("💾 Cache Statistics")
st.sidebar.info(f"""
**Static Data Cache:** {len(st.session_state.ship_static_cache)} vessels
**Risk Data Cache:** {len(st.session_state.risk_data_cache)} vessels

Cached data is reused to save API costs! 💰
""")

if st.sidebar.button("🗑️ Clear All Cache"):
    st.session_state.ship_static_cache = {}
    st.session_state.risk_data_cache = {}
    save_cache({}, {})
    st.sidebar.success("Cache cleared!")
    st.rerun()

# Risk filter
st.sidebar.header("🚨 Risk Filters")
show_high_risk = st.sidebar.checkbox("High risk only (≥50)", value=False)
show_sanctioned = st.sidebar.checkbox("Sanctioned vessels only", value=False)
show_static_only = st.sidebar.checkbox("Ships with static data only", value=False)

# Main content
status_placeholder = st.empty()
map_placeholder = st.empty()
stats_placeholder = st.empty()
table_placeholder = st.empty()

def update_map():
    # Initialize API only if credentials provided
    sp_api = None
    if enable_risk_check and sp_username and sp_password:
        sp_api = SPMaritimeAPI(sp_username, sp_password)
    
    with status_placeholder:
        with st.spinner(f'🔄 Collecting AIS data for {duration} seconds...'):
            tracker = AISTracker()
            asyncio.run(tracker.collect_data(duration, ais_api_key))
            df = tracker.get_dataframe_with_risk(sp_api)
    
    if df.empty:
        st.warning("⚠️ No ships detected. Try increasing collection time.")
        return
    
    # Apply filters
    if show_high_risk and 'risk_score' in df.columns:
        df = df[df['risk_score'] >= 50]
    
    if show_sanctioned and 'un_sanction' in df.columns:
        df = df[(df['un_sanction'] == True) | (df['ofac_sanction'] == True)]
    
    if show_static_only:
        df = df[df['has_static'] == True]
    
    if df.empty:
        st.info("ℹ️ No ships match the selected filters.")
        return
    
    # Display stats
    with stats_placeholder:
        cols = st.columns(6)
        cols[0].metric("🚢 Total Ships", len(df))
        cols[1].metric("⚡ Moving", len(df[df['speed'] > 1]))
        cols[2].metric("📡 Has Static", df['has_static'].sum())
        
        if 'risk_score' in df.columns:
            high_risk = len(df[df['risk_score'] >= 50])
            cols[3].metric("🔴 High Risk", high_risk)
            
            if 'un_sanction' in df.columns:
                sanctioned = len(df[(df['un_sanction'] == True) | (df['ofac_sanction'] == True)])
                cols[4].metric("🚨 Sanctioned", sanctioned)
            
            cols[5].metric("📊 Avg Risk", f"{df['risk_score'].mean():.0f}")
        else:
            cols[3].metric("📊 Avg Speed", f"{df['speed'].mean():.1f} kts")
    
    # Create map
    with map_placeholder:
        st.pydeck_chart(pdk.Deck(
            map_style='mapbox://styles/mapbox/dark-v10',
            initial_view_state=pdk.ViewState(
                latitude=1.27,
                longitude=103.85,
                zoom=11,
                pitch=45,
            ),
            layers=[
                pdk.Layer(
                    'ScatterplotLayer',
                    data=df,
                    get_position='[longitude, latitude]',
                    get_color='color',
                    get_radius=150,
                    pickable=True,
                    auto_highlight=True,
                ),
            ],
            tooltip={
                'html': '''
                <b>{name}</b><br/>
                IMO: {imo}<br/>
                Speed: {speed} kts<br/>
                Risk Score: {risk_score}<br/>
                Destination: {destination}
                ''',
                'style': {
                    'backgroundColor': 'steelblue',
                    'color': 'white'
                }
            }
        ))
    
    # Show detailed table
    with table_placeholder:
        st.subheader("📋 Vessel Details")
        
        display_cols = ['name', 'imo', 'speed', 'destination', 'has_static']
        
        if 'risk_score' in df.columns:
            display_cols.extend(['risk_score', 'flag_disputed', 'un_sanction', 'ofac_sanction', 'dark_activity'])
            
            # Format risk indicators
            df_display = df[display_cols].copy()
            df_display['has_static'] = df_display['has_static'].map({True: '✅', False: '❌'})
            df_display['flag_disputed'] = df_display['flag_disputed'].map({True: '⚠️', False: '✅', None: '-'})
            df_display['un_sanction'] = df_display['un_sanction'].map({True: '🚨', False: '✅', None: '-'})
            df_display['ofac_sanction'] = df_display['ofac_sanction'].map({True: '🚨', False: '✅', None: '-'})
            df_display['dark_activity'] = df_display['dark_activity'].map({True: '🌑', False: '✅', None: '-'})
            
            # Color code risk scores
            def highlight_risk(val):
                if pd.isna(val) or val == 0:
                    return ''
                elif val >= 50:
                    return 'background-color: #ff4444; color: white'
                elif val >= 25:
                    return 'background-color: #ffaa00; color: white'
                else:
                    return 'background-color: #44ff44'
            
            styled_df = df_display.style.applymap(highlight_risk, subset=['risk_score'])
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
        else:
            df_display = df[display_cols].copy()
            df_display['has_static'] = df_display['has_static'].map({True: '✅', False: '❌'})
            st.dataframe(df_display.sort_values('speed', ascending=False), 
                        use_container_width=True, hide_index=True)
    
    # Save cache after update
    save_cache(st.session_state.ship_static_cache, st.session_state.risk_data_cache)
    
    st.success(f"✅ Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Initial load
if 'initialized' not in st.session_state:
    st.session_state.initialized = True
    update_map()

# Manual refresh
if st.sidebar.button("🔄 Refresh Now", type="primary"):
    update_map()

# Auto-refresh
if auto_refresh:
    time.sleep(60)
    st.rerun()

# Legend
st.sidebar.markdown("---")
st.sidebar.markdown("### 🎨 Color Legend")
st.sidebar.markdown("""
- 🔴 **Red**: High risk (≥50)
- 🟠 **Orange**: Medium risk (25-49)
- 🔵 **Blue**: Passenger (low risk)
- 🟢 **Green**: Low/No risk
- 🟣 **Purple**: Tug/Service
""")

st.sidebar.markdown("### 🚨 Risk Indicators")
st.sidebar.markdown("""
- ⚠️ Flag disputed
- 🚨 UN/OFAC sanctions
- 🌑 Dark activity detected
- ✅ No issues
- ❌ No static data yet
""")

st.sidebar.markdown("---")
st.sidebar.caption("Data: AISStream.io + S&P Global Maritime")
