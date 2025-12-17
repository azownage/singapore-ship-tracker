"""
Singapore AIS Tracker with S&P Maritime Risk Intelligence
Complete S&P Compliance Screening v3.71 Implementation
Based on official S&P Global documentation
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

# S&P Maritime API Integration with COMPLETE Compliance Screening
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
                                
                                # Extract ALL S&P compliance fields per documentation
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
        """Extract complete S&P compliance screening per v3.71 specification"""
        
        # Convert string values to proper format (2/1/0)
        def parse_value(val):
            if val == '2' or val == 2:
                return 2
            elif val == '1' or val == 1:
                return 1
            else:
                return 0
        
        # Ship Sanctions (Severe = 2)
        ship_bes = parse_value(detail.get('ShipBESSanctionList', '0'))
        ship_eu = parse_value(detail.get('ShipEUSanctionList', '0'))
        ship_ofac = parse_value(detail.get('ShipOFACSanctionList', '0'))
        ship_ofac_non_sdn = parse_value(detail.get('ShipOFACNonSDNSanctionList', '0'))
        ship_swiss = parse_value(detail.get('ShipSwissSanctionList', '0'))
        ship_un = parse_value(detail.get('ShipUNSanctionList', '0'))
        ship_ofac_advisory = parse_value(detail.get('ShipUSTreasuryOFACAdvisoryList', '0'))
        
        # Port Call History (Severe = 2, Warning = 1)
        port_call_3m = parse_value(detail.get('ShipSanctionedCountryPortCallLast3m', '0'))
        port_call_6m = parse_value(detail.get('ShipSanctionedCountryPortCallLast6m', '0'))
        port_call_12m = parse_value(detail.get('ShipSanctionedCountryPortCallLast12m', '0'))
        
        # Dark Activity (Severe = 2, Warning = 1)
        dark_activity = parse_value(detail.get('ShipDarkActivityIndicator', '0'))
        
        # Ship Flag Issues (Severe = 2, Warning = 1)
        flag_disputed = parse_value(detail.get('ShipFlagDisputed', '0'))
        flag_sanctioned = parse_value(detail.get('ShipFlagSanctionedCountry', '0'))
        flag_historical = parse_value(detail.get('ShipHistoricalFlagSanctionedCountry', '0'))
        
        # Owner/Operator Sanctions (Severe = 2)
        owner_australian = parse_value(detail.get('ShipOwnerAustralianSanctionList', '0'))
        owner_bes = parse_value(detail.get('ShipOwnerBESSanctionList', '0'))
        owner_canadian = parse_value(detail.get('ShipOwnerCanadianSanctionList', '0'))
        owner_eu = parse_value(detail.get('ShipOwnerEUSanctionList', '0'))
        owner_fatf = parse_value(detail.get('ShipOwnerFATFJurisdiction', '0'))
        owner_ofac_ssi = parse_value(detail.get('ShipOFACSSIList', '0'))
        owner_ofac = parse_value(detail.get('ShipOwnerOFACSanctionList', '0'))
        owner_swiss = parse_value(detail.get('ShipOwnerSwissSanctionList', '0'))
        owner_uae = parse_value(detail.get('ShipOwnerUAESanctionList', '0'))
        owner_un = parse_value(detail.get('ShipOwnerUNSanctionList', '0'))
        owner_ofac_country = parse_value(detail.get('ShipOwnerOFACSanctionedCountry', '0'))
        
        # Calculate Legal Overall Score (highest value from all checks)
        all_scores = [
            ship_bes, ship_eu, ship_ofac, ship_ofac_non_sdn, ship_swiss, ship_un, ship_ofac_advisory,
            port_call_3m, port_call_6m, port_call_12m,
            dark_activity,
            flag_disputed, flag_sanctioned,
            owner_australian, owner_bes, owner_canadian, owner_eu, owner_fatf,
            owner_ofac_ssi, owner_ofac, owner_swiss, owner_uae, owner_un, owner_ofac_country
        ]
        legal_overall = max(all_scores)
        
        return {
            # Basic Info
            'ship_name': detail.get('ShipName', ''),
            'ship_status': detail.get('ShipStatus', ''),
            'flag_name': detail.get('FlagName', ''),
            'ship_manager': detail.get('ShipManager', ''),
            'registered_owner': detail.get('RegisteredOwner', ''),
            'technical_manager': detail.get('TechnicalManager', ''),
            
            # Overall Score (per S&P spec)
            'legal_overall': legal_overall,
            
            # Ship Sanctions
            'ship_bes_sanction': ship_bes,
            'ship_eu_sanction': ship_eu,
            'ship_ofac_sanction': ship_ofac,
            'ship_ofac_non_sdn': ship_ofac_non_sdn,
            'ship_swiss_sanction': ship_swiss,
            'ship_un_sanction': ship_un,
            'ship_ofac_advisory': ship_ofac_advisory,
            
            # Port Calls
            'port_call_3m': port_call_3m,
            'port_call_6m': port_call_6m,
            'port_call_12m': port_call_12m,
            
            # Dark Activity
            'dark_activity': dark_activity,
            
            # Flag Issues
            'flag_disputed': flag_disputed,
            'flag_sanctioned': flag_sanctioned,
            'flag_historical': flag_historical,
            
            # Owner/Operator Sanctions
            'owner_australian': owner_australian,
            'owner_bes': owner_bes,
            'owner_canadian': owner_canadian,
            'owner_eu': owner_eu,
            'owner_fatf': owner_fatf,
            'owner_ofac_ssi': owner_ofac_ssi,
            'owner_ofac': owner_ofac,
            'owner_swiss': owner_swiss,
            'owner_uae': owner_uae,
            'owner_un': owner_un,
            'owner_ofac_country': owner_ofac_country,
            
            # Risk Score (0-100 for display)
            'risk_score': self._calculate_risk_score(legal_overall, all_scores),
            
            'cached_at': datetime.now().isoformat()
        }
    
    def _calculate_risk_score(self, legal_overall: int, all_scores: List[int]) -> int:
        """
        Calculate 0-100 risk score based on S&P compliance
        
        Per S&P Spec:
        - Legal Overall 2 = Severe (80-100 points)
        - Legal Overall 1 = Warning (40-79 points)
        - Legal Overall 0 = Clear (0-39 points)
        """
        if legal_overall == 2:
            # Severe - base 80, add 4 points per additional severe issue
            severe_count = sum(1 for s in all_scores if s == 2)
            return min(80 + (severe_count * 4), 100)
        elif legal_overall == 1:
            # Warning - base 40, add 5 points per warning
            warning_count = sum(1 for s in all_scores if s == 1)
            return min(40 + (warning_count * 5), 79)
        else:
            # Clear
            return 0

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
            return [255, 0, 0, 220]  # RED - Severe
        elif legal_overall == 1:
            return [255, 165, 0, 220]  # ORANGE - Warning
        elif type_code:
            if 60 <= type_code <= 69:
                return [0, 0, 255, 180]  # Blue - Passenger
            elif 70 <= type_code <= 79:
                return [100, 200, 100, 180]  # Light Green - Cargo
            elif 80 <= type_code <= 89:
                return [139, 0, 139, 180]  # Purple - Tanker
            elif type_code == 52:
                return [128, 128, 0, 180]  # Olive - Tug
        
        return [0, 255, 0, 180]  # GREEN - Clear/OK
    
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
                'legal_overall': 0,
                'risk_score': 0,
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
                    df.at[idx, 'risk_score'] = risk_info.get('risk_score', 0)
                    df.at[idx, 'flag_name'] = risk_info.get('flag_name', '')
                    df.at[idx, 'registered_owner'] = risk_info.get('registered_owner', '')
                    
                    # Key compliance flags
                    df.at[idx, 'ship_un_sanction'] = risk_info.get('ship_un_sanction', 0)
                    df.at[idx, 'ship_ofac_sanction'] = risk_info.get('ship_ofac_sanction', 0)
                    df.at[idx, 'dark_activity'] = risk_info.get('dark_activity', 0)
                    df.at[idx, 'flag_disputed'] = risk_info.get('flag_disputed', 0)
                    df.at[idx, 'port_call_3m'] = risk_info.get('port_call_3m', 0)
                    df.at[idx, 'owner_un'] = risk_info.get('owner_un', 0)
                    df.at[idx, 'owner_ofac'] = risk_info.get('owner_ofac', 0)
                    
                    # Update color based on legal overall
                    df.at[idx, 'color'] = self.get_ship_color(row['type'], legal_overall)
        
        return df


# Streamlit UI
st.title("🚢 Singapore Ship Risk Tracker")
st.markdown("Real-time vessel tracking with **S&P Global Compliance Screening v3.71**")

# Sidebar
st.sidebar.header("⚙️ Configuration")

# Load credentials
try:
    sp_username = st.secrets["sp_maritime"]["username"]
    sp_password = st.secrets["sp_maritime"]["password"]
    ais_api_key = st.secrets.get("aisstream", {}).get("api_key", "e38db7cbbfbd792829696a346f41a6630d74c53d")
    st.sidebar.success("🔐 Using credentials from secrets")
except:
    with st.sidebar.expander("🔐 S&P Maritime API (Admin Only)", expanded=False):
        st.warning("⚠️ Credentials should be in Streamlit Secrets")
        sp_username = st.text_input("Username", type="password")
        sp_password = st.text_input("Password", type="password")
    ais_api_key = "e38db7cbbfbd792829696a346f41a6630d74c53d"

duration = st.sidebar.slider("AIS collection time (seconds)", 10, 60, 30)
enable_risk_check = st.sidebar.checkbox("Enable S&P compliance screening", value=True)
auto_refresh = st.sidebar.checkbox("Auto-refresh every 60s", value=False)

# Cache stats
st.sidebar.header("💾 Cache Statistics")
st.sidebar.info(f"""
**Static Data:** {len(st.session_state.ship_static_cache)} vessels
**Compliance Data:** {len(st.session_state.risk_data_cache)} vessels

Cached data saves API costs! 💰
""")

if st.sidebar.button("🗑️ Clear All Cache"):
    st.session_state.ship_static_cache = {}
    st.session_state.risk_data_cache = {}
    save_cache({}, {})
    st.sidebar.success("Cache cleared!")
    st.rerun()

# Filters based on S&P Legal Overall
st.sidebar.header("🚨 Compliance Filters")
show_severe = st.sidebar.checkbox("Severe only (Legal Overall = 2)", value=False)
show_warning = st.sidebar.checkbox("Warning+ (Legal Overall ≥ 1)", value=False)
show_sanctioned = st.sidebar.checkbox("UN/OFAC sanctions only", value=False)

# Main content
status_placeholder = st.empty()
map_placeholder = st.empty()
stats_placeholder = st.empty()
table_placeholder = st.empty()

def update_map():
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
    if show_severe and 'legal_overall' in df.columns:
        df = df[df['legal_overall'] == 2]
    
    if show_warning and 'legal_overall' in df.columns:
        df = df[df['legal_overall'] >= 1]
    
    if show_sanctioned and 'ship_un_sanction' in df.columns:
        df = df[(df['ship_un_sanction'] == 2) | (df['ship_ofac_sanction'] == 2)]
    
    if df.empty:
        st.info("ℹ️ No ships match the selected filters.")
        return
    
    # Display stats
    with stats_placeholder:
        cols = st.columns(6)
        cols[0].metric("🚢 Total Ships", len(df))
        cols[1].metric("⚡ Moving", len(df[df['speed'] > 1]))
        
        if 'has_static' in df.columns:
            cols[2].metric("📡 Has Static", int(df['has_static'].sum()))
        
        if 'legal_overall' in df.columns:
            severe = len(df[df['legal_overall'] == 2])
            warning = len(df[df['legal_overall'] == 1])
            cols[3].metric("🔴 Severe", severe)
            cols[4].metric("🟠 Warning", warning)
            cols[5].metric("📊 Avg Risk", f"{df['risk_score'].mean():.0f}")
    
    # Create map
    with map_placeholder:
        view_state = pdk.ViewState(
            latitude=1.27,
            longitude=103.85,
            zoom=11,
            pitch=0,
        )
        
        scatter_layer = pdk.Layer(
            'ScatterplotLayer',
            data=df,
            get_position='[longitude, latitude]',
            get_color='color',
            get_radius=200,
            pickable=True,
            auto_highlight=True,
        )
        
        deck = pdk.Deck(
            map_style='',
            initial_view_state=view_state,
            layers=[scatter_layer],
            tooltip={
                'html': '<b>{name}</b><br/>IMO: {imo}<br/>Speed: {speed} kts<br/>Risk: {risk_score}<br/>Legal Overall: {legal_overall}',
                'style': {'backgroundColor': 'steelblue', 'color': 'white'}
            }
        )
        
        st.pydeck_chart(deck)
    
    # Show table
    with table_placeholder:
        st.subheader("📋 S&P Compliance Screening Results")
        
        available_cols = list(df.columns)
        display_cols = []
        
        for col in ['name', 'imo', 'speed', 'destination', 'legal_overall', 'risk_score',
                    'ship_un_sanction', 'ship_ofac_sanction', 'dark_activity', 'flag_disputed',
                    'port_call_3m', 'owner_un', 'owner_ofac']:
            if col in available_cols:
                display_cols.append(col)
        
        df_display = df[display_cols].copy()
        
        # Format S&P values (2 = Severe, 1 = Warning, 0 = OK)
        def format_sp_value(val):
            if pd.isna(val):
                return '-'
            elif val == 2:
                return '🔴'  # Severe
            elif val == 1:
                return '🟡'  # Warning
            else:
                return '✅'  # OK
        
        for col in ['legal_overall', 'ship_un_sanction', 'ship_ofac_sanction', 'dark_activity',
                    'flag_disputed', 'port_call_3m', 'owner_un', 'owner_ofac']:
            if col in df_display.columns:
                df_display[col] = df_display[col].apply(format_sp_value)
        
        # Color code risk scores
        if 'risk_score' in df_display.columns:
            def highlight_risk(val):
                if pd.isna(val) or val == 0:
                    return ''
                elif val >= 80:
                    return 'background-color: #ff0000; color: white; font-weight: bold'
                elif val >= 40:
                    return 'background-color: #ffaa00; color: white'
                else:
                    return 'background-color: #90EE90'
            
            styled_df = df_display.style.applymap(highlight_risk, subset=['risk_score'])
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
        else:
            st.dataframe(df_display, use_container_width=True, hide_index=True)
    
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
st.sidebar.markdown("### 🎨 S&P Compliance Legend")
st.sidebar.markdown("""
**Ship Colors (Legal Overall):**
- 🔴 **Red**: Severe (2)
- 🟠 **Orange**: Warning (1)
- 🟢 **Green**: Clear (0)

**Risk Score:**
- 80-100: Severe compliance issue
- 40-79: Warning detected
- 0-39: Clear/OK

**Indicators:**
- 🔴 = Severe (2)
- 🟡 = Warning (1)
- ✅ = OK (0)
""")

st.sidebar.markdown("### 📊 S&P Screening v3.71")
st.sidebar.caption("""
Checks:
• UN, OFAC, EU, UK sanctions
• Port calls (sanctioned countries)
• Dark activity detection
• Flag disputes
• Owner/operator compliance
""")

st.sidebar.markdown("---")
st.sidebar.caption("Data: AISStream.io + S&P Global Maritime")
