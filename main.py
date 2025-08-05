import streamlit as st
import pandas as pd
import pydeck as pdk
import json
import requests
import time
import os
from typing import List, Dict, Tuple

# Set page config
st.set_page_config(page_title="Tree Construction Protection Zones", layout="wide")

st.title("Tree Construction Protection Zone Visualizer")
st.markdown("""
This app visualizes construction protection zones around trees based on the policy:
- Minimum protection zone: 5 feet
- Additional zone: 1 foot for every inch of diameter over 5 inches
""")

# Function to calculate protection zone radius
def calculate_protection_radius(diameter):
    """
    Calculate protection zone radius in feet based on tree diameter.
    Formula: 5 feet minimum + 1 foot for every inch over 5 inches
    """
    if diameter <= 5:
        return 5.0
    else:
        return 5.0 + (diameter - 5.0)

# Function to convert feet to meters (pydeck uses meters by default)
def feet_to_meters(feet):
    return feet * 0.3048

# Function to process GeoJSON data
def process_geojson_data(geojson_data):
    """Extract features from GeoJSON and create DataFrame."""
    features = []
    for feature in geojson_data['features']:
        props = feature['properties']
        coords = feature['geometry']['coordinates']
        
        # Extract relevant fields with fallbacks
        features.append({
            'longitude': coords[0],
            'latitude': coords[1],
            'diameter': props.get('DIAMETER', props.get('diameter', 0)),
            'species_common': props.get('SPP_COM', props.get('species_common', 'Unknown')),
            'species_botanical': props.get('SPP_BOT', props.get('species_botanical', 'Unknown')),
            'status': props.get('STATUS', props.get('status', 'Unknown')),
            'site_id': props.get('site_id', props.get('OBJECTID', 'Unknown')),
            'object_id': props.get('OBJECTID', props.get('object_id', None))
        })
    
    return pd.DataFrame(features)

# Function to load local GeoJSON file
def load_local_geojson(filename="Urban_Forestry_Street_Trees.geojson"):
    """Attempt to load a local GeoJSON file."""
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                return json.load(f)
        return None
    except Exception as e:
        st.error(f"Error reading local file {filename}: {str(e)}")
        return None

# Try to load local file first
geojson_data = None
local_file_loaded = False

# Check for local file on app start
if 'checked_local_file' not in st.session_state:
    st.session_state['checked_local_file'] = True
    local_data = load_local_geojson()
    if local_data:
        st.session_state['geojson_data'] = local_data
        local_file_loaded = True
        st.success("✅ Automatically loaded local file: Urban_Forestry_Street_Trees.geojson")

# Use cached local data if available
if 'geojson_data' in st.session_state and not local_file_loaded:
    geojson_data = st.session_state['geojson_data']
    local_file_loaded = True

# Default URLs for Madison tree data
MADISON_TREES_BASE_URL = "https://maps.cityofmadison.com/arcgis/rest/services/Public/OPEN_DATA/MapServer/0/query"

# Default batch sizes
OBJECT_ID_BATCH_SIZE = 1000
FEATURE_BATCH_SIZE = 100

class ArcGISPaginatedClient:
    """Client for handling paginated requests to ArcGIS REST services."""
    
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url
        self.timeout = timeout
        
    def get_object_ids(self, where_clause: str = "1=1", batch_size: int = 1000) -> List[int]:
        """Get all object IDs using pagination."""
        all_object_ids = []
        offset = 0
        batch_count = 0
        
        progress_placeholder = st.empty()
        
        while True:
            progress_placeholder.text(f"Fetching object IDs: {len(all_object_ids)} retrieved...")
            
            params = {
                'where': where_clause,
                'returnIdsOnly': 'true',
                'f': 'json',
                'resultOffset': offset,
                'resultRecordCount': batch_size
            }
            
            try:
                response = requests.get(self.base_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                
                if 'objectIds' in data and data['objectIds']:
                    batch_ids = data['objectIds']
                    all_object_ids.extend(batch_ids)
                    
                    # If we got fewer records than requested, we've reached the end
                    if len(batch_ids) < batch_size:
                        break
                        
                    offset += len(batch_ids)
                    batch_count += 1

                    # Sleep for 5 seconds every 10 batches
                    if batch_count % 10 == 0:
                        time.sleep(5)
                    else:
                        time.sleep(1)  # Regular delay between batches
                else:
                    break
                    
            except requests.exceptions.RequestException as e:
                st.error(f"Error fetching object IDs at offset {offset}: {str(e)}")
                break
            except Exception as e:
                st.error(f"Error processing object IDs response: {str(e)}")
                break
    
        progress_placeholder.text(f"✅ Retrieved {len(all_object_ids)} object IDs total")
        return all_object_ids
    
    def get_features_by_ids(self, object_ids: List[int], out_fields: str = "*") -> Dict:
        """Get features by object IDs in batches."""
        all_features = []
        
        # Process object IDs in chunks
        id_chunks = [object_ids[i:i + FEATURE_BATCH_SIZE] 
                    for i in range(0, len(object_ids), FEATURE_BATCH_SIZE)]
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, chunk in enumerate(id_chunks):
            status_text.text(f"Processing batch {i+1}/{len(id_chunks)} ({len(chunk)} features)...")
            
            # Convert IDs to comma-separated string
            object_ids_str = ','.join(map(str, chunk))
            
            params = {
                'objectIds': object_ids_str,
                'outFields': out_fields,
                'returnGeometry': 'true',
                'f': 'geojson'
            }
            
            try:
                response = requests.get(self.base_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                chunk_data = response.json()
                
                if 'features' in chunk_data:
                    all_features.extend(chunk_data['features'])
                
                # Update progress
                progress_bar.progress((i + 1) / len(id_chunks))
                
                # Sleep for 5 seconds every 10 batches
                if (i + 1) % 10 == 0:
                    time.sleep(5)
                else:
                    time.sleep(1)  # Regular delay between batches
                
            except requests.exceptions.RequestException as e:
                st.error(f"Error fetching features for batch {i+1}: {str(e)}")
                continue
            except Exception as e:
                st.error(f"Error processing features response for batch {i+1}: {str(e)}")
                continue
        
        status_text.text(f"✅ Successfully processed {len(all_features)} features")
        progress_bar.empty()
        
        # Return in GeoJSON format
        return {
            'type': 'FeatureCollection',
            'features': all_features
        }

# Function to fetch GeoJSON from URL (legacy method for simple requests)
@st.cache_data
def get_geojson_from_url(url):
    """Fetch GeoJSON data from URL with caching."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data from URL: {str(e)}")
        return None

# Sidebar for configuration
st.sidebar.header("Configuration")

# Show data source info and alternative options
if local_file_loaded:
    st.sidebar.success("🏠 Using local data file")
    geojson_data = st.session_state['geojson_data']
    
    # Option to reload or use alternative sources
    st.sidebar.subheader("Alternative Data Sources")
    if st.sidebar.button("🔄 Reload Local File"):
        local_data = load_local_geojson()
        if local_data:
            st.session_state['geojson_data'] = local_data
            st.sidebar.success("✅ Local file reloaded!")
            st.rerun()
        else:
            st.sidebar.error("❌ Could not reload local file")
    
    # Show alternative options in an expander
    with st.sidebar.expander("🌐 Use Alternative Data Source"):
        alt_data_source = st.radio(
            "Choose alternative data source:",
            ["City of Madison Trees", "Upload Custom GeoJSON"],
            index=0
        )
        
        if alt_data_source == "City of Madison Trees":
            if st.button("🔄 Load Madison Tree Data", type="primary"):
                with st.spinner("Loading tree data..."):
                    try:
                        # Initialize the paginated client
                        client = ArcGISPaginatedClient(MADISON_TREES_BASE_URL)
                        
                        st.info("Step 1: Fetching all object IDs...")
                        # Get all object IDs first
                        object_ids = client.get_object_ids(
                            batch_size=OBJECT_ID_BATCH_SIZE
                        )
                        
                        if not object_ids:
                            st.warning("No object IDs found with the given criteria.")
                        else:
                            st.info(f"Step 2: Fetching {len(object_ids)} features in batches of {FEATURE_BATCH_SIZE}...")
                            # Get features using the object IDs
                            madison_data = client.get_features_by_ids(object_ids)
                            
                            if madison_data and 'features' in madison_data:
                                st.success(f"✅ Successfully loaded {len(madison_data['features'])} features!")
                                # Cache the result in session state
                                st.session_state['geojson_data'] = madison_data
                                st.rerun()
                            else:
                                st.error("Failed to fetch feature data.")
                                
                    except Exception as e:
                        st.error(f"Error during paginated fetch: {str(e)}")
        
        elif alt_data_source == "Upload Custom GeoJSON":
            uploaded_file = st.file_uploader("Choose a GeoJSON file", type=['geojson', 'json'])
            if uploaded_file is not None:
                try:
                    upload_data = json.load(uploaded_file)
                    st.session_state['geojson_data'] = upload_data
                    st.success("✅ File uploaded successfully")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error loading file: {str(e)}")

else:
    # No local file found, show all options
    st.sidebar.info("📁 No local 'Urban_Forestry_Street_Trees.geojson' file found")
    st.sidebar.subheader("Data Source")
    data_source = st.sidebar.radio(
        "Choose data source:",
        ["City of Madison Trees", "Upload Custom GeoJSON"],
        index=0
    )
    
    # Handle Madison data loading
    if data_source == "City of Madison Trees":
        if st.sidebar.button("🔄 Load Madison Tree Data", type="primary"):
            with st.spinner("Loading tree data..."):
                try:
                    # Initialize the paginated client
                    client = ArcGISPaginatedClient(MADISON_TREES_BASE_URL)
                    
                    st.info("Step 1: Fetching all object IDs...")
                    # Get all object IDs first
                    object_ids = client.get_object_ids(
                        batch_size=OBJECT_ID_BATCH_SIZE
                    )
                    
                    if not object_ids:
                        st.warning("No object IDs found with the given criteria.")
                    else:
                        st.info(f"Step 2: Fetching {len(object_ids)} features in batches of {FEATURE_BATCH_SIZE}...")
                        # Get features using the object IDs
                        geojson_data = client.get_features_by_ids(object_ids)
                        
                        if geojson_data and 'features' in geojson_data:
                            st.success(f"✅ Successfully loaded {len(geojson_data['features'])} features!")
                            # Cache the result in session state
                            st.session_state['geojson_data'] = geojson_data
                        else:
                            st.error("Failed to fetch feature data.")
                            
                except Exception as e:
                    st.error(f"Error during paginated fetch: {str(e)}")
        
        # Use cached data if available
        if 'geojson_data' in st.session_state:
            geojson_data = st.session_state['geojson_data']
            st.sidebar.success(f"✅ Using cached data ({len(geojson_data['features'])} features)")
    
    elif data_source == "Upload Custom GeoJSON":
        uploaded_file = st.sidebar.file_uploader("Choose a GeoJSON file", type=['geojson', 'json'])
        if uploaded_file is not None:
            try:
                geojson_data = json.load(uploaded_file)
                st.sidebar.success("✅ File uploaded successfully")
            except Exception as e:
                st.error(f"Error loading file: {str(e)}")

# Diameter filter slider
min_diameter = st.sidebar.slider(
    "Minimum Tree Diameter (inches)",
    min_value=0.0,
    max_value=50.0,
    value=0.0,
    step=0.5,
    help="Filter trees by minimum diameter"
)

# Protection zone visualization options
st.sidebar.subheader("Visualization Options")
show_trees = st.sidebar.checkbox("Show tree points", value=False)
show_zones = st.sidebar.checkbox("Show protection zones", value=True)
zone_opacity = st.sidebar.slider("Zone opacity", 0.0, 1.0, 0.3, 0.05)

# Process and visualize data if available
if geojson_data is not None:
    try:
        # Create DataFrame from GeoJSON
        df = process_geojson_data(geojson_data)
        
        if df.empty:
            st.warning("No data found in the loaded dataset.")
        else:
            # Filter by minimum diameter
            filtered_df = df[df['diameter'] >= min_diameter].copy()
            
            # Calculate protection zone radius for each tree
            filtered_df['protection_radius_feet'] = filtered_df['diameter'].apply(calculate_protection_radius)
            filtered_df['protection_radius_meters'] = filtered_df['protection_radius_feet'].apply(feet_to_meters)
            
            # Display statistics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Trees", len(df))
            with col2:
                st.metric("Filtered Trees", len(filtered_df))
            with col3:
                avg_diameter = filtered_df['diameter'].mean() if len(filtered_df) > 0 else 0
                st.metric("Avg Diameter", f"{avg_diameter:.1f} inches")
            with col4:
                avg_protection = filtered_df['protection_radius_feet'].mean() if len(filtered_df) > 0 else 0
                st.metric("Avg Protection Zone", f"{avg_protection:.1f} feet")
            
            if len(filtered_df) == 0:
                st.warning("No trees match the current diameter filter. Try reducing the minimum diameter.")
            else:
                # Create map layers
                layers = []
                
                # Protection zones layer (circles)
                if show_zones:
                    zone_layer = pdk.Layer(
                        'ScatterplotLayer',
                        data=filtered_df,
                        get_position=['longitude', 'latitude'],
                        get_radius='protection_radius_meters',
                        get_fill_color=[255, 140, 0, int(zone_opacity * 255)],
                        get_line_color=[255, 100, 0, 200],
                        stroked=True,
                        filled=True,
                        line_width_min_pixels=2,
                        pickable=True,
                        auto_highlight=True
                    )
                    layers.append(zone_layer)
                
                # Tree points layer
                if show_trees:
                    tree_layer = pdk.Layer(
                        'ScatterplotLayer',
                        data=filtered_df,
                        get_position=['longitude', 'latitude'],
                        get_radius=2,  # Fixed small radius for tree points
                        get_fill_color=[34, 139, 34, 255],  # Forest green
                        get_line_color=[0, 0, 0, 255],
                        stroked=True,
                        filled=True,
                        radius_min_pixels=3,
                        radius_max_pixels=10,
                        line_width_min_pixels=1,
                        pickable=True
                    )
                    layers.append(tree_layer)
                
                # Set initial view state (centered on data)
                view_state = pdk.ViewState(
                    latitude=filtered_df['latitude'].mean(),
                    longitude=filtered_df['longitude'].mean(),
                    zoom=12,
                    pitch=0
                )
                
                # Create tooltip
                tooltip = {
                    "html": """
                    <b>Species:</b> {species_common}<br/>
                    <b>Botanical:</b> {species_botanical}<br/>
                    <b>Diameter:</b> {diameter} inches<br/>
                    <b>Protection Zone:</b> {protection_radius_feet} feet<br/>
                    <b>Status:</b> {status}<br/>
                    <b>Site ID:</b> {site_id}
                    """,
                    "style": {
                        "backgroundColor": "steelblue",
                        "color": "white",
                        "padding": "5px",
                        "borderRadius": "5px"
                    }
                }
                
                # Create deck
                deck = pdk.Deck(
                    map_style='light',
                    initial_view_state=view_state,
                    layers=layers,
                    tooltip=tooltip,
                )
                
                # Display the map
                st.pydeck_chart(deck,height=800)
                

                
                
            
    except Exception as e:
        st.error(f"Error processing data: {str(e)}")
        st.info("Please make sure the data has the expected structure.")
        
        # Show expected structure
        with st.expander("Expected GeoJSON Structure"):
            st.code("""
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "OBJECTID": 1,
        "DIAMETER": 6.0,
        "SPP_COM": "Honeylocust 'Skyline'",
        "SPP_BOT": "Gleditsia triacanthos 'Skyline'",
        "STATUS": "Active",
        "site_id": 403144
      },
      "geometry": {
        "type": "Point",
        "coordinates": [-89.385608, 43.056392]
      }
    }
  ]
}
            """)
else:
    # Show instructions when no data is loaded
    if local_file_loaded:
        st.info("No data available. Try using one of the alternative data sources in the sidebar.")
    else:
        st.info("👈 Please use one of the data loading options in the sidebar to get started.")

# Sidebar information
st.sidebar.markdown("---")
st.sidebar.subheader("Legend")
st.sidebar.markdown("🟢 **Green dots**: Tree locations")
st.sidebar.markdown("🟠 **Orange circles**: Protection zones")
st.sidebar.markdown(f"**Current filter**: Trees ≥ {min_diameter} inches")

