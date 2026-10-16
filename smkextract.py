#!/proj/ie/proj/SMOKE/htran/Emission_Modeling_Platform/utils/smkextract/.venv/bin/python

import os
import sys
import csv
import argparse
import warnings
import datetime
import yaml
import geopandas as gpd
from shapely.geometry import Polygon
import pyproj
import re
import gzip

# --- Tee: write stdout to both console and log file ---
class _Tee:
    def __init__(self, *streams):
        self._streams = streams
    def write(self, data):
        for s in self._streams:
            s.write(data)
    def flush(self):
        for s in self._streams:
            s.flush()
    def fileno(self):
        return self._streams[0].fileno()

# --- GRIDDESC Parsing Logic (integrated from griddesc2shp.py) ---
def _clean_name(raw: str) -> str:
    """Helper to clean coordinate and grid names from GRIDDESC."""
    raw = raw.split('!')[0]  # drop inline comment
    raw = raw.strip()
    raw = re.sub(r"['`,]", "", raw)  # drop quotes/commas/backticks
    raw = re.sub(r"\s+", " ", raw)
    return raw.strip()

def parse_griddesc_all(path: str):
    """
    Parses a SMOKE/CMAQ GRIDDESC file containing coordinate systems and grid definitions.
    Returns: (coords_dict, grids_dict)
    """
    with open(path, 'r') as f:
        lines = f.readlines()
    try:
        sep_idx = next(i for i, ln in enumerate(lines) if "' '  !  end coords." in ln)
    except StopIteration:
        raise ValueError("Missing coordinate block terminator (' '  !  end coords.)")

    coords = {}
    i = 0
    while i < sep_idx:
        line = lines[i].strip()
        if line.startswith("'") and not line.startswith("!"):
            name = _clean_name(line)
            i += 1
            if i < sep_idx:
                params_line = lines[i].strip()
                # Handle Fortran 'D' notation
                nums = [float(tok.replace('D', 'E')) for tok in re.split(r',\s*|\s+', params_line) if tok.strip()]
                coords[name] = nums
        i += 1

    grids = {}
    i = sep_idx + 1
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("'") and not line.startswith("!") and line != "' '":
            gname = _clean_name(line)
            i += 1
            if i < len(lines):
                params_line = lines[i].strip()
                parts = [p.strip() for p in params_line.split(',') if p.strip()]
                if parts:
                    coord_ref = _clean_name(parts[0])
                    rest = [float(p.replace('D', 'E')) for p in parts[1:]]
                    grids[gname] = [coord_ref] + rest
        i += 1
    return coords, grids

def extract_grid(path: str, grid_id: str):
    """
    Extracts projection and grid parameters for a specific grid_id from a GRIDDESC file.
    """
    coords, grids = parse_griddesc_all(path)
    gid_clean = _clean_name(grid_id)
    if gid_clean not in grids:
        raise ValueError(f"Grid '{grid_id}' not found. Available: {', '.join(sorted(grids.keys()))}")
    grid_params = grids[gid_clean]
    coord_name = grid_params[0]
    if coord_name not in coords:
        raise ValueError(f"Projection '{coord_name}' referenced by grid '{gid_clean}' not defined in coords section.")
    return coords[coord_name], grid_params

# --- Default FF10 Headers ---
DEFAULT_HEADERS = {
    'FF10_POINT':['country_cd', 'region_cd', 'tribal_code', 'facility_id', 'unit_id', 'rel_point_id', 'process_id', 'agy_facility_id', 'agy_unit_id', 'agy_rel_point_id', 'agy_process_id', 'scc', 'poll', 'ann_value', 'ann_pct_red', 'facility_name', 'erptype', 'stkhgt', 'stkdiam', 'stktemp', 'stkflow', 'stkvel', 'naics', 'longitude', 'latitude', 'll_datum', 'horiz_coll_mthd', 'design_capacity', 'design_capacity_units', 'reg_codes', 'fac_source_type', 'unit_type_code', 'control_ids', 'control_measures', 'current_cost', 'cumulative_cost', 'projection_factor', 'submitter_id', 'calc_method', 'data_set_id', 'facil_category_code', 'oris_facility_code', 'oris_boiler_id', 'ipm_yn', 'calc_year', 'date_updated', 'fug_height', 'fug_width_xdim', 'fug_length_ydim', 'fug_angle', 'zipcode', 'annual_avg_hours_per_year', 'jan_value', 'feb_value', 'mar_value', 'apr_value', 'may_value', 'jun_value', 'jul_value', 'aug_value', 'sep_value', 'oct_value', 'nov_value', 'dec_value',"jan_pctred","feb_pctred","mar_pctred","apr_pctred","may_pctred","jun_pctred","jul_pctred","aug_pctred","sep_pctred","oct_pctred","nov_pctred","dec_pctred","comment"],
    'FF10_NONPOINT':['country_cd', 'region_cd', 'tribal_code', 'census_tract_cd', 'shape_id', 'scc', 'emis_type', 'poll', 'ann_value', 'ann_pct_red', 'control_ids', 'control_measures', 'current_cost', 'cumulative_cost', 'projection_factor', 'reg_codes', 'calc_method', 'calc_year', 'date_updated', 'data_set_id', 'jan_value', 'feb_value', 'mar_value', 'apr_value', 'may_value', 'jun_value', 'jul_value', 'aug_value', 'sep_value', 'oct_value', 'nov_value', 'dec_value', 'jan_pctred','feb_pctred','mar_pctred','apr_pctred','may_pctred','jun_pctred','jul_pctred','aug_pctred','sep_pctred','oct_pctred','nov_pctred','dec_pctred','comment'],
    'FF10_ACTIVITY':['country_cd', 'region_cd', 'tribal_code', 'census_tract_cd', 'shape_id', 'scc', 'CD', 'MSR', 'activity_type', 'ann_parm_value', 'calc_year', 'date_updated', 'data_set_id', 'jan_value', 'feb_value', 'mar_value', 'apr_value', 'may_value', 'jun_value', 'jul_value', 'aug_value', 'sep_value', 'oct_value', 'nov_value', 'dec_value', 'comment'],
    'FF10_HOURLY_POINT':['country_cd', 'region_cd', 'tribal_code', 'facility_id', 'unit_id', 'rel_point_id', 'process_id', 'scc', 'poll', 'op_type_cd', 'calc_method', 'date_updated', 'date', 'daytot', 'hrval0', 'hrval1', 'hrval2', 'hrval3', 'hrval4', 'hrval5', 'hrval6', 'hrval7', 'hrval8', 'hrval9', 'hrval10', 'hrval11', 'hrval12', 'hrval13', 'hrval14', 'hrval15', 'hrval16', 'hrval17', 'hrval18', 'hrval19', 'hrval20', 'hrval21', 'hrval22', 'hrval23', 'comment'],
    'FF10_NONROAD':['country_cd', 'region_cd', 'tribal_code', 'census_tract_cd', 'shape_id', 'scc', 'emis_type', 'poll', 'ann_value', 'ann_pct_red', 'control_ids', 'control_measures', 'current_cost', 'cumulative_cost', 'projection_factor', 'reg_codes', 'calc_method', 'calc_year', 'date_updated', 'data_set_id', 'jan_value', 'feb_value', 'mar_value', 'apr_value', 'may_value', 'jun_value', 'jul_value', 'aug_value', 'sep_value', 'oct_value', 'nov_value', 'dec_value','jan_pctred','feb_pctred','mar_pctred','apr_pctred','may_pctred','jun_pctred','jul_pctred','aug_pctred','sep_pctred','oct_pctred','nov_pctred','dec_pctred','comment'],
    'FF10_ONROAD':['country_cd', 'region_cd', 'tribal_code', 'facility_id', 'unit_id', 'rel_point_id', 'process_id', 'scc', 'emis_type', 'poll', 'ann_value', 'ann_pct_red', 'control_ids', 'control_measures', 'current_cost', 'cumulative_cost', 'projection_factor', 'reg_codes', 'calc_method', 'calc_year', 'date_updated', 'data_set_id', 'jan_value', 'feb_value', 'mar_value', 'apr_value', 'may_value', 'jun_value', 'jul_value', 'aug_value', 'sep_value', 'oct_value', 'nov_value', 'dec_value','jan_pctred','feb_pctred','mar_pctred','apr_pctred','may_pctred','jun_pctred','jul_pctred','aug_pctred','sep_pctred','oct_pctred','nov_pctred','dec_pctred','comment'],
    'FF10_DAILY_POINT':['country_cd','region_cd','tribal_code','facility_id','unit_id','rel_point_id','process_id','scc','poll','op_type_cd','calc_method','date_updated','monthnum','monthtot','dayval1','dayval2','dayval3','dayval4','dayval5','dayval6','dayval7','dayval8','dayval9','dayval10','dayval11','dayval12','dayval13','dayval14','dayval15','dayval16','dayval17','dayval18','dayval19','dayval20','dayval21','dayval22','dayval23','dayval24','dayval25','dayval26','dayval27','dayval28','dayval29','dayval30','dayval31','comment'],
    'FF10_DAILY_NONPOINT':['country_cd','region_cd','tribal_code','census_tract','shape_id','tbd','emis_type','scc','poll','op_type_cd','calc_method','date_updated','monthnum','monthtot','dayval1','dayval2','dayval3','dayval4','dayval5','dayval6','dayval7','dayval8','dayval9','dayval10','dayval11','dayval12','dayval13','dayval14','dayval15','dayval16','dayval17','dayval18','dayval19','dayval20','dayval21','dayval22','dayval23','dayval24','dayval25','dayval26','dayval27','dayval28','dayval29','dayval30','dayval31','comment']
}

def split_csv_line(line):
    return next(csv.reader([line], skipinitialspace=False))

def get_ff10_type(path):
    opener = gzip.open if path.endswith('.gz') else open
    mode = 'rt' if path.endswith('.gz') else 'r'
    
    try:
        with opener(path, mode, encoding='utf-8') as f:
            first_line = f.readline()
    except (UnicodeDecodeError, EOFError):
        with opener(path, mode, encoding='latin-1') as f:
            first_line = f.readline()

    ff10_fmt = None
    if 'FF10_POINT_DAILY' in first_line or 'FF10_DAILY_POINT' in first_line:
        ff10_fmt = 'FF10_DAILY_POINT'
    elif 'FF10_DAILY_NONPOINT' in first_line:
        ff10_fmt = 'FF10_DAILY_NONPOINT'
    elif 'FF10_ACTIVITY' in first_line:
        ff10_fmt = 'FF10_ACTIVITY'
    elif 'FF10_HOURLY_POINT' in first_line:
        ff10_fmt = 'FF10_HOURLY_POINT'
    elif 'FF10_ONROAD' in first_line:
        ff10_fmt = 'FF10_ONROAD'
    elif 'FF10_NONROAD' in first_line:
        ff10_fmt = 'FF10_NONROAD'
    elif 'FF10_NONPOINT' in first_line:
        ff10_fmt = 'FF10_NONPOINT'
    elif 'FF10_POINT' in first_line:
        ff10_fmt = 'FF10_POINT'
    else:
        warnings.warn(f"Unknown FF10 file format identifier in first line of {path}.")

    expected_headers = DEFAULT_HEADERS.get(ff10_fmt)

    encoding = 'utf-8'
    try:
        with opener(path, mode, encoding='utf-8') as f:
            f.read(1024)
    except (UnicodeDecodeError, EOFError):
        encoding = 'latin-1'

    return ff10_fmt, expected_headers, encoding

_COUNTY_CACHE = {}

def get_intersecting_counties(domain_gdf, county_shp_path):
    global _COUNTY_CACHE
    if county_shp_path in _COUNTY_CACHE:
        counties = _COUNTY_CACHE[county_shp_path]
    else:
        print(f"Loading reference county shapefile from {county_shp_path}...")
        counties = gpd.read_file(county_shp_path)
        _COUNTY_CACHE[county_shp_path] = counties
        
    if counties.crs != domain_gdf.crs:
        counties = counties.to_crs(domain_gdf.crs)
    intersecting = gpd.sjoin(counties, domain_gdf, how="inner", predicate="intersects")
    
    # Look for full FIPS / GEOID column first
    county_id_col = None
    for col in ["FIPS", "fips", "GEOID", "geoid"]:
        if col in intersecting.columns:
            county_id_col = col
            break
    if county_id_col:
        unique_counties = intersecting[[county_id_col]].drop_duplicates()
        return unique_counties[county_id_col].astype(str).tolist()

    # If separate STATEFP and COUNTYFP columns exist, combine them into 5-digit FIPS
    cols_lower = {c.lower(): c for c in intersecting.columns}
    if 'statefp' in cols_lower and 'countyfp' in cols_lower:
        st_col = cols_lower['statefp']
        co_col = cols_lower['countyfp']
        combined = intersecting[st_col].astype(str).str.zfill(2) + intersecting[co_col].astype(str).str.zfill(3)
        return combined.drop_duplicates().tolist()

    for col in ["COUNTYFP", "countyfp", "NAME", "name"]:
        if col in intersecting.columns:
            county_id_col = col
            break
    if county_id_col:
        unique_counties = intersecting[[county_id_col]].drop_duplicates()
        return unique_counties[county_id_col].astype(str).tolist()
    else:
        return intersecting.index.astype(str).tolist()

def create_domain_gdf(griddesc_path, grid_id):
    coord_params, grid_params = extract_grid(griddesc_path, grid_id)
    proj_type, p_alpha, p_beta, p_gamma, x_cent, y_cent = coord_params
    _, xorig, yorig, xcell, ycell, ncols, nrows, _ = grid_params
    
    proj_str = (
        f"+proj=lcc +lat_1={p_alpha} +lat_2={p_beta} +lat_0={y_cent} "
        f"+lon_0={x_cent} +a=6370000.0 +b=6370000.0 +x_0=0 +y_0=0 +units=m +no_defs"
    )
    lcc_proj = pyproj.Proj(proj_str)
    wgs84_proj = pyproj.Proj(proj='latlong', datum='WGS84')
    transformer = pyproj.Transformer.from_proj(lcc_proj, wgs84_proj, always_xy=True)
    
    features = []
    rows_attr = []
    cols_attr = []
    for r in range(1, int(nrows)+1):
        y0 = yorig + (r-1) * ycell
        y1 = y0 + ycell
        for c in range(1, int(ncols)+1):
            x0 = xorig + (c-1) * xcell
            x1 = x0 + xcell
            pts_proj = [(x0, y0), (x1, y0), (x1, y1), (x0, y1)]
            pts_ll = [transformer.transform(px, py) for (px, py) in pts_proj]
            poly = Polygon(pts_ll + [pts_ll[0]])
            features.append(poly)
            rows_attr.append(r)
            cols_attr.append(c)
            
    gdf = gpd.GeoDataFrame(
        {
            'name': [grid_id]*len(features),
            'ROWNUM': rows_attr,
            'COLNUM': cols_attr
        },
        geometry=features,
        crs='EPSG:4326'
    )
    return gdf

def stream_extract(input_path, output_path, filters):
    ff10_fmt, expected_headers, encoding = get_ff10_type(input_path)
    count = 0
    opener = gzip.open if input_path.endswith('.gz') else open
    mode = 'rt' if input_path.endswith('.gz') else 'r'
    
    with opener(input_path, mode, encoding=encoding) as fin, \
         open(output_path, 'w', encoding=encoding) as fout:
        
        header_cols = None
        filter_indices = []
        today = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for line_raw in fin:
            line = line_raw.rstrip('\n')
            if line.strip() == '' or line.lstrip().startswith('#'):
                fout.write(line_raw)
                continue
            
            if header_cols is None:
                cand_vals = [c.strip() for c in split_csv_line(line)]
                cand_lower = [c.lower() for c in cand_vals]
                
                if 'region_cd' in cand_lower or 'country_cd' in cand_lower or 'scc' in cand_lower:
                    header_cols = cand_vals
                    is_header = True
                else:
                    header_cols = expected_headers
                    if header_cols is None:
                        warnings.warn(f"No header found for {input_path}. Copying as-is.")
                        fout.write(line_raw)
                        for rem in fin: fout.write(rem)
                        return 0
                    is_header = False
                
                name_to_idx = {name.lower(): idx for idx, name in enumerate(header_cols)}
                for alt in ['region', 'fips']:
                    if alt in name_to_idx and 'region_cd' not in name_to_idx:
                        name_to_idx['region_cd'] = name_to_idx[alt]

                for f in filters:
                    idx = name_to_idx.get(f['col_name'].lower())
                    if idx is not None:
                        filter_indices.append((idx, f))
                    else:
                        warnings.warn(f"Filter column '{f['col_name']}' not found in {input_path}.")
                
                fout.write(f"#Extracted from {input_path} on {today}\n")
                for _, f in filter_indices:
                    mode = "exclusion" if f.get('is_exclusion') else "extraction"
                    crit = f"values: {list(f['vals'])[:20]}{'...' if len(f['vals']) > 20 else ''}" if f['vals'] else f"range: {f['range']}"
                    fout.write(f"#Filter: {f['col_name']} [{mode}] {crit}\n")
                
                if is_header:
                    fout.write(line_raw)
                    continue
            
            values = split_csv_line(line)
            passes_all = True
            for idx, f in filter_indices:
                if idx >= len(values):
                    # For grid/gis filters, sometimes records might not have FIPS. 
                    # If extraction mode, we fail them. If exclusion mode, they pass.
                    passed = f.get('is_exclusion', False)
                    if not passed:
                        passes_all = False
                        break
                    continue
                    
                val = values[idx].strip().strip('"')
                
                match = False
                if f['vals'] is not None:
                    # Clean the field value for comparison
                    if val in f['vals']: 
                        match = True
                    # Special robust handling for geographic region_cd (FIPS):
                    # Canonicalize any numeric code of length 1-6 to SMOKE 12-digit format
                    elif f['col_name'] == 'region_cd' and val.isdigit() and 1 <= len(val) <= 6:
                        try:
                            fips_12digit = f"{int(val):012d}"
                            if fips_12digit in f['vals']: 
                                match = True
                        except ValueError:
                            pass

                elif f['range'] is not None:
                    try:
                        fval = float(val)
                        start = float(f['range'][0]) if f['range'][0] is not None else float('-inf')
                        end = float(f['range'][1]) if f['range'][1] is not None else float('inf')
                        if start <= fval <= end:
                            match = True
                    except (ValueError, TypeError):
                        start_str = str(f['range'][0]) if f['range'][0] is not None else ""
                        end_str = str(f['range'][1]) if f['range'][1] is not None else "\uffff"
                        if start_str <= val <= end_str:
                            match = True
                
                passed = (not match) if f.get('is_exclusion') else match
                if not passed:
                    passes_all = False
                    break
            
            if passes_all:
                fout.write(line_raw)
                count += 1
    return count

def parse_costcy(path):
    """
    Parses COSTCY file to extract mapping of names/states to 6-digit FIPS.
    Uses fixed-width column extraction matching SMOKE's rdstcy.f parsing.
    
    Fixed-width column positions (1-based, matching Fortran):
    - Columns 2-3: State abbreviation
    - Columns 5-24: County name
    - Column 26: Country code
    - Columns 27-28: State code
    - Columns 29-31: County code
    - Columns 40-42: Time zone
    
    Note: File lines are wrapped at ~80 characters, so we check if a line looks
    like a county record (starts with space + 2-char state + spaces).
    """
    if not os.path.isfile(path):
        return {}
    
    data = {'fips': {}, 'states': {}, 'counties': {}, 'entries': []}
    in_county = False
    
    with open(path, 'r', encoding='latin-1') as f:
        for line in f:
            line = line.rstrip('\n')
            
            if '/COUNTY/' in line:
                in_county = True
                continue
            if line.startswith('/') and in_county:
                break
            if not in_county or line.startswith('#') or not line.strip():
                continue
            
            # Must be long enough for our extraction (at least 42 chars for timezone)
            if len(line) < 42:
                continue
            
            # Check if this looks like a county line:
            # Starts with space, then 2-char state abbr, then spaces
            if not (line[0:1] == ' ' and len(line) > 2):
                continue
            
            try:
                # Extract using fixed-width columns (Python uses 0-based indexing)
                # Fortran column 1 is Python index 0, so Fortran columns 2-3 are Python [1:3]
                
                st_abbr = line[1:3].strip()
                
                # Fortran columns 5-24 → Python indices 4:24 (county name)
                name = line[4:24].strip()
                
                # Fortran column 26 → Python index 25
                country_str = line[25:26].strip()
                
                # Fortran columns 27-28 → Python indices 26:28
                state_str = line[26:28].strip()
                
                # Fortran columns 29-31 → Python indices 28:31
                county_str = line[28:31].strip()
                
                # Fortran columns 40-42 → Python indices 39:42
                tz = line[39:42].strip()
                
                # Validate required fields
                if not st_abbr or not name or not tz:
                    continue
                
                # Validate and convert to integers
                try:
                    country = int(country_str) if country_str else 0
                    state = int(state_str) if state_str else 0
                    county = int(county_str) if county_str else 0
                except ValueError:
                    continue
                
                # Build 12-digit FIPS code following SMOKE/rdstcy.f format
                # country (1 digit) + state (2 digits) + county (3 digits)
                # Matches rdstcy.f: FIP = COU*100000 + STA*1000 + CNY; WRITE(CFIP,'(I12.12)')
                fip = country * 100000 + state * 1000 + county
                fips12 = f"{fip:012d}"
                fips6 = f"{country}{state:02d}{county:03d}"
                fips5 = f"{state:02d}{county:03d}" if country == 0 else None
                
                entry = {
                    'country': country,
                    'state': state,
                    'county': county,
                    'fips6': fips6,
                    'fips12': fips12,
                    'fips5': fips5,
                    'state_abbr': st_abbr,
                    'name': name,
                    'timezone': tz
                }
                data['entries'].append(entry)
                data['fips'][fips12] = entry
                data['fips'][fips6] = entry
                if fips5:
                    data['fips'][fips5] = entry
                
                # Group by state
                if st_abbr not in data['states']:
                    data['states'][st_abbr] = []
                data['states'][st_abbr].append(entry)
                
                # Map name to entry
                full_name = f"{st_abbr}:{name}".lower()
                data['counties'][full_name] = entry
                
                # Support variations with/without 'Co' or 'County'
                name_lower = name.lower()
                if name_lower.endswith(' co'):
                    base = name_lower[:-3].strip()
                    data['counties'][f"{st_abbr}:{base}".lower()] = entry
                    data['counties'][f"{st_abbr}:{base} county".lower()] = entry
                elif name_lower.endswith(' county'):
                    base = name_lower[:-7].strip()
                    data['counties'][f"{st_abbr}:{base}".lower()] = entry
                    data['counties'][f"{st_abbr}:{base} co".lower()] = entry
                else:
                    data['counties'][f"{st_abbr}:{name_lower} co".lower()] = entry
                    data['counties'][f"{st_abbr}:{name_lower} county".lower()] = entry
                    
            except (ValueError, IndexError):
                # Skip malformed lines
                continue

    return data

def load_config(config_path):
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Config not found: {config_path}")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    config_dir = os.path.dirname(os.path.abspath(config_path))
    def resolve(path):
        if path and not os.path.isabs(path):
            return os.path.normpath(os.path.join(config_dir, path))
        return path
    
    for key in ['outputs', 'county_shp', 'griddesc_path', 'input_shp', 'costcy_file', 'filter_shp']:
        if key in config:
            if isinstance(config[key], list):
                config[key] = [resolve(p) for p in config[key] if p]
            else:
                config[key] = resolve(config[key])
    return config

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consolidated SMOKE extractor (Strict YAML).")
    parser.add_argument("--config", help="Path to smkextract.yaml configuration.")
    args = parser.parse_args()
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = args.config if args.config else os.path.join(script_dir, "smkextract.yaml")
    
    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found at {config_path}.")
        sys.exit(1)

    print(f"Loading configuration from {config_path}")
    config = load_config(config_path)
    
    outputs_root = config.get('outputs', '')
    sector_mapping = config.get('sector', {})
    
    # Global Filter Mode
    global_filter_mode = config.get('filter_mode', 'extraction').lower()
    valid_filter_modes = {'extraction', 'extract', 'exclusion', 'exclude'}
    if global_filter_mode not in valid_filter_modes:
        warnings.warn(
            f"Unrecognized filter_mode '{global_filter_mode}'. "
            f"Expected one of: {', '.join(sorted(valid_filter_modes))}. Defaulting to 'extraction'."
        )
        global_filter_mode = 'extraction'
    is_exclusion = global_filter_mode in ['exclusion', 'exclude']
    print(f"Global Filter Mode: {global_filter_mode.upper()} {'(Excluding matches)' if is_exclusion else '(Keeping matches)'}")

    raw_skip = config.get('skip_sector', config.get('sector_skip', []))
    is_auto_mode = (raw_skip == 'auto' or (isinstance(raw_skip, list) and 'auto' in raw_skip))
    
    if isinstance(raw_skip, list):
        skip_sectors = set(raw_skip)
    elif raw_skip == 'auto':
        skip_sectors = set()
    else:
        skip_sectors = set()

    filter_sector = config.get('filter_sector', 'all')
    if isinstance(filter_sector, list):
        filter_sector = set(filter_sector)
    is_all_sectors = (filter_sector == 'all' or (isinstance(filter_sector, (set, list)) and 'all' in filter_sector))

    if not sector_mapping:
        print("Error: No sector mapping found in config.")
        sys.exit(1)

    # Validate all top-level configuration keywords
    valid_config_keys = {
        'outputs', 'filename_prepend', 'filename_append',
        'filter_sector', 'skip_sector', 'sector_skip',
        'filter_mode', 'costcy_file', 'filter_fips',
        'filter_states', 'filter_counties', 'filter_grid',
        'griddesc_path', 'filter_shp', 'input_shp', 'county_shp',
        'filter_cols', 'filter_col', 'filtered_val', 'start_val',
        'end_val', 'sector'
    }
    unrecognized_config_keys = set(config.keys()) - valid_config_keys
    if unrecognized_config_keys:
        warnings.warn(
            f"Unrecognized keyword(s) in configuration file: {', '.join(sorted(unrecognized_config_keys))}. "
            f"Expected: {', '.join(sorted(valid_config_keys))}"
        )

    # Pre-flight companion file dependency validation
    if config.get('filter_grid'):
        if not config.get('griddesc_path') or not os.path.isfile(config['griddesc_path']):
            raise FileNotFoundError(f"filter_grid is set but griddesc_path is missing or not found: {config.get('griddesc_path')}")
        if not config.get('county_shp') or not os.path.isfile(config['county_shp']):
            raise FileNotFoundError(f"filter_grid is set but county_shp is missing or not found: {config.get('county_shp')}")

    shp_check = config.get('filter_shp') or config.get('input_shp')
    if shp_check:
        if not config.get('county_shp') or not os.path.isfile(config['county_shp']):
            raise FileNotFoundError(f"filter_shp/input_shp is set but county_shp is missing or not found: {config.get('county_shp')}")

    os.makedirs(outputs_root, exist_ok=True)

    # --- Log file setup ---
    config_stem = os.path.splitext(os.path.basename(config_path))[0]
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_path = os.path.join(outputs_root, f"smkextract_{config_stem}_{timestamp}.log")
    _log_fh = open(log_path, 'w', encoding='utf-8')
    _log_fh.write(f"# generated by smkextract with config: {os.path.abspath(config_path)}\n")
    _orig_stdout = sys.stdout
    _orig_showwarning = warnings.showwarning
    sys.stdout = _Tee(_orig_stdout, _log_fh)
    def _warn_to_stdout(message, category, filename, lineno, file=None, line=None):
        sys.stdout.write(f"{category.__name__}: {message}\n")
    warnings.showwarning = _warn_to_stdout
    print(f"Log file: {log_path}")
    # --- End log file setup ---

    active_filters = []
    
    # Check for generic FIPS in filter_fips (ending in 000) to trigger expansion
    raw_fips = config.get('filter_fips', [])
    if raw_fips is None: raw_fips = []
    if not isinstance(raw_fips, list): 
        raw_fips = [str(raw_fips)]
    else: 
        raw_fips = [str(f) for f in raw_fips]
    
    use_fips_expansion = any(f.endswith('000') for f in raw_fips)
    
    # COSTCY Loading if needed (Only if using State, County Name filters, or Generic FIPS)
    costcy_data = None
    use_states = config.get('filter_states') and len(config.get('filter_states')) > 0
    use_counties = config.get('filter_counties') and len(config.get('filter_counties')) > 0
    
    if use_states or use_counties or use_fips_expansion:
        costcy_path = config.get('costcy_file')
        if not costcy_path or not os.path.isfile(costcy_path):
            raise FileNotFoundError(f"COSTCY file missing or not found at '{costcy_path}', but required for State/County/Generic-FIPS filtering.")
        print(f"Loading COSTCY from {costcy_path}...")
        costcy_data = parse_costcy(costcy_path)

    # 1. Direct FIPS Filter (Follows COSTCY C-SS-CCC structure)
    fips_to_filter = []
    for f in raw_fips:
        f_str = str(f).strip()
        if not f_str:
            continue

        if f_str.isdigit():
            val_int = int(f_str)
            if len(f_str) == 5:
                # 5-digit US FIPS (SSCCC, implicit Country 0)
                c_code, s_code, co_code = 0, val_int // 1000, val_int % 1000
            elif len(f_str) == 6:
                # 6-digit SMOKE FIPS (CSSCCC)
                c_code, s_code, co_code = val_int // 100000, (val_int % 100000) // 1000, val_int % 1000
            elif len(f_str) == 12:
                # 12-digit SMOKE FIPS (COU*100000 + STA*1000 + CNY)
                c_code, s_code, co_code = val_int // 100000, (val_int % 100000) // 1000, val_int % 1000
            else:
                c_code, s_code, co_code = None, None, None

            # Check for generic state FIPS in COSTCY (County == 0, e.g. '036000' or '36000')
            if c_code is not None and co_code == 0 and costcy_data:
                expanded = [
                    e for e in costcy_data.get('entries', [])
                    if e['country'] == c_code and e['state'] == s_code and e['county'] > 0
                ]
                if expanded:
                    for e in expanded:
                        fips_to_filter.extend([e['fips6'], e['fips12']])
                        if e['fips5']:
                            fips_to_filter.append(e['fips5'])
                    print(f"Expanded generic FIPS {f} to {len(expanded)} counties in country {c_code}, state {s_code}.")
                else:
                    fips_to_filter.append(f_str)
                    print(f"Warning: Generic FIPS {f} found no matching counties in COSTCY.")
            elif c_code is not None:
                # Specific county FIPS: populate canonical forms (6-digit, 12-digit, and 5-digit if US)
                fips6 = f"{c_code}{s_code:02d}{co_code:03d}"
                fips12 = f"{c_code * 100000 + s_code * 1000 + co_code:012d}"
                fips_to_filter.extend([f_str, fips6, fips12])
                if c_code == 0:
                    fips_to_filter.append(f"{s_code:02d}{co_code:03d}")
            else:
                fips_to_filter.append(f_str)
        else:
            fips_to_filter.append(f_str)

    # 2. State Filter (Lookup in COSTCY)
    if config.get('filter_states') and costcy_data:
        states = config.get('filter_states')
        if not isinstance(states, list): states = [states]
        for st in states:
            entries = costcy_data['states'].get(st.upper(), [])
            if entries:
                for e in entries:
                    fips_to_filter.extend([e['fips6'], e['fips12']])
                    if e['fips5']:
                        fips_to_filter.append(e['fips5'])
            else:
                print(f"Warning: State '{st}' not found in COSTCY.")

    # 3. County Name Filter (Strictly Enforces 'ST:County Name' Format)
    if config.get('filter_counties') and costcy_data:
        counties = config.get('filter_counties')
        if not isinstance(counties, list): counties = [counties]
        for co in counties:
            co_str = str(co).strip()
            if not co_str:
                continue

            # Strict check: Reject numeric FIPS codes
            if co_str.isdigit():
                print(f"Error in filter_counties: '{co_str}' is a numeric FIPS code. "
                      f"'filter_counties' strictly accepts county names in 'ST:County Name' format "
                      f"(e.g., 'NY:Queens Co'). Please move numeric FIPS codes to 'filter_fips'.")
                continue

            # Strict check: Require 'ST:Name' format
            if ':' not in co_str:
                print(f"Error in filter_counties: '{co_str}' does not match 'ST:County Name' format "
                      f"(e.g., 'NY:Queens Co').")
                continue

            st_part, name_part = [p.strip() for p in co_str.split(':', 1)]
            if len(st_part) != 2 or not st_part.isalpha() or not name_part:
                print(f"Error in filter_counties: '{co_str}' must have a valid 2-letter state abbreviation "
                      f"(e.g., 'NY:Queens Co').")
                continue

            entry = costcy_data['counties'].get(co_str.lower())
            if entry:
                fips_to_filter.extend([entry['fips6'], entry['fips12']])
                if entry['fips5']:
                    fips_to_filter.append(entry['fips5'])
            else:
                print(f"Warning: County '{co}' not found in COSTCY.")

    if fips_to_filter:
        active_filters.append({
            'col_name': 'region_cd',
            'vals': set(fips_to_filter),
            'range': None,
            'is_exclusion': is_exclusion
        })
        print(f"Added FIPS filter with {len(fips_to_filter)} unique codes.")

    # 4. Grid Filter
    if config.get('filter_grid') and config.get('griddesc_path'):
        print(f"Creating grid domain for {config['filter_grid']}...")
        grid_gdf = create_domain_gdf(config['griddesc_path'], config['filter_grid'])
        counties_spatial = get_intersecting_counties(grid_gdf, config['county_shp'])
        counties_spatial_raw = set()
        counties_spatial_all = set()
        for county_code in counties_spatial:
            counties_spatial_raw.add(county_code)
            counties_spatial_all.add(county_code)
            if county_code.isdigit() and 1 <= len(county_code) <= 6:
                fips_int = int(county_code)
                counties_spatial_all.add(f"{fips_int:012d}")
                counties_spatial_all.add(f"{fips_int:05d}")
                counties_spatial_all.add(f"{fips_int:06d}")
        active_filters.append({'col_name': 'region_cd', 'vals': counties_spatial_all, 'range': None, 'is_exclusion': is_exclusion})
        print(f"Added grid filter with {len(counties_spatial_raw)} counties.")

    # 5. Shapefile Filter (GIS) — supports single path or list of paths
    raw_shp = config.get('filter_shp') or config.get('input_shp')
    if raw_shp:
        shp_paths = raw_shp if isinstance(raw_shp, list) else [raw_shp]
        shp_paths = [p for p in shp_paths if p]  # drop blank list entries
        counties_spatial_raw = set()
        counties_spatial_all = set()
        for shp_path in shp_paths:
            shp_path = shp_path.strip() if isinstance(shp_path, str) else shp_path
            if not shp_path:
                continue
            if not os.path.isfile(shp_path):
                print(f"Warning: filter_shp is defined but file not found: {shp_path}")
                continue
            print(f"Loading input shapefile {shp_path}...")
            gis_gdf = gpd.read_file(shp_path)
            counties_spatial = get_intersecting_counties(gis_gdf, config['county_shp'])
            for county_code in counties_spatial:
                counties_spatial_raw.add(county_code)
                counties_spatial_all.add(county_code)
                if county_code.isdigit() and 1 <= len(county_code) <= 6:
                    fips_int = int(county_code)
                    counties_spatial_all.add(f"{fips_int:012d}")
                    counties_spatial_all.add(f"{fips_int:05d}")
                    counties_spatial_all.add(f"{fips_int:06d}")
        if counties_spatial_all:
            active_filters.append({'col_name': 'region_cd', 'vals': counties_spatial_all, 'range': None, 'is_exclusion': is_exclusion})
            print(f"Added shapefile filter with {len(counties_spatial_raw)} counties (from {len(shp_paths)} shapefile(s)).")

    # 6. Column filters (Supports multiple columns and per-filter modes)
    col_filters = config.get('filter_cols', [])
    if not isinstance(col_filters, list):
        col_filters = [col_filters] if col_filters else []
    
    # Backward compatibility for legacy single filter
    if config.get('filter_col'):
        col_filters.append({
            'col_name': config['filter_col'],
            'filtered_val': config.get('filtered_val'),
            'start_val': config.get('start_val'),
            'end_val': config.get('end_val'),
            'filter_mode': config.get('filter_mode')
        })

    for fc in col_filters:
        if not isinstance(fc, dict): continue
        recognized_keys = {'col_name', 'filtered_val', 'start_val', 'end_val', 'filter_mode'}
        unrecognized = set(fc.keys()) - recognized_keys
        if unrecognized:
            warnings.warn(
                f"Unrecognized keyword(s) in filter_cols: {', '.join(sorted(unrecognized))}. "
                f"Expected: {', '.join(sorted(recognized_keys))}"
            )

        cname = fc.get('col_name')
        if not cname: continue
        
        vals = fc.get('filtered_val')
        if vals and not isinstance(vals, list): vals = [vals]
        
        has_range = (fc.get('start_val') is not None or fc.get('end_val') is not None)
        if not vals and not has_range:
            print(f"Warning: Column filter '{cname}' has no values or range specified. Skipping.")
            continue

        # Determine mode for this specific filter (prioritize local 'filter_mode')
        local_mode = fc.get('filter_mode')
        if local_mode:
            local_is_exclusion = local_mode.lower() in ['exclusion', 'exclude']
        else:
            local_is_exclusion = is_exclusion # Fallback to global setting

        active_filters.append({
            'col_name': cname,
            'vals': set(str(v) for v in vals) if vals else None,
            'range': (fc.get('start_val'), fc.get('end_val')) if fc.get('start_val') is not None else None,
            'is_exclusion': local_is_exclusion
        })
        print(f"Added column filter for '{cname}' ({'EXCLUSION' if local_is_exclusion else 'EXTRACTION'})")

    # Processing Loop
    for sector, fnames in sector_mapping.items():
        if not is_all_sectors and sector not in filter_sector:
            continue
            
        if sector in skip_sectors:
            print(f"Skipping sector '{sector}'.")
            continue
        
        sector_output = os.path.join(outputs_root, sector)
        os.makedirs(sector_output, exist_ok=True)
        sector_success, files_processed = True, 0
        n_files = len(fnames)
        used_out_names = set()
        for i, fname in enumerate(fnames):
            ff10_path = fname
            
            if not os.path.exists(ff10_path):
                warnings.warn(f"File not found: {ff10_path}. Skipping.")
                sector_success = False
                continue

            try:
                fn = os.path.basename(ff10_path)
                # Robust extension splitting for compound extensions like .csv.gz
                if fn.endswith('.csv.gz'):
                    base, ext = fn[:-7], '.csv.gz'
                elif fn.endswith('.txt.gz'):
                    base, ext = fn[:-7], '.txt.gz'
                else:
                    base, ext = os.path.splitext(fn)

                prepend = config.get('filename_prepend') or ""
                append = config.get('filename_append') or ""
                
                if prepend:
                    # Handle multiple input files targeting the same sector to avoid overwrites
                    suffix = f"_{i+1}" if n_files > 1 else ""
                    out_name = f"{prepend}{append}{suffix}{ext}"
                else:
                    out_name = f"{base}{append}{ext}"
                    if out_name in used_out_names:
                        out_name = f"{base}{append}_{i+1}{ext}"
                used_out_names.add(out_name)
                    
                outfile = os.path.join(sector_output, out_name)
                print(f"Processing {ff10_path} -> {outfile}")
                count = stream_extract(ff10_path, outfile, active_filters)
                if count == 0:
                    try:
                        os.remove(outfile)
                    except OSError:
                        pass
                    warnings.warn(
                        f"No records matched filters in {ff10_path}. "
                        f"Output file not written: {outfile}"
                    )
                else:
                    print(f"  Wrote {count} lines.")
                    files_processed += 1
            except Exception as e:
                print(f"  Error processing {ff10_path}: {e}")
                sector_success = False
                break
        
        if sector_success and files_processed > 0 and is_auto_mode:
            try:
                with open(config_path, 'r') as f:
                    lines = f.readlines()
                
                # Get current skip list for logical check
                temp_data = yaml.safe_load("".join(lines)) or {}
                skip = temp_data.get('skip_sector', temp_data.get('sector_skip', []))
                
                if skip == 'auto':
                    skip = ['auto'] # Initialize as list with auto marker
                elif not isinstance(skip, list):
                    skip = []
                
                if sector not in skip:
                    skip.append(sector)
                    # We'll replace the existing line to preserve formatting/comments
                    with open(config_path, 'w') as f:
                        found = False
                        for line in lines:
                            # Match skip_sector or legacy sector_skip
                            if not found and (line.strip().startswith('skip_sector:') or line.strip().startswith('sector_skip:')):
                                indent = line[:line.find('s')]
                                # Use flow style for the internal list to keep it on one line if possible
                                list_str = yaml.dump(skip, default_flow_style=True).strip()
                                f.write(f"{indent}skip_sector: {list_str}\n")
                                found = True
                            else:
                                f.write(line)
                    print(f"Sector '{sector}' added to skip list (formatting preserved).")
            except Exception as e:
                print(f"Warning: Could not update config: {e}")

    print("All sectors processed.")

    # --- Teardown log file ---
    warnings.showwarning = _orig_showwarning
    sys.stdout = _orig_stdout
    _log_fh.close()
    print(f"Log written to: {log_path}")
