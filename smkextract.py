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
    
    county_id_col = None
    for col in ["GEOID", "geoid", "COUNTYFP", "NAME", "name"]:
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
                        fval, start, end = float(val), float(f['range'][0]), float(f['range'][1])
                        if start <= fval <= end: match = True
                    except:
                        if str(f['range'][0]) <= val <= str(f['range'][1]): match = True
                
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
    
    data = {'fips': {}, 'states': {}, 'counties': {}}
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
                
                entry = {
                    'fips': fips12,
                    'state_abbr': st_abbr,
                    'name': name,
                    'timezone': tz
                }
                data['fips'][fips12] = entry
                
                # Group by state
                if st_abbr not in data['states']:
                    data['states'][st_abbr] = []
                data['states'][st_abbr].append(fips12)
                
                # Map name to fips (use state abbreviation if available)
                full_name = f"{st_abbr}:{name}".lower()
                data['counties'][full_name] = fips12
                    
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
    
    for key in ['outputs', 'county_shp', 'griddesc_path', 'input_shp', 'costcy_file']:
        if key in config:
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

    if not sector_mapping:
        print("Error: No sector mapping found in config.")
        sys.exit(1)

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
    
    if (use_states or use_counties or use_fips_expansion) and config.get('costcy_file'):
        print(f"Loading COSTCY from {config['costcy_file']}...")
        costcy_data = parse_costcy(config['costcy_file'])
    elif (use_states or use_counties or use_fips_expansion) and not config.get('costcy_file'):
        print("Warning: COSTCY file missing but required for State/County/Generic-FIPS filtering.")

    # 1. Direct FIPS Filter (with expansion)
    fips_to_filter = []
    for f in raw_fips:
        # Convert user-provided 6-digit FIPS to 12-digit format for internal use
        f_12digit = f
        if len(f) == 6 and f.isdigit():
            # User provided 6-digit (SSCCC format, e.g., "08041" = state 08 + county 041)
            # Convert to 12-digit integer format used by SMOKE: format the integer with I12.12
            # Example: 08041 (int) → "000000008041" (12-digit string)
            try:
                fips_int = int(f)
                f_12digit = f"{fips_int:012d}"
            except ValueError:
                pass  # Keep original if not convertible
        
        # Check for generic state FIPS (e.g. '053000' in 6-digit or '000000053000' in 12-digit)
        # Generic FIPS ends with '000' and is used to select all counties in a state
        if f_12digit.endswith('000') and costcy_data:
            # Extract state code: for 12-digit "000000053000", state code is at positions 6-9
            # The format is "0CCCSSCCC" where CCC=country(1)+expansion(5), SS=state, CCC=county
            # For generic FIPS ending in 000, we want all counties in that state
            state_code = f_12digit[6:9]  # Get state code (e.g., "053" from "000000053000")
            state_prefix = f"000000{state_code}"  # Prefix to match all counties in state
            expanded = [k for k in costcy_data['fips'].keys() if k.startswith(state_prefix)]
            if expanded:
                fips_to_filter.extend(expanded)
                print(f"Expanded generic FIPS {f} to {len(expanded)} counties.")
            else:
                fips_to_filter.append(f_12digit)
                print(f"Warning: Generic FIPS {f} found no matches in COSTCY.")
        else:
             fips_to_filter.append(f_12digit)

    # 2. State Filter (Lookup in COSTCY)
    if config.get('filter_states') and costcy_data:
        states = config.get('filter_states')
        if not isinstance(states, list): states = [states]
        for st in states:
            fips_list = costcy_data['states'].get(st.upper())
            if fips_list:
                fips_to_filter.extend(fips_list)
            else:
                print(f"Warning: State '{st}' not found in COSTCY.")

    # 3. County Name Filter (Lookup in COSTCY)
    if config.get('filter_counties') and costcy_data:
        counties = config.get('filter_counties')
        if not isinstance(counties, list): counties = [counties]
        for co in counties:
            fips = costcy_data['counties'].get(co.lower())
            if fips:
                fips_to_filter.append(fips)
            else:
                print(f"Warning: County '{co}' not found in COSTCY (use 'ST:Name' format).")

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
        # Convert county codes to 12-digit format if needed
        counties_spatial_12digit = []
        for county_code in counties_spatial:
            if len(county_code) == 5 and county_code.isdigit():
                # 5-digit FIPS: convert to 12-digit
                fips_int = int(county_code)
                counties_spatial_12digit.append(f"{fips_int:012d}")
            else:
                # Use as-is (already 12-digit or other format)
                counties_spatial_12digit.append(county_code)
        active_filters.append({'col_name': 'region_cd', 'vals': set(counties_spatial_12digit), 'range': None, 'is_exclusion': is_exclusion})
        print(f"Added grid filter with {len(counties_spatial_12digit)} counties.")

    # 5. Shapefile Filter (GIS) — supports single path or list of paths
    raw_shp = config.get('filter_shp')
    if raw_shp:
        shp_paths = raw_shp if isinstance(raw_shp, list) else [raw_shp]
        shp_paths = [p for p in shp_paths if p]  # drop blank list entries
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
                if len(county_code) == 5 and county_code.isdigit():
                    fips_int = int(county_code)
                    counties_spatial_all.add(f"{fips_int:012d}")
                else:
                    counties_spatial_all.add(county_code)
        if counties_spatial_all:
            active_filters.append({'col_name': 'region_cd', 'vals': counties_spatial_all, 'range': None, 'is_exclusion': is_exclusion})
            print(f"Added shapefile filter with {len(counties_spatial_all)} counties (from {len(shp_paths)} shapefile(s)).")

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
        cname = fc.get('col_name')
        if not cname: continue
        
        vals = fc.get('filtered_val')
        if vals and not isinstance(vals, list): vals = [vals]
        
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
        if filter_sector != 'all' and sector not in filter_sector:
            continue
            
        if sector in skip_sectors:
            print(f"Skipping sector '{sector}'.")
            continue
        
        sector_output = os.path.join(outputs_root, sector)
        os.makedirs(sector_output, exist_ok=True)
        sector_success, files_processed = True, 0
        n_files = len(fnames)
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
