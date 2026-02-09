#!/nas/longleaf/home/tranhuy/software/pkg/miniconda3/envs/AERMOD/bin/python

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

# Import the function from griddesc2shp.py
try:
    from griddesc2shp import extract_grid
except ImportError:
    warnings.warn("Could not import extract_grid from griddesc2shp.py. Grid extraction might fail if not found.")
    def extract_grid(*args, **kwargs):
        raise ImportError("extract_grid function not found. Please ensure griddesc2shp.py is in the search path.")

# --- Default FF10 Headers ---
DEFAULT_HEADERS = {
    'FF10_POINT':['country_cd', 'region_cd', 'tribal_code', 'facility_id', 'unit_id', 'rel_point_id', 'process_id', 'agy_facility_id', 'agy_unit_id', 'agy_rel_point_id', 'agy_process_id', 'scc', 'poll', 'ann_value', 'ann_pct_red', 'facility_name', 'erptype', 'stkhgt', 'stkdiam', 'stktemp', 'stkflow', 'stkvel', 'naics', 'longitude', 'latitude', 'll_datum', 'horiz_coll_mthd', 'design_capacity', 'design_capacity_units', 'reg_codes', 'fac_source_type', 'unit_type_code', 'control_ids', 'control_measures', 'current_cost', 'cumulative_cost', 'projection_factor', 'submitter_id', 'calc_method', 'data_set_id', 'facil_category_code', 'oris_facility_code', 'oris_boiler_id', 'ipm_yn', 'calc_year', 'date_updated', 'fug_height', 'fug_width_xdim', 'fug_length_ydim', 'fug_angle', 'zipcode', 'annual_avg_hours_per_year', 'jan_value', 'feb_value', 'mar_value', 'apr_value', 'may_value', 'jun_value', 'jul_value', 'aug_value', 'sep_value', 'oct_value', 'nov_value', 'dec_value',"jan_pctred","feb_pctred","mar_pctred","apr_pctred","may_pctred","jun_pctred","jul_pctred","aug_pctred","sep_pctred","oct_pctred","nov_pctred","dec_pctred","comment"],
    'FF10_NONPOINT':['country_cd', 'region_cd', 'tribal_code', 'census_tract_cd', 'shape_id', 'scc', 'emis_type', 'poll', 'ann_value', 'ann_pct_red', 'control_ids', 'control_measures', 'current_cost', 'cumulative_cost', 'projection_factor', 'reg_codes', 'calc_method', 'calc_year', 'date_updated', 'data_set_id', 'jan_value', 'feb_value', 'mar_value', 'apr_value', 'may_value', 'jun_value', 'jul_value', 'aug_value', 'sep_value', 'oct_value', 'nov_value', 'dec_value', 'jan_pctred','feb_pctred','mar_pctred','apr_pctred','may_pctred','jun_pctred','jul_pctred','aug_pctred','sep_pctred','oct_pctred','nov_pctred','dec_pctred','comment'],
    'FF10_ACTIVITY':['country_cd', 'region_cd', 'tribal_code', 'census_tract_cd', 'shape_id', 'scc', 'CD', 'MSR', 'activity_type', 'ann_parm_value', 'calc_year', 'date_updated', 'data_set_id', 'jan_value', 'feb_value', 'mar_value', 'apr_value', 'may_value', 'jun_value', 'jul_value', 'aug_value', 'sep_value', 'oct_value', 'nov_value', 'dec_value', 'comment'],
    'FF10_HOURLY_POINT':['country_cd', 'region_cd', 'tribal_code', 'facility_id', 'unit_id', 'rel_point_id', 'process_id', 'scc', 'poll', 'op_type_cd', 'calc_method', 'date_updated', 'date', 'daytot', 'hrval0', 'hrval1', 'hrval2', 'hrval3', 'hrval4', 'hrval5', 'hrval6', 'hrval7', 'hrval8', 'hrval9', 'hrval10', 'hrval11', 'hrval12', 'hrval13', 'comment'],
    'FF10_NONROAD':['country_cd', 'region_cd', 'tribal_code', 'census_tract_cd', 'shape_id', 'scc', 'emis_type', 'poll', 'ann_value', 'ann_pct_red', 'control_ids', 'control_measures', 'current_cost', 'cumulative_cost', 'projection_factor', 'reg_codes', 'calc_method', 'calc_year', 'date_updated', 'data_set_id', 'jan_value', 'feb_value', 'mar_value', 'apr_value', 'may_value', 'jun_value', 'jul_value', 'aug_value', 'sep_value', 'oct_value', 'nov_value', 'dec_value','jan_pctred','feb_pctred','mar_pctred','apr_pctred','may_pctred','jun_pctred','jul_pctred','aug_pctred','sep_pctred','oct_pctred','nov_pctred','dec_pctred','comment'],
    'FF10_DAILY_POINT':['country_cd','region_cd','tribal_code','facility_id','unit_id','rel_point_id','process_id','scc','poll','op_type_cd','calc_method','date_updated','monthnum','monthtot','dayval1','dayval2','dayval3','dayval4','dayval5','dayval6','dayval7','dayval8','dayval9','dayval10','dayval11','dayval12','dayval13','dayval14','dayval15','dayval16','dayval17','dayval18','dayval19','dayval20','dayval21','dayval22','dayval23','dayval24','dayval25','dayval26','dayval27','dayval28','dayval29','dayval30','dayval31','comment'],
    'FF10_DAILY_NONPOINT':['country_cd','region_cd','tribal_code','census_tract','shape_id','tbd','emis_type','scc','poll','op_type_cd','calc_method','date_updated','monthnum','monthtot','dayval1','dayval2','dayval3','dayval4','dayval5','dayval6','dayval7','dayval8','dayval9','dayval10','dayval11','dayval12','dayval13','dayval14','dayval15','dayval16','dayval17','dayval18','dayval19','dayval20','dayval21','dayval22','dayval23','dayval24','dayval25','dayval26','dayval27','dayval28','dayval29','dayval30','dayval31','comment']
}

def split_csv_line(line):
    return next(csv.reader([line], skipinitialspace=False))

def get_ff10_type(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
    except UnicodeDecodeError:
        with open(path, 'r', encoding='latin-1') as f:
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
        with open(path, 'r', encoding='utf-8') as f:
            f.read(1024)
    except UnicodeDecodeError:
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
    with open(input_path, 'r', encoding=encoding) as fin, \
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
                    
                val = values[idx].strip('"')
                
                match = False
                if f['vals'] is not None:
                    # Generic string match
                    if val in f['vals']: match = True
                    # Special handling for FIPS (allow matching 5-digit region_cd with 6-digit fips if country is 0)
                    elif f['col_name'] == 'region_cd' and len(val) == 5:
                        if ('0' + val) in f['vals']: match = True
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
    """
    if not os.path.isfile(path):
        return {}
    
    data = {'fips': {}, 'states': {}, 'counties': {}}
    in_county = False
    
    # Example line: " AL           Autauga Co 0 1  1        CST ..."
    county_pattern = re.compile(r'^\s*([A-Z0-9]{2})\s+(.*?)\s+(\d+)\s+(\d+)\s+(\d+)\s+([A-Z]{3,4})')
    
    with open(path, 'r', encoding='latin-1') as f:
        for line in f:
            if '/COUNTY/' in line:
                in_county = True
                continue
            if line.startswith('/') and in_county:
                break
            if not in_county or line.startswith('#') or not line.strip():
                continue
            
            match = county_pattern.match(line)
            if match:
                st_abbr, name, country, st_fips, co_fips, tz = match.groups()
                fips6 = f"{int(country)}{int(st_fips):02d}{int(co_fips):03d}"
                name = name.strip()
                
                entry = {
                    'fips': fips6,
                    'state_abbr': st_abbr.strip(),
                    'name': name,
                    'timezone': tz
                }
                data['fips'][fips6] = entry
                
                # Group by state
                if st_abbr not in data['states']: data['states'][st_abbr] = []
                data['states'][st_abbr].append(fips6)
                
                # Map name to fips
                full_name = f"{st_abbr}:{name}".lower()
                data['counties'][full_name] = fips6

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
    active_filters = []
    
    # COSTCY Loading if needed (Only if using State or County Name filters)
    costcy_data = None
    use_states = config.get('filter_states') and len(config.get('filter_states')) > 0
    use_counties = config.get('filter_counties') and len(config.get('filter_counties')) > 0
    
    if (use_states or use_counties) and config.get('costcy_file'):
        print(f"Loading COSTCY from {config['costcy_file']}...")
        costcy_data = parse_costcy(config['costcy_file'])
    elif (use_states or use_counties) and not config.get('costcy_file'):
        print("Warning: State or County filtering requested but 'costcy_file' is missing.")

    # 1. Direct FIPS Filter
    fips_to_filter = []
    if config.get('filter_fips'):
        vals = config.get('filter_fips')
        if not isinstance(vals, list): vals = [vals]
        fips_to_filter.extend([str(v) for v in vals])

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
        active_filters.append({'col_name': 'region_cd', 'vals': set(counties_spatial), 'range': None, 'is_exclusion': is_exclusion})
        print(f"Added grid filter with {len(counties_spatial)} counties.")

    # 5. Shapefile Filter (GIS)
    if config.get('filter_shp') and os.path.isfile(config.get('filter_shp')):
        print(f"Loading input shapefile {config['filter_shp']}...")
        gis_gdf = gpd.read_file(config['filter_shp'])
        counties_spatial = get_intersecting_counties(gis_gdf, config['county_shp'])
        active_filters.append({'col_name': 'region_cd', 'vals': set(counties_spatial), 'range': None, 'is_exclusion': is_exclusion})
        print(f"Added shapefile filter with {len(counties_spatial)} counties.")
    elif config.get('filter_shp'):
        print(f"Warning: filter_shp is defined but file not found: {config.get('filter_shp')}")

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

        for fname in fnames:
            ff10_path = fname
            
            if not os.path.exists(ff10_path):
                warnings.warn(f"File not found: {ff10_path}. Skipping.")
                sector_success = False
                continue

            try:
                base, ext = os.path.splitext(os.path.basename(ff10_path))
                prepend = config.get('filename_prepend') or ""
                append = config.get('filename_append') or ""
                
                if prepend:
                    # User specified a prepend to replace the base name
                    out_name = f"{prepend}{append}{ext}"
                else:
                    # Use current auto logic (original base name)
                    out_name = f"{base}{append}{ext}"
                    
                outfile = os.path.join(sector_output, out_name)
                print(f"Processing {ff10_path} -> {outfile}")
                count = stream_extract(ff10_path, outfile, active_filters)
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
