#!/nas/longleaf/home/tranhuy/software/pkg/miniconda3/envs/AERMOD/bin/python

import argparse
import os
import re
import sys
import yaml

SETENV_PATTERN = re.compile(r'^setenv\s+(\S+)\s+"([^"]*)"')

def parse_run_script(script_path, inputs_root=None):
    """
    Parses a SMOKE .csh run script to find the SECTOR and its input files.
    Tries to resolve environment variables, particularly ${CASEINPUTS}.
    """
    if not os.path.isfile(script_path):
        raise FileNotFoundError(f"Run script not found: {script_path}")
    
    sector = None
    inventory_paths = []
    
    # Local variable tracking for resolution
    script_vars = {}
    if inputs_root:
        script_vars['CASEINPUTS'] = inputs_root
        script_vars['CASEINPUT'] = inputs_root

    with open(script_path, 'r') as script_file:
        for raw_line in script_file:
            line = raw_line.strip()
            # Skip comments and empty lines, but only if they are at start of line
            # Keep setenv lines that might be commented out but relevant if needed? 
            # No, stick to active setenvs.
            if not line or line.startswith('#'):
                continue
                
            match = SETENV_PATTERN.match(line)
            if not match:
                continue
                
            key, value = match.groups()
            key = key.strip()
            value = value.strip()
            
            # Resolve variables in the value
            # We'll do a simple iteration to handle nested vars if any
            resolved_value = value
            for _ in range(3): # Max depth of 3 for resolution
                start_val = resolved_value
                for v_name, v_val in script_vars.items():
                    resolved_value = resolved_value.replace(f"${v_name}", v_val).replace(f"${{{v_name}}}", v_val)
                if start_val == resolved_value:
                    break
            
            script_vars[key] = resolved_value
            
            if key == 'SECTOR':
                sector = resolved_value
            elif key.startswith('EMISINV_') or key.startswith('EMISDAY_') or key.startswith('EMISHOUR_'):
                if resolved_value:
                    inventory_paths.append(resolved_value)
                
    if sector is None:
        return None, []
    
    # Ensure absolute paths and uniqueness
    unique_abs_paths = []
    for path in inventory_paths:
        # If it's still got unresolved variables (like $INSTALL_DIR), we can't make it absolute.
        # But we'll try to find it relative to current dir as a fallback.
        if '$' in path:
            # Try to expand what remains from the OS environment
            path = os.path.expandvars(path)
            
        abs_path = os.path.abspath(path)
        if abs_path not in unique_abs_paths:
            unique_abs_paths.append(abs_path)
            
    return sector, unique_abs_paths

def update_config(config_path, sector, filenames):
    """
    Updates the sector section of smkextract.yaml.
    Recognizes the '# --- Sectors & Files Mapping ---' marker.
    """
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        lines = f.readlines()
        content = "".join(lines)
    
    marker = "# --- Sectors & Files Mapping ---"
    marker_line_idx = -1
    for i, line in enumerate(lines):
        if marker in line:
            marker_line_idx = i
            break
            
    # Load current data for merging
    data = yaml.safe_load(content) or {}
    sector_mapping = data.get('sector', {})
    if not isinstance(sector_mapping, dict):
        sector_mapping = {}
        
    existing_list = sector_mapping.get(sector, [])
    if not isinstance(existing_list, list):
        existing_list = []
        
    # Merging logic: keep manual entries, but prioritize absolute paths from script
    # If a basename exists, replace it with the absolute path from the script.
    merged = list(existing_list)
    for new_path in filenames:
        found = False
        new_base = os.path.basename(new_path)
        for i, existing in enumerate(merged):
            if existing == new_path or os.path.basename(existing) == new_base:
                merged[i] = new_path # Update to absolute
                found = True
                break
        if not found:
            merged.append(new_path)
            
    sector_mapping[sector] = merged
    data['sector'] = sector_mapping

    if marker_line_idx != -1:
        # Rewrite preserving order and comments before marker
        with open(config_path, 'w') as f:
            for i in range(marker_line_idx + 1):
                f.write(lines[i])
            
            # Dump the sector dictionary
            # PyYAML's dump of a dict includes the key.
            # We want to format it nicely.
            sector_yaml = yaml.dump({'sector': sector_mapping}, default_flow_style=False, sort_keys=False)
            f.write(sector_yaml)
    else:
        # Fallback to full dump if marker is missing
        with open(config_path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
            f.write('\n')

def load_runscripts(yaml_path):
    if not os.path.isfile(yaml_path):
        raise FileNotFoundError(f"Runscript manifest not found: {yaml_path}")
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise TypeError("Runscript manifest must be a YAML object mapping groups to lists of scripts.")
    return data

def main():
    parser = argparse.ArgumentParser(description='Populate smkextract.yaml sector entries from run scripts.')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.join(script_dir, 'smkextract.yaml')
    default_runscripts = os.path.join(script_dir, 'emp_runscripts.yaml')
    
    parser.add_argument('--runscripts', default=default_runscripts, help='Path to emp_runscripts.yaml manifest.')
    parser.add_argument('--config', default=default_config, help='Path to smkextract.yaml configuration file.')
    args = parser.parse_args()
    
    try:
        # Get inputs_root from config for variable resolution
        with open(args.config, 'r') as f:
            temp_config = yaml.safe_load(f) or {}
        inputs_root = temp_config.get('inputs')

        manifest = load_runscripts(args.runscripts)
        updates = 0
        manifest_dir = os.path.dirname(os.path.abspath(args.runscripts))
        
        for group, scripts in manifest.items():
            for script_path in scripts:
                if not os.path.isabs(script_path):
                    script_path = os.path.normpath(os.path.join(manifest_dir, script_path))
                
                if not script_path.endswith('.csh'):
                    continue

                try:
                    sector, filenames = parse_run_script(script_path, inputs_root=inputs_root)
                    if sector and filenames:
                        update_config(args.config, sector, filenames)
                        print(f"Updated sector '{sector}' from {os.path.basename(script_path)}")
                        updates += 1
                except Exception as exc:
                    print(f"Error processing script {script_path}: {exc}", file=sys.stderr)
                    sys.exit(1)
                    
    except Exception as exc:
        print(f"General Error: {exc}", file=sys.stderr)
        sys.exit(1)
        
    print(f"Completed updates for {updates} run script(s). Configuration saved to {args.config}.")

if __name__ == '__main__':
    main()
