#!/usr/bin/env python3

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
        abs_p = os.path.abspath(os.path.expandvars(path))
        
        # Path re-anchoring: if file doesn't exist locally, try to find it under inputs_root
        if not os.path.exists(abs_p) and inputs_root and os.path.isdir(inputs_root):
            # Strategy 1: Match relative structure after 'inputs' folder
            parts = abs_p.split(os.sep)
            if 'inputs' in parts:
                idx = parts.index('inputs')
                rel_parts = parts[idx+1:]
                attempt = os.path.normpath(os.path.join(inputs_root, *rel_parts))
                if os.path.exists(attempt):
                    abs_p = attempt
            
            # Strategy 2: Deep search if still not found
            if not os.path.exists(abs_p):
                base = os.path.basename(abs_p)
                for root, _, files in os.walk(inputs_root):
                    if base in files:
                        abs_p = os.path.join(root, base)
                        break
        
        if abs_p not in unique_abs_paths:
            unique_abs_paths.append(abs_p)
            
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
            
            # Dump the sector dictionary and add inline comments
            sector_yaml = yaml.dump({'sector': sector_mapping}, default_flow_style=False, sort_keys=False)
            yaml_lines = sector_yaml.splitlines()
            final_yaml_lines = []
            for line in yaml_lines:
                sline = line.strip()
                # If it looks like a list item with a path
                if sline.startswith('- ') and ('/' in sline or sline.endswith('.csv')):
                    path = sline[2:].strip().strip("'").strip('"')
                    if os.path.exists(path):
                        line += " # Valid"
                    else:
                        line += " # Missing"
                final_yaml_lines.append(line)
            f.write("\n".join(final_yaml_lines) + "\n")
    else:
        # Fallback to full dump if marker is missing
        with open(config_path, 'w') as f:
            content = yaml.dump(data, default_flow_style=False, sort_keys=False)
            yaml_lines = content.splitlines()
            final_yaml_lines = []
            for line in yaml_lines:
                sline = line.strip()
                if sline.startswith('- ') and ('/' in sline or sline.endswith('.csv')):
                    path = sline[2:].strip().strip("'").strip('"')
                    if os.path.exists(path):
                        line += " # Valid"
                    else:
                        line += " # Missing"
                final_yaml_lines.append(line)
            f.write("\n".join(final_yaml_lines) + "\n")

def load_runscripts(yaml_path):
    if not os.path.isfile(yaml_path):
        raise FileNotFoundError(f"Runscript manifest not found: {yaml_path}")
    with open(yaml_path, 'r') as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise TypeError("Runscript manifest must be a YAML object mapping groups to lists of scripts.")
    return data

def get_script_vars(script_path, initial_vars=None):
    """
    Parses a SMOKE .csh script to extract setenv variables.
    """
    script_vars = dict(initial_vars) if initial_vars else {}
    if not os.path.isfile(script_path):
        return script_vars
        
    with open(script_path, 'r') as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith('#'):
                continue
            match = SETENV_PATTERN.match(line)
            if match:
                key, value = match.groups()
                key = key.strip()
                value = value.strip()
                
                resolved_value = value
                for _ in range(3):
                    start_val = resolved_value
                    for v_name, v_val in script_vars.items():
                        resolved_value = resolved_value.replace(f"${v_name}", str(v_val)).replace(f"${{{v_name}}}", str(v_val))
                    if start_val == resolved_value:
                        break
                script_vars[key] = resolved_value
    return script_vars

def main():
    parser = argparse.ArgumentParser(description='Populate smkextract.yaml sector entries from run scripts.')
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.join(script_dir, 'smkextract.yaml')
    default_runscripts = os.path.join(script_dir, 'emp_runscripts.yaml')
    
    parser.add_argument('--runscripts', default=default_runscripts, help='Path to emp_runscripts.yaml manifest.')
    parser.add_argument('--config', default=default_config, help='Path to smkextract.yaml configuration file.')
    args = parser.parse_args()
    
    try:
        manifest = load_runscripts(args.runscripts)
        manifest_dir = os.path.dirname(os.path.abspath(args.runscripts))

        # 1. Get initial inputs_root from config
        inputs_root = None
        if os.path.exists(args.config):
            with open(args.config, 'r') as f:
                temp_config = yaml.safe_load(f) or {}
            inputs_root = temp_config.get('inputs')

        # 2. Check for directory_definitions in manifest
        global_vars = {}
        dir_defs_path = manifest.get('directory_definitions')
        if dir_defs_path:
            if not os.path.isabs(dir_defs_path):
                dir_defs_path = os.path.normpath(os.path.join(manifest_dir, dir_defs_path))
            print(f"Loading directory definitions from {dir_defs_path}")
            global_vars = get_script_vars(dir_defs_path)
            
            # If CASEINPUTS is found in directory_definitions, it overrides config
            if 'CASEINPUTS' in global_vars:
                inputs_root = global_vars['CASEINPUTS']
            elif 'CASEINPUT' in global_vars:
                inputs_root = global_vars['CASEINPUT']
        
        if inputs_root:
            print(f"Using inputs_root: {inputs_root}")

        updates = 0
        for group, scripts in manifest.items():
            if group == 'directory_definitions': continue
            if not isinstance(scripts, list): continue

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
