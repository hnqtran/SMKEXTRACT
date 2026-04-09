#!/proj/ie/proj/SMOKE/htran/Emission_Modeling_Platform/utils/smkextract/.venv/bin/python
"""
Auto-generate emp_runscripts.yaml from EMP script folder structure.
Usage:
  python build_emp_runscripts_yaml.py [config.yaml]
If no config is given, looks for build_emp_runscripts_config.yaml in the script directory.
"""
import os
import sys
import yaml

def load_config(config_path):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def find_scripts(scripts_root, include_groups=None, exclude_groups=None):
    result = {}
    directory_def = None
    for root, dirs, files in os.walk(scripts_root):
        rel = os.path.relpath(root, scripts_root)
        if rel == '.':
            # Look for directory_definitions.csh at top level
            for f in files:
                if f == 'directory_definitions.csh':
                    directory_def = os.path.join(root, f)
            continue
        group = rel.split(os.sep)[0]
        if include_groups and group not in include_groups:
            continue
        if exclude_groups and group in exclude_groups:
            continue
        for f in files:
            if f.endswith('.csh'):
                result.setdefault(group, []).append(os.path.join(root, f))
    return directory_def, result

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(script_dir, 'build_emp_runscripts_config.yaml')
    config = load_config(config_path)
    scripts_root = config['scripts_root']
    output_yaml = config.get('output_yaml') or os.path.join(script_dir, 'emp_runscripts.yaml')
    include_groups = config.get('include_groups')
    exclude_groups = config.get('exclude_groups')

    directory_def, group_map = find_scripts(scripts_root, include_groups, exclude_groups)
    # Sort scripts in each group
    for group in group_map:
        group_map[group] = sorted(group_map[group])
    # Sort groups
    group_map = dict(sorted(group_map.items()))
    out = {}
    if directory_def:
        out['directory_definitions'] = os.path.abspath(directory_def)
    out.update(group_map)
    with open(output_yaml, 'w') as f:
        yaml.dump(out, f, default_flow_style=False, sort_keys=False)
    print(f"Wrote {output_yaml} with {len(group_map)} groups.")

if __name__ == '__main__':
    main()
