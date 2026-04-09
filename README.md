# smkextract

This utility facilitates filtering and processing emission inventory files (FF10 format) from variable sources (e.g. EPA's Emission Modeling Platform) to match a target modeling grid, intersecting counties, specific column values, or geographic identifiers.

The toolkit comprises three component python scripts:
1. `build_emp_runscripts_yaml.py`: Scans EMP scripts folder to generate a manifest of all run scripts.
2. `build_sector_config.py`: Automates configuration setup by parsing EMP run scripts. **Maintains all comments and settings from `smkextract_template.yaml`.**
3. `smkextract.py`: The consolidated extractor tool that filters inventories based on Grid, Shapefile, FIPS/State/County, or Column values.

# Quick Start: 3-Step Workflow

**STEP 1: Generate emp_runscripts.yaml**

Use the helper script to scan your EMP scripts directory and build a manifest of all run scripts, grouped by sector:

```sh
./build_emp_runscripts_yaml.py --input <path/to/scripts> --output emp_runscripts.yaml
```
- This scans the directory for `.csh` scripts and groups them by sector name found within the files.
- Output: `emp_runscripts.yaml` listing all relevant scripts.

**STEP 2: Build/Update smkextract.yaml sector mapping**

Parse the run scripts and update the `sector` section of your main config:

```sh
./build_sector_config.py --runscripts emp_runscripts.yaml --config smkextract.yaml
```
- **Automatic Initialization**: If `smkextract.yaml` does not exist, it is created automatically using `smkextract_template.yaml` as a base.
- **Comment Preservation**: The script preserves all settings and descriptive comments in the header (everything before the `# --- Sectors & Files Mapping ---` marker).
- **Validation**: Each file path in the `sector:` section is tagged with `# Valid` or `# Missing` for easy verification.

**STEP 3: Run the extractor**

Filter and process emission inventories as needed:

```sh
./smkextract.py --config smkextract.yaml
```
- This applies your filters and outputs processed inventories.

---

# A. build_emp_runscripts_yaml.py

## Overview

`build_emp_runscripts_yaml.py` is a utility designed to scan a directory containing EMP (Emission Modeling Platform) run scripts (typically `.csh` files) and generate a structured YAML manifest (`emp_runscripts.yaml`). This manifest groups the scripts by their sector name, which is parsed directly from inside the `.csh` files.

This script automates **STEP 1** of the workflow, providing the necessary input for `build_sector_config.py`.

## Usage

```sh
./build_emp_runscripts_yaml.py --input <path/to/scripts_folder> [--output <path/to/manifest.yaml>]
```

- `--input`: **Required.** The path to the folder containing the EMP `.csh` run scripts.
- `--output`: Path where the generated manifest will be saved (default: `emp_runscripts.yaml`).

## Example

```sh
./build_emp_runscripts_yaml.py --input /proj/ie/proj/SMOKE/htran/Emission_Modeling_Platform/2022v2/2022he_cb6_22m/scripts
```

---

# B. build_sector_config.py

## Overview

`build_sector_config.py` is a utility script designed to automate the population of sector entries in the `smkextract.yaml` configuration file. It parses a set of EMP run scripts to extract sector names and emission inventory file references, then updates the configuration file accordingly. This helps streamline the setup and management of emission sector data.

It reads a manifest of runscripts (default: `emp_runscripts.yaml`) and updates the `sector` section of the target configuration (default: `smkextract.yaml`). It handles variable resolution (e.g., `${CASEINPUTS}`) to ensure absolute paths are captured.

## Usage

```sh
./build_sector_config.py [--runscripts <path/to/emp_runscripts.yaml>] [--config <path/to/smkextract.yaml>]
```

- `--runscripts`: Path to the manifest YAML file listing run scripts (default: `emp_runscripts.yaml`).
- `--config`: Path to the configuration YAML file to update (default: `smkextract.yaml`).

## Example

```sh
./build_sector_config.py
```

---

# C. smkextract.py

## Overview

`smkextract.py` is the consolidated utility for filtering emission inventory files (FF10 format). It replaces the legacy `smkxtgrid`, `smkxtgis`, and `smkxtcol` scripts. It supports multiple filtering methods simultaneously and allows for both **extraction** (keeping matches) and **exclusion** (removing matches).

## Features

- **Unified Configuration**: Uses `smkextract.yaml` for all settings.
- **Multiple Filter Types**:
    - **Grid:** Filter by intersection with a modeling grid (requires `GRIDDESC`).
    - **Shapefile (GIS):** Filter by intersection with a custom polygon (e.g., basin, region).
    - **FIPS/State/County:** Filter by 6-digit FIPS, State Abbreviation, or County Name (using `COSTCY` lookup).
    - **Column:** Filter by values or ranges in specific columns (e.g., `SCC`, `poll`).
- **Flexible Modes**: Global and per-filter supports `extraction` (keep) or `exclusion` (drop).
- **Format Support**: Automatically detects various FF10 header formats (Point, Nonpoint, Onroad, etc.).

## Usage

```sh
./smkextract.py --config <path/to/smkextract.yaml>
```

- `--config`: Path to the configuration YAML file (default: looks for `smkextract.yaml` in script dir).

## Configuration (`smkextract.yaml`)

The configuration file controls all aspects of the extraction. Key sections:

- **Path Settings**: `outputs` directory.
- **Process Control**: `filter_sector` (list or 'all'), `filter_mode` (extraction/exclusion).
- **Geographic Filters**: `filter_fips`, `filter_states`, `filter_counties` (requires `costcy_file`).
- **Spatial Filters**: `filter_grid` (requires `griddesc_path`, `county_shp`) or `filter_shp` (requires `county_shp`).
- **Column Filters**: `filter_cols` list.
- **Sectors**: Mapping of sector names to input files (populated by `build_sector_config.py` or manually).

Example snippet:
```yaml
filter_mode: extraction
filter_states: ['NC', 'VA']
filter_cols:
  - col_name: poll
    filtered_val: ["NOX"]
```

---

# D. Author
Huy Tran: tranhuy@email.unc.edu