# smkextract

This utility facilitates filtering and processing emission inventory files (FF10 format) from variable sources (e.g. EPA's Emission Modeling Platform) to match a target modeling grid, intersecting counties, specific column values, or geographic identifiers.

The toolkit comprises two component python scripts:
1. `build_sector_config.py`: Automates configuration setup by parsing EMP run scripts.
2. `smkextract.py`: The consolidated extractor tool that filters inventories based on Grid, Shapefile, FIPS/State/County, or Column values.

# Setup Instructions

These scripts require Python 3 and several libraries. An installation script is provided to set up a local virtual environment and configure the scripts for execution.

## Installation

Run the provided `install.sh` script. This will:
1. Create a local python virtual environment (`.venv`) in the script directory.
2. Install necessary dependencies (`geopandas`, `shapely`, `pyproj`, `pandas`, `pyyaml`).
3. Update the python scripts (`smkextract.py`, etc.) to use this environment directly.
4. Make the scripts executable.

```sh
./install.sh
```

## Usage

After installation, you can run the tool components directly from the command line:

```sh
./smkextract.py --help
./build_sector_config.py --help
```

---

# A. build_sector_config.py

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

# B. smkextract.py

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

# C. Author
Huy Tran: tranhuy@email.unc.edu