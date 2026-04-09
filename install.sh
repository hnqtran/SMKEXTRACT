#!/bin/bash

# Define variables
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
VENV_PATH="$DIR/.venv"
PYTHON_EXEC="$VENV_PATH/bin/python"

echo "========================================================"
echo "      Setting up smkextract Environment"
echo "========================================================"

# 1. Create Virtual Environment
if [ -d "$VENV_PATH" ]; then
    echo "Virtual environment already exists at $VENV_PATH"
else
    echo "Creating virtual environment at $VENV_PATH..."
    python3 -m venv "$VENV_PATH"
fi

# 2. Install Dependencies
echo "Installing dependencies..."
# Upgrade pip first
"$VENV_PATH/bin/pip" install --upgrade pip
# Install required packages
"$VENV_PATH/bin/pip" install geopandas shapely pyproj pandas pyyaml

# 3. Update Shebangs and Permissions
echo "Configuring scripts..."

SCRIPTS=("smkextract.py" "build_sector_config.py" "build_emp_runscripts_yaml.py")

for script in "${SCRIPTS[@]}"; do
    SCRIPT_PATH="$DIR/$script"
    if [ -f "$SCRIPT_PATH" ]; then
        echo "Processing $script..."
        
        # Determine if we need to replace or insert
        # Read the first line
        FIRST_LINE=$(head -n 1 "$SCRIPT_PATH")
        
        if [[ "$FIRST_LINE" == \#!* ]]; then
            # Replace existing shebang
            # Generate temp file skipping first line
            tail -n +2 "$SCRIPT_PATH" > "$SCRIPT_PATH.tmp"
            # Prepend new shebang
            echo "#!$PYTHON_EXEC" | cat - "$SCRIPT_PATH.tmp" > "$SCRIPT_PATH"
            rm "$SCRIPT_PATH.tmp"
        else
            # Insert shebang at top
            echo "#!$PYTHON_EXEC" | cat - "$SCRIPT_PATH" > "$SCRIPT_PATH.tmp"
            mv "$SCRIPT_PATH.tmp" "$SCRIPT_PATH"
        fi
        
        # Make executable
        chmod +x "$SCRIPT_PATH"
        echo "  - Updated shebang"
        echo "  - set +x permission"
    else
        echo "Warning: $script not found in $DIR"
    fi
done

echo "========================================================"
echo "      Installation Complete!"
echo "========================================================"
echo "You can now run the scripts directly:"
echo "  ./smkextract.py --help"
echo "  ./build_sector_config.py --help"
