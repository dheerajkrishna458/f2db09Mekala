import argparse
import re
from pathlib import Path

"""
Script to bump the version number in src/core_utils/_version.py.
Usage:
    python scripts/bump_version.py [major|minor|patch]
"""

def bump_version(version_part: str):
    """
    Reads the _version.py file, increments the specified version part,
    and writes it back.
    
    Args:
        version_part (str): One of "major", "minor", "patch".
    """
    
    version_file = Path("src/core_utils/_version.py")
    if not version_file.exists():
        # Fallback if running from root without src context known
        # Try finding it relative to script location
        version_file = Path(__file__).parent.parent / "src" / "core_utils" / "_version.py"
        
    content = version_file.read_text()
    
    # Extract current version
    match = re.search(r'__version__ = "(\d+)\.(\d+)\.(\d+)"', content)
    if not match:
        raise ValueError("Could not find version string in _version.py")
        
    major, minor, patch = map(int, match.groups())
    
    if version_part == "major":
        major += 1
        minor = 0
        patch = 0
    elif version_part == "minor":
        minor += 1
        patch = 0
    elif version_part == "patch":
        patch += 1
        
    new_version = f"{major}.{minor}.{patch}"
    print(f"Bumping version from {match.group(0)} to {new_version}")
    
    new_content = re.sub(r'__version__ = ".*"', f'__version__ = "{new_version}"', content)
    
    version_file.write_text(new_content)
    print(f"Successfully updated {version_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bump version number")
    parser.add_argument("part", choices=["major", "minor", "patch"], help="Version part to bump")
    args = parser.parse_args()
    
    bump_version(args.part)
