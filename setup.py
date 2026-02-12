#!/usr/bin/env python3
"""
Setup script for Research Loom
A parsimonious research pipeline framework.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read version from __init__.py
version = "1.0.0"
init_file = Path(__file__).parent / "__init__.py"
if init_file.exists():
    for line in init_file.read_text().splitlines():
        if line.startswith("__version__"):
            version = line.split("=")[1].strip().strip('"').strip("'")
            break

# Read README for long description
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text() if readme_file.exists() else ""

setup(
    name="research-loom",
    version=version,
    description="A parsimonious research pipeline framework with caching, artifact management, and export capabilities",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Research Team",
    author_email="research@example.com",
    url="https://github.com/yourusername/research_loom",
    package_dir={"research_loom": "."},
    packages=["research_loom", "research_loom.pipeline"],
    python_requires=">=3.8",
    install_requires=[
        "pyyaml>=6.0",
    ],
    extras_require={
        "data": [
            "pandas>=1.5.0",
            "pyarrow>=10.0.0",
            "zstandard>=0.19.0",
        ],
        "viz": [
            "matplotlib>=3.5.0",
            "plotly>=5.0.0",
        ],
        "dev": [
            "pytest>=7.0.0",
            "black>=22.0.0",
            "mypy>=0.991",
        ],
        "all": [
            "pandas>=1.5.0",
            "pyarrow>=10.0.0",
            "zstandard>=0.19.0",
            "matplotlib>=3.5.0",
            "plotly>=5.0.0",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering",
    ],
    entry_points={
        "console_scripts": [
            "research-loom=research_loom.cli:main",
        ],
    },
)

