#!/usr/bin/env python
"""
Setup script for the ETL Framework.

This script handles package installation, dependencies, and entry points.
"""

from setuptools import setup, find_packages
from pathlib import Path
import re

# Read version from __init__.py or set default
def get_version():
    """Extract version from package"""
    try:
        init_file = Path(__file__).parent / "src" / "__init__.py"
        if init_file.exists():
            content = init_file.read_text()
            match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
            if match:
                return match.group(1)
    except Exception:
        pass
    return "1.0.0"

# Read README for long description
def get_long_description():
    """Read README file"""
    readme_file = Path(__file__).parent / "README.md"
    if readme_file.exists():
        return readme_file.read_text()
    return ""

# Core dependencies
INSTALL_REQUIRES = [
    "pyyaml>=6.0",
    "python-dateutil>=2.8.0",
    "pytz>=2023.0",
]

# Optional dependencies
EXTRAS_REQUIRE = {
    # Database drivers
    "postgresql": ["psycopg2-binary>=2.9.0"],
    "mysql": ["pymysql>=1.0.0"],
    "sqlite": [],  # Built-in

    # Data processing
    "pandas": ["pandas>=1.5.0"],
    "polars": ["polars>=0.15.0"],

    # Web APIs
    "flask": ["flask>=2.3.0", "flask-cors>=4.0.0"],
    "fastapi": ["fastapi>=0.95.0", "uvicorn>=0.21.0", "pydantic>=1.10.0"],

    # Security
    "security": ["cryptography>=41.0.0", "bcrypt>=4.0.0"],

    # Monitoring
    "monitoring": ["psutil>=5.9.0", "prometheus-client>=0.16.0"],

    # Cloud integrations
    "aws": ["boto3>=1.28.0"],
    "azure": ["azure-identity>=1.12.0", "azure-storage-blob>=12.16.0"],
    "gcp": ["google-cloud-storage>=2.8.0", "google-auth>=2.17.0"],

    # Message queues
    "kafka": ["kafka-python>=2.0.0"],
    "rabbitmq": ["pika>=1.3.0"],

    # All optional dependencies
    "all": [
        # Database
        "psycopg2-binary>=2.9.0",
        "pymysql>=1.0.0",
        # Data processing
        "pandas>=1.5.0",
        "polars>=0.15.0",
        # Web APIs
        "flask>=2.3.0",
        "flask-cors>=4.0.0",
        "fastapi>=0.95.0",
        "uvicorn>=0.21.0",
        "pydantic>=1.10.0",
        # Security
        "cryptography>=41.0.0",
        "bcrypt>=4.0.0",
        # Monitoring
        "psutil>=5.9.0",
        "prometheus-client>=0.16.0",
        # Cloud
        "boto3>=1.28.0",
        "azure-identity>=1.12.0",
        "azure-storage-blob>=12.16.0",
        "google-cloud-storage>=2.8.0",
        "google-auth>=2.17.0",
        # Message queues
        "kafka-python>=2.0.0",
        "pika>=1.3.0",
    ]
}

setup(
    name="metadata-etl-framework",
    version=get_version(),
    description="Production-ready metadata-driven ETL framework with comprehensive monitoring and error recovery",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="ETL Framework Team",
    author_email="etl-framework@example.com",
    url="https://github.com/your-org/metadata-etl-framework",
    license="MIT",

    # Package configuration
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,

    # Dependencies
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,

    # Entry points for command-line tools
    entry_points={
        "console_scripts": [
            "etl=src.api.cli:main",
            "etl-api=src.api.rest_api:main",
        ],
    },

    # Classifiers for PyPI
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
    ],

    # Python version requirements
    python_requires=">=3.8",

    # Project keywords
    keywords=[
        "etl", "data-pipeline", "data-engineering", "metadata-driven",
        "data-quality", "monitoring", "error-recovery", "orchestration"
    ],

    # Project structure
    project_urls={
        "Documentation": "https://github.com/your-org/metadata-etl-framework/docs",
        "Source": "https://github.com/your-org/metadata-etl-framework",
        "Tracker": "https://github.com/your-org/metadata-etl-framework/issues",
    },

    # Additional files to include
    package_data={
        "": [
            "config/*.yaml",
            "database/*.sql",
            "docs/*.md",
            "examples/*.py",
            "docker/*",
        ],
    },

    # Data files outside packages
    data_files=[
        ("config", ["config/config.yaml"]),
        ("database", [
            "database/metadata_schema.sql",
            "database/execution_schema.sql",
            "database/data_quality_schema.sql",
            "database/security_schema.sql",
            "database/incremental_schema.sql",
            "database/error_recovery_schema.sql",
        ]),
        ("docs", [
            "docs/security_manager.md",
            "docs/incremental_load_manager.md",
            "docs/error_recovery_manager.md",
        ]),
        ("examples", [
            "examples/security_demo.py",
            "examples/incremental_demo.py",
            "examples/error_recovery_demo.py",
            "examples/etl_demo.py",
        ]),
    ],

    # Zip safe (can be installed as a zip file)
    zip_safe=False,

    # Testing
    test_suite="tests",

    # Command options
    options={
        "bdist_wheel": {
            "universal": False,
        },
    },
)