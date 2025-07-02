"""
Setup script for MQTT-Redis Data Ingestion System
"""

from setuptools import setup, find_packages
import pathlib

# Read the README file
HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setup(
    name="mqtt-redis-ingestion",
    version="1.0.0",
    description="A modular Python application for MQTT-Redis data ingestion",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/yourusername/mqtt-redis-ingestion",
    license="MIT",
    
    # Package configuration
    packages=find_packages(),
    include_package_data=True,
    
    # Python version requirement
    python_requires=">=3.7",
    
    # Dependencies
    install_requires=[
        "paho-mqtt>=1.6.1,<2.0.0",
        "redis>=4.5.0,<6.0.0",
        "typing-extensions>=4.0.0; python_version < '3.8'",
    ],
    
    # Optional dependencies
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-mock>=3.10.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
        ],
        "enhanced": [
            "hiredis>=2.0.0",
            "colorlog>=6.7.0",
        ],
        "docs": [
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.2.0",
        ]
    },
    
    # Entry points
    entry_points={
        "console_scripts": [
            "mqtt-redis-ingestion=main:main",
        ],
    },
    
    # Classifiers
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: System :: Monitoring",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    
    # Keywords
    keywords="mqtt redis iot data-ingestion messaging",
    
    # Project URLs
    project_urls={
        "Bug Reports": "https://github.com/yourusername/mqtt-redis-ingestion/issues",
        "Source": "https://github.com/yourusername/mqtt-redis-ingestion",
        "Documentation": "https://mqtt-redis-ingestion.readthedocs.io/",
    },
)