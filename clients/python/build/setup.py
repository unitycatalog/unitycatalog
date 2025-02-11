# coding: utf-8

"""
    Unity Catalog API

    Official Python SDK for Unity Catalog
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the long description from README.md
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="unitycatalog-client",
    version="0.3.0.dev0",
    description="Official Python SDK for Unity Catalog",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Unity Catalog Developers",
    license="Apache-2.0",
    python_requires=">=3.9",
    install_requires=[
        "urllib3>=1.25.3",
        "aiohttp>=3.8.4",
        "aiohttp-retry>=2.8.3",
        "python-dateutil>=2.8.2",
        "typing-extensions>=4.7.1",
        "pydantic<3,>=2",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: End Users/Desktop",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Other Audience",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Software Development :: User Interfaces",
    ],
    keywords=["unitycatalog", "sdk"],
    url="https://www.unitycatalog.io/",
    project_urls={
        "Homepage": "https://www.unitycatalog.io/",
        "Issues": "https://github.com/unitycatalog/unitycatalog/issues",
        "Repository": "https://github.com/unitycatalog/unitycatalog",
    },
    packages=find_packages(include=["unitycatalog.client"], exclude=["test", "tests"]),
    include_package_data=True,
    package_data={
        "unitycatalog.client": ["py.typed"],
    },
    extras_require={
        "test": [
            "pytest>=7.2.1",
            "pytest-asyncio>=0.24.0",
            "tox>=3.9.0",
            "flake8>=4.0.0",
            "types-python-dateutil>=2.8.19.14",
            "mypy>=1.4.1",
        ],
    },
)
