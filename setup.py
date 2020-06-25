#!/usr/bin/env python
from setuptools import setup

setup(
    name="target_gcs",
    version="0.1.0",
    description="Read stdin and write out to Google Cloud Storage",
    author="Daigo Tanaka",
    url="http://www.anelen.co",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_gcs"],
    install_requires=[
        "setuptools>=40.3.0",
        "singer-python>=5.0.12",
        "google-cloud>=0.34.0",
        "google-cloud-storage>=1.29.0",
        "google-resumable-media>=0.5.1",
        "oauth2client>=4.1.3",
        "google-api-python-client>=1.9.3"
    ],
    dependency_links=[
    ],
    entry_points="""
    [console_scripts]
    target_gcs=target_gcs:main
    """,
    packages=["target_gcs"],
    package_data = {},
    include_package_data=True,
)
