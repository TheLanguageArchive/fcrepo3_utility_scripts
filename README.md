# fcrepo3_utility_scripts
Python scripts for manipulating Fedora Commons 3 content via its REST API

## purge_ds_versions.py
Script for deleting older versions of specified datastream IDs from Fedora Commons 3.x

Usage:\
```purge_ds_versions.py -r islandora:root -d POLICY,RELS-EXT -k 2```\
keep the 2 most recent versions of the POLICY and RELS-EXT datastreams for all objects in the islandora:root collection