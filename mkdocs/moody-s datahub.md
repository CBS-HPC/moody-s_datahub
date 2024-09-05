title: Moody's Datahub
# Moody's Datahub

## Introduction

This page introduces the moodys_datahub Python package, designed to facilitate access to Moody's Datahub "Data Products" exported via SFTP. 

The package offers a suite of functions for selecting, downloading, and curating "Data Products" in a highly parallelized manner, optimizing the use of compute power available on cloud or HPC systems.

 
## Access

To CBS associates should contact [CBS staff]() to provide a personal "privatkey" (.pem) that is used to estabilish connection to the available "CBS server".


## System Reccomendation

Given the large size of most data tables, it is highly recommended that users utilize more powerful machines available through cloud or HPC systems. Based on user experience, a "rule of thumb" has been established: each "worker" (which processes one "subfile" at a time) should have approximately 12 GB of memory available.

For CBS Associates, it is highly recommended to use [UCloud](https://cbs-hpc.github.io/HPC_Facilities/UCloud/) and run the application on a u1-standard machine with 64 cores, 384 GB of RAM, and a high-speed internet connection.


## Links
- [How to get started](/moody-s_datahub/mkdocs/how_to_get_started/)

- [Reference](/moody-s_datahub/mkdocs/reference/)
