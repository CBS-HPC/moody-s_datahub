# Moody's Datahub

## Introduction

This page introduces the "moodys_datahub" Python package, designed to facilitate access to Moody's Datahub "Data Products" exported via SFTP.

The package offers a suite of functions for selecting, downloading, and curating "Data Products" in a highly parallelized manner, optimizing the use of compute power available on cloud or HPC systems.

## SFTP server Access

To CBS associates should contact CBS staff to provide a personal "privatkey" (.pem) that is used to estabilish connection to the available "CBS server".

To access other SFTP servers the user also needs additional information regarding "hostname" and "username".

When connecting to an SFTP server, the package detects all export folders and attempts to match them to specific "Data Products" based on their "Table" titles. However, some "Data Products" have identical table names, making automatic differentiation impossible. In such cases, the user will be prompted to manually match the export folder with the correct "Data Product."

## Format recommandation

Moody's Datahub provides several export formats (".csv", ".parquet", ".orc", and ".avro"). The moodys_datahub Python package supports all these formats, but the preferred format is ".parquet" due to its high compression, columnar storage format, and the fact that each table is partitioned into many small files, offering significant performance benefits.

Using .csv is not recommended because of its lack of compression and partitioning, which results in some tables being single files exceeding 300 GB in size.

## System Reccomendation

Given the large size of most data tables, it is highly recommended that users utilize more powerful machines available through cloud or HPC systems. Based on user experience, a "rule of thumb" has been established: each "worker" (which processes one "subfile" at a time) should have approximately 12 GB of memory available.

For CBS Associates, it is highly recommended to use UCloud and run the application on a "u1-standard" machine with 64 cores, 384 GB of RAM, and a high-speed internet connection.

## Getting Started

Below you will find links to a "How to Get Started" tutorial (which can also be downloaded as a Jupyter notebook), the moodys_datahub Git repository, and the "API Reference" page.

    [How to get started](https://cbs-hpc.github.io/moody-s_datahub/mkdocs/how_to_get_started/)

    [Git Repository: moody-s_datahub](https://github.com/CBS-HPC/moody-s_datahub)

    [API Reference](https://cbs-hpc.github.io/moody-s_datahub/mkdocs/reference/)

