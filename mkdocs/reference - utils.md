title: Utility Reference

# Utility Reference

Most functions in `moodys_datahub.utils` are internal implementation details and
are not part of the stable public API.

If you need package functionality, prefer the high-level `Sftp` class first.
The documented standalone helper below is the main utility that is useful on its
own.

## Supported standalone helper

::: moodys_datahub.utils.fuzzy_query
    handler: python
    options:
        show_source: false
        show_root_heading: true
        heading_level: 2

## Internal utilities

The remaining functions in `moodys_datahub.utils` are primarily used by the
package internals for IO, filtering, multiprocessing, and file handling. They
may change without the same compatibility guarantees as `Sftp`.
