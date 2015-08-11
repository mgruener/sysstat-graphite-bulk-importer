sysstat-graphite-bulk-importer
==============================

Synopsis:

```
sysstat-bulk-import.py <safile>
```

`<safile>` is a standard binary sysstat data file.

Requirements
------------

python, sysstat and carbon-cache need to be installed on the system running
the script.

Limitations
-----------

Currently the server name is hardcoded to localhost:2004 and the script has
only been tested with sar data files created with sysstat 11.0.1
