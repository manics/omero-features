OMERO.features
==============

Documentation and code for investigating potential feature storage solutions for OMERO.

Version 1
---------

This is the first version of an API for storing and retrieving features in OMERO.
See examples.py for an example of how to use this.

The aim was to create a basic functional client-side library usable by external developers without requiring any changes to OMERO.

Feedback from this initial implementation will be used to decide how to support the additional metadata associated with the calculation of features, to work out a good system for querying/retrieving features, and to scope out changes to the core OMERO components needed to support big and potentially unstructured feature sets.

Main limitations
----------------

* Features are stored in a single DoubleArrayColumn due to limitations on the number of scalar columns that can exist in a table.
* The combined length of feature names is currently limited to just under 64K bytes (using standard PyTables settings).
* Image-ID and Roi-ID are the only row metadata supported at present, so for example version information or other labels cannot be stored inside the table.
* The use of ROIs to describe a single plane instead of an explicit Z/C/T index can be inconvenient.
* Each feature store is designed to be used by a single user and group, though it is possible to read other user's features by passing additional parameters.

Additional information
----------------------

See the wiki, contributions from everyone are welcome: https://github.com/ome/omero-features/wiki
