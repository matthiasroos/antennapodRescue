# antennapodRescue

This repository contains scripts and tools to repair issues in the database of the podcatcher app
[AntennaPod](https://github.com/AntennaPod/AntennaPod).

The underlying database file contains all information about feeds, episodes and media.
It can be exported in the app and afterwards be reimported.

**Disclaimer**: I give absolutely no warranty for problems arising from the usage of these scripts 
like accidental data loss or database corruption. 
Only use the scripts if you know what you're doing. 

## Use cases
1. Consolidation of duplicate entries:

    Remove duplicate entries introduced by upstream changes of some values in the feed XML.
    Other values like title must still be identical to allow automatic merging.


