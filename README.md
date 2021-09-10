# antennapodRescue

This repository contains scripts and tools to repair issues in the database of the podcatcher app
[AntennaPod](https://github.com/AntennaPod/AntennaPod).

The underlying database file contains all information about feeds, episodes and media.
It can be exported in the app and afterwards be reimported.

**Disclaimer**: Absolutely no warranty can be given for problems arising from the usage of these scripts 
like accidental data loss or database corruption. 
Use the scripts if and only if you know what you're doing.

Please also read the docstring at the op of each script!

## Use cases
1. Consolidation of duplicate entries:

    Remove duplicate entries introduced by upstream changes of some values in the feed XML.
    Other values like title must still be identical to allow automatic merging.
2. Removal of episodes which are no longer in the XML file:

   Remove episodes, which are no longer distributed in the currently available XML file.
3. Removal of old unusually short episodes:

    Remove episodes which are no longer distributed in the XML file and have a duration significantly shorter than usual.

    


