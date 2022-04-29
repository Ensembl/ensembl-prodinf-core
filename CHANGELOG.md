Changelog
=========

2.0.4 - bug fixes
-----------------
- Better error display from failed handover client submission
- Better handling HTTRequestError
- Prevent HiveInstance from using nested sessions
- Update SQL Alchemy version and SQL Alchemy-utils

2.0.3 - Fixed perl String parsing
---------------------------------

- Fixed issues with hidden chars in perl strings

2.0.2 - Fixed Typo
------------------

- Fixed typo in `hive.py` module causing error in DC jobs status retrieval.

2.0.1 - Dependencies updates
----------------------------

- Requests version range instead of "Compatible" (>=2.4,<3)
- PyYAML version changed to comply with ensembl-py (PyYAML~=6.0)

2.0.0 - Change DB Introspect database a table list functions behaviour
----------------------------------------------------------------------

- Change `db_introspects` module
  - `get_database_set()` and `get_table_set()` filters "match" instead of "search"

1.4.0 - Ready to be published on PyPI
-----------------------------------
- Fix tests and run them with pytest
- Update some functions and methods
- Automate test & deploy with Travis
- Fix setup.py to be PyPI compliant

1.3.1 - Fix typo
----------------
- Fix typo in indentation for job_progress retrieval

1.3 - Update Hive to retrieve job status
----------------------------------------
- Hive retrieve job status for given job id
- Enable filtering of metadata job lists in metadata client

1.2 - Updated DatacheckClient submit payload
--------------------------------------------
- `DatacheckClient.submit_job` set `target_url` only if not `None`

1.1 - Updated clients uris scheme
---------------------------------
- Updated path for handover client to point to expected new uri scheme
- Added few FIXME for next version

1.0 - Initial package version
-----------------------------
- Retrieved shared library compatible code from old ensembl-srv and ensembl-core git hub legacy repository
- Packaging with setup.py to integrate with other apps

