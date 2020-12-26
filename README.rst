Open Academic Graph
===================

~ Day project for Secret Santa ~

This package makes a simple interface to a locally downloaded version of
the Open Academic Graph dataset.

(insert citation, licenses, etc to OAG here)

Example Usage:
```
import open_academic_graph as oag
db = oag.Database()

# Random sample of papers from the microsoft Academic Search Database
sample_papers_df = db.sample("mag_papers_0")

# Query the author table from the database
author_df = db.query("select * from mag_authors_0 limit 1000")
```
