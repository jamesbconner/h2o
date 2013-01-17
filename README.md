h2o
=======

h2o = fast analytics at scale
Fast Analytics Engine over HDFS

Product Vision for first cut:
-----------------------------
- Our product (Analytics Engine) capabilities are *only* limited
 - Upto 6 stat functions we write namely, RandomForest, SVM, LR, COV, logistic regression, k-Means, available over REST-API that will scale on 10's-of-nodes on 100GB-1TB of data.
 - Source Data is immutable.
- We read and write from/to HDFS (akin to the Spark project.)
- While we will cache. We won't be responsible for any storage at all. No fault tolerance -
    Other than to snapshotting a system wide state for restart. (And that too is not critical in our initial cut.)

Users
--------
- Simplify our problem drastically by defining our first end users -
  - Just Microsoft Excel Using Data Scientists/Statisticians. Web search bar-like/completion of statistics functions.
             Connect via Excel using REST.
  - Advanced statisticians will use R within R IDE environments. (v1.1) via a cran download.
  - They will be no java programmers other than ourselves.
- Summary: Vast majority of the end users will be accessing us via a 'search-like bar' - more like-an excel bar with completion for specific functions.

Design
--------
- We use simple execution frameworks for fast analytics performance - tailored for the Algorithm at hand.
- Data Structures and Data Partitioning are key aspects.
 
Extensions
----------
- As an extension, our first use case will be a small tool belt of stats such as for Fraud Detection.
  It is an example of our first Analytics Application. (Think, StoredProcedure/DSL/Reusable App.)
- In v1.2, A small advanced users will use our REST-api via, R through a specialized Data.frame (distributed via cran) that connects our Analytics Engine into  R-IDE/RStudio.

Community & Marketing
----------------------
- We will make our simplified vision of Mining BigData public & immediate along the way to our product readiness.
     (& foster a clean image with bios of every contributor)

We will breathe & sustain a vibrant community with the sharp focus of scaling datascience.

Team
--------
```
SriSatish Ambati
Cliff Click
Jan Vitek
Petr Maj
Kevin Normoyle
Tomas Nykodym
Matt Fowles
Cyprien Noel
Michal Malohlava
Lauren Brems
```
