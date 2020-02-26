timeseriesdb is going on a short hiatus to allow other projects their time
in the sun.

## Rights
* identify which functions need which rights
* should metadata also be under RLS?
* consider moving access column to catalog
  * ensures consistency across vintages
  * allows rls for other tables via joins
  * consider creating functions to move rights (also under RLS)
* implement rights in functions (w/o policies)
* adapt tests to use appropriate connections
  * or at least add tests for the rights stuff
* orchestrate "install" process (do as much as possible as tsdb_admin instead of root)

## temp tables as input
* ensure existence in tables that use them
* orchestrate cleanup

## Journal/stats
?
