# First shipped local factory skill

These four directories contain the exact public skill files from PR #4554 at
commit `7d316d15bf8a9b7d486105000926de811a2aade5`. Keep their bytes
unchanged: the upgrade process test installs this historical version and checks
that the current CLI recognizes all four embedded directory digests. This is a
compatibility fixture, not a prompt wording assertion.
