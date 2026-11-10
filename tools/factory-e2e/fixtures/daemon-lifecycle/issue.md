Run the Docker lifecycle probe for this repository.

The repository is opted into a per-run Docker daemon. Start one container
through the daemon the run was given, report the daemon's identity, and hold
until the operator releases the run or the daemon goes away.

This issue is a fixture for the daemon lifecycle acceptance driver. It never
needs a pull request.
