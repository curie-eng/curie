"""Principal resolution through identity links (#2910, ADR 0198, ADR 0201).

``service`` answers "which principal sent this event?" by ADR 0198 decision
7's ordered checks; ``slack`` is Slack's sender field mapping (the realization
ADR 0201 decision 1 places in code); ``attach`` records what a Slack
identity's own ``auth.test`` reported (#3039). Kept free of imports so the
schema modules can import the reserved attribute key without a cycle.
"""
