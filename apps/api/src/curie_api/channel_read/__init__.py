"""An agent's bounded read of its own bound channels (ADR 0100, #2877).

``service`` holds the authorization order, ``ledger`` the shared Valkey budget
and generations, ``window`` the time window and cursor rules, ``token`` the
``chr`` capability, and ``slack_reads`` the only Slack read operations.
"""
