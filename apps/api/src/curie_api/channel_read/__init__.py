"""An agent's bounded read of its own bound channels (ADR 0100, #2877).

``service`` holds the authorization order, ``ledger`` the shared Valkey budget
and generations, ``window`` the time window and cursor rules, ``token`` the
``chr`` capability. ``readers`` is the surface neutral reader contract, and
``slack_reads`` the only Slack read operations. ``canvas`` authorizes canvas
list, read and cell edit (ADR 0200), ``slack_canvas`` holds the only Slack
canvas calls and ``canvas_sections`` the per turn record of readable cells.
"""
