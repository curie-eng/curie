# curie cluster deploy, four runs

## run 1
```
Error: docker.io/acme/stubfin-bot-stubfin@sha256:d116f518992aede27560018ccb16cd4b167a1845c0d83c487523e46cd5d691d2 covers [] in the registry, but this cluster's nodes report [amd64], so [amd64] has no image to run. The lock declares [linux/amd64].
· docker.io/acme/stubfin-bot-stubfin@sha256:d116f518992aede27560018ccb16cd4b167a1845c0d83c487523e46cd5d691d2 covers [] in the registry, but this cluster's nodes report [amd64], so [amd64] has no image to run. The lock declares [linux/amd64].
exit=2
```

## run 2
```
deploying stubfin-bot (6845 bytes) to http://127.0.0.1:35053 [dev]
deploying stubfin-bot as stubfin-bot: ok (0.1s)
connectors: applied 4 object(s) for stubfin-bot
connector stubfin: http://curie-adr-stubfin-bot-mcp-stubfin.curie-adr.svc.cluster.local:8000/mcp
deployed stubfin-bot 0.1.0-1788540216 -> dev
agent         stubfin-bot (<uuid-1>)
version       0.1.0-1788540216 (<uuid-2>)
channel       C0LOCALDEV
bundle        bundles/<uuid-1>/<uuid-2>.tar.gz sha256:549930dfed5ded3329f96e2a2e94cd00855e4b641155c7cbbf37f4fc833e7744 6845 bytes
deployment    <uuid-3> [dev] active
exit=0
```

## run 3
```
deploying stubfin-bot (7005 bytes) to http://127.0.0.1:42957 [dev]
deploying stubfin-bot as stubfin-bot: ok (0.1s)
connectors: applied 4 object(s) for stubfin-bot
connector stubfin: http://curie-adr-stubfin-bot-mcp-stubfin.curie-adr.svc.cluster.local:8000/mcp
deployed stubfin-bot 0.1.0-1788540288 -> dev
agent         stubfin-bot (<uuid-1>)
version       0.1.0-1788540288 (<uuid-4>)
channel       unchanged (C0LOCALDEV); pass --slack-channel to bind another
bundle        bundles/<uuid-1>/<uuid-4>.tar.gz sha256:2a807f257390f137bc9a7e68d4e5c79ed0caf91a9c1371b5831ddedb526c1bda 7005 bytes
deployment    <uuid-5> [dev] active
exit=0
```

## run 4
```
deploying stubfin-bot (7005 bytes) to http://127.0.0.1:39231 [dev]
deploying stubfin-bot as stubfin-bot: ok (0.1s)
connectors: applied 4 object(s) for stubfin-bot
connector stubfin: http://curie-adr-stubfin-bot-mcp-stubfin.curie-adr.svc.cluster.local:8000/mcp
deployed stubfin-bot 0.1.0-1788540457 -> dev
agent         stubfin-bot (<uuid-1>)
version       0.1.0-1788540457 (<uuid-6>)
channel       unchanged (C0LOCALDEV); pass --slack-channel to bind another
bundle        bundles/<uuid-1>/<uuid-6>.tar.gz sha256:2a807f257390f137bc9a7e68d4e5c79ed0caf91a9c1371b5831ddedb526c1bda 7005 bytes
deployment    <uuid-7> [dev] active
exit=0
```
