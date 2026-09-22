# curie build, four runs

## run 1
default docker builder, first source
```json
{"connectors":[{"delivery":"registry","image":"docker.io/acme/stubfin-bot-stubfin@sha256:9d900f3e60f9ed7a18ca5e1377841f927b033d3240717f2c8e08c5cfee1f9268","name":"stubfin","platforms":["linux/amd64"],"source_digest":"sha256:dad4f6d7efd169ca6770df81f7662984580c7c2101c8ef33b38a0af865b94c1c"}]}

```

## run 2
default docker builder, after a source edit (new source_digest, new image digest)
```json
{"connectors":[{"delivery":"registry","image":"docker.io/acme/stubfin-bot-stubfin@sha256:d116f518992aede27560018ccb16cd4b167a1845c0d83c487523e46cd5d691d2","name":"stubfin","platforms":["linux/amd64"],"source_digest":"sha256:9b760fb568e2471d123362d58819f1c69a2ea47dc55be0b889556270e471659c"}]}

```

## run 3
BUILDX_BUILDER=<docker-container builder>, same source as run 2 (same source_digest, DIFFERENT image digest)
```json
{"connectors":[{"delivery":"registry","image":"docker.io/acme/stubfin-bot-stubfin@sha256:d1f526311779ae3174020c77d33ed17afc296046659288d86fead6e1b13c3758","name":"stubfin","platforms":["linux/amd64"],"source_digest":"sha256:9b760fb568e2471d123362d58819f1c69a2ea47dc55be0b889556270e471659c"}]}

```

## run 4
docker-container builder, after the tempdir fix
```json
{"connectors":[{"delivery":"registry","image":"docker.io/acme/stubfin-bot-stubfin@sha256:424195b050f38786218f545d9aa678ff8dae3e9c45af721e6b9af220c9f49e75","name":"stubfin","platforms":["linux/amd64"],"source_digest":"sha256:eef4ea902283447a7a8bbcf1556e5d90ec401898aef90cf9fbd6d1d4bebbd0d9"}]}

```

## the buildx command run 3 issued
```
=== docker buildx build --platform linux/amd64 --push --metadata-file /tmp/curie-build-<id>/stubfin.metadata.json -f <evidence-dir>/bundle/connector/Dockerfile -t docker.io/acme/stubfin-bot-stubfin:9b760fb568e2471d <evidence-dir>/bundle/connector ===
```
