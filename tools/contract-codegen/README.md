# Contract TypeScript toolchain

This private pnpm project owns the tools used to generate and compile the
committed ACI TypeScript contract. `package.json` pins
`json-schema-to-typescript` to 15.0.4, TypeScript to 5.9.3, and pnpm to 9.15.9.
`pnpm-lock.yaml` locks their transitive dependencies. Dependency updates must
update the manifest and lockfile together and preserve the generated contract
bytes unless a contract change is intended.

Install Node.js 22 and pnpm 9.15.9, then install the locked dependencies from the
repository root:

```bash
pnpm --dir tools/contract-codegen install --frozen-lockfile
```

Run the contributor contract gate with `curie dev contracts`. CI and fast
preflight install this same frozen lockfile before executing the generator and
compiler. The commands use `pnpm --dir tools/contract-codegen exec json2ts` and
`pnpm --dir tools/contract-codegen exec tsc`, so their schema, output, and
TypeScript configuration arguments must be absolute paths: pnpm executes the
tools from this directory.
