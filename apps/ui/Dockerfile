# syntax=docker/dockerfile:1
# Curie UI image: static Vite build served by non-root nginx.
#
# BUILD FROM THE REPO ROOT, not from apps/ui/, because the UI resolves the frozen
# ACI TypeScript types through a path alias (@aci -> ../../packages/aci-protocol/
# generated/ts, in both tsconfig and vite), which lives outside apps/ui. The repo
# layout is replicated in the build stage so that relative alias resolves:
#
#   docker build -f apps/ui/Dockerfile -t curie-ui .
#
# apps/ui/Dockerfile.dockerignore (BuildKit honors <dockerfile>.dockerignore over
# the context-root one) trims the root context to the UI plus that one
# cross-package tree.
FROM node:22.23.2-slim@sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5 AS build
WORKDIR /app
# Pin pnpm explicitly: package.json has no packageManager field, so corepack
# would otherwise pull whatever pnpm is latest. pnpm 10+ turns a dependency with
# an unapproved build script (esbuild, which vite needs) into a hard install
# failure; pnpm 9 runs those scripts by default and reads this lockfile
# (lockfileVersion 9.0) unchanged, so the build is reproducible without an
# approve-builds step or a package.json edit.
RUN corepack enable && corepack prepare pnpm@9.15.9 --activate
# Install deps first (own cache layer) from the UI's self-contained lockfile.
COPY apps/ui/package.json apps/ui/pnpm-lock.yaml ./apps/ui/
WORKDIR /app/apps/ui
RUN pnpm install --frozen-lockfile
# The frozen ACI types the @aci alias points at, then the UI sources. Both land
# at their repo-relative paths so `../../packages/...` resolves from apps/ui.
WORKDIR /app
COPY packages/aci-protocol/generated ./packages/aci-protocol/generated
COPY apps/ui ./apps/ui
WORKDIR /app/apps/ui
RUN pnpm build

FROM nginxinc/nginx-unprivileged:1.27.5-alpine@sha256:65e3e85dbaed8ba248841d9d58a899b6197106c23cb0ff1a132b7bfe0547e4c0
# Only CURIE_API_TARGET is substituted; nginx's own $vars are left intact.
ENV CURIE_API_TARGET=http://curie-api:8000 \
    NGINX_ENVSUBST_FILTER=CURIE_API_TARGET
COPY --from=build /app/apps/ui/dist /usr/share/nginx/html
COPY <<'EOF' /etc/nginx/templates/default.conf.template
server {
    listen 8080;
    server_name _;
    root /usr/share/nginx/html;
    index index.html;

    # Compress static text payloads. The single Vite JS bundle is about
    # 567 kB raw and about 155 kB gzipped, and it is render-blocking for the
    # SPA, so shrinking it on the wire is worth the CPU. text/html is always
    # gzipped by nginx and cannot be listed here.
    gzip on;
    gzip_vary on;
    gzip_comp_level 6;
    gzip_min_length 1024;
    gzip_types application/javascript text/javascript text/css application/json image/svg+xml;

    # Internal worker routes are never exposed through the browser-facing
    # reverse proxy, even when a client knows their path.
    location = /api/v1/internal {
        return 404;
    }
    location ^~ /api/v1/internal/ {
        return 404;
    }

    # Reverse-proxy operator API calls, stripping the /api prefix.
    location /api/ {
        # gzip is on at server scope and application/json is in gzip_types,
        # so proxied API responses would otherwise get compressed here.
        # gzip off keeps them byte-identical to what the upstream sent.
        # Buffering is left at its default on purpose; streaming behavior
        # is unchanged from before this commit.
        gzip off;
        proxy_pass ${CURIE_API_TARGET}/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Vite emits content-hashed filenames for build assets, so a new build is
    # a new URL and these can be cached for a year without ever going stale.
    location ^~ /assets/ {
        add_header Cache-Control "public, max-age=31536000, immutable";
        # A missing hashed asset must 404, not fall through to index.html and
        # get cached for a year as an HTML body.
        try_files $uri =404;
    }

    # SPA fallback.
    location / {
        # index.html is the pointer at the hashed assets, so a redeploy is
        # only picked up if it is revalidated on every request.
        add_header Cache-Control "no-cache" always;
        try_files $uri $uri/ /index.html;
    }
}
EOF
EXPOSE 8080
