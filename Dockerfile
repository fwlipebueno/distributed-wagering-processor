FROM oven/bun:1.3.10 AS build
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY tsconfig*.json ./
COPY src ./src
RUN bun run build

FROM build AS test
COPY tests ./tests
COPY scripts ./scripts
CMD ["bun", "run", "test:integration"]

FROM oven/bun:1.3.10
WORKDIR /app
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./
USER bun
CMD ["bun", "dist/src/main.js"]
