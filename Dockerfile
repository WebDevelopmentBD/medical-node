# ---- Stage 1: builder (has python3/make/g++ for native modules) ----
FROM node:20-alpine AS builder
RUN apk add --no-cache python3 make g++
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev

# ---- Stage 2: runtime (clean, no build tools, no python) ----
FROM node:20-alpine
RUN apk add --no-cache tini
WORKDIR /app
COPY --from=builder /app/node_modules ./node_modules
COPY package*.json ./
COPY . .

ENV TZ=Asia/Dhaka
ENV NODE_ENV=production

ENTRYPOINT ["/sbin/tini", "--"]
