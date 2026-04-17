FROM node:24-bookworm-slim AS build

WORKDIR /app

COPY package.json ./
RUN npm install

COPY . .

ARG PUBLIC_API_URL=http://localhost:8192
ENV PUBLIC_API_URL=${PUBLIC_API_URL}

RUN npm run build

FROM node:24-bookworm-slim

WORKDIR /app

COPY --from=build /app/build ./build
COPY --from=build /app/package.json ./package.json

RUN npm install --omit=dev

ENV NODE_ENV=production

EXPOSE 3000

CMD ["node", "build"]
