# Usamos un tag explícito y multi-stage para producción ligera :contentReference[oaicite:0]{index=0}
FROM node:lts-buster-slim AS build

WORKDIR /app
COPY package*.json ./
RUN npm ci                        # sólo dependencias de producción :contentReference[oaicite:1]{index=1}

FROM node:lts-buster-slim
WORKDIR /app
COPY --from=build /app .  
USER node                        # no correr como root :contentReference[oaicite:2]{index=2}
CMD ["node", "scraper.js"]
