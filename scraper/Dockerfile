FROM node:18

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .

# crear data y dar permisos al usuario node
RUN mkdir -p /app/data \
  && chown -R node:node /app/data

USER node
CMD ["node", "scraper.js"]