FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --production
COPY index.js ./
EXPOSE 8080
CMD ["node", "index.js"]
