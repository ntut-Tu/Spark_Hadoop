# frontend-predict/Dockerfile
FROM node:18-alpine as build

WORKDIR /app
COPY package*.json ./
RUN npm install

COPY . .
RUN npm install
RUN npm run build

ENV REACT_APP_API_BASE_URL=http://spark-master:8000

EXPOSE 80
CMD ["npm", "start"]