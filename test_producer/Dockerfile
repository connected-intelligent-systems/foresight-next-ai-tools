FROM node:23

RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser

WORKDIR /app

COPY package.json /app
COPY package-lock.json /app

RUN npm install --production

RUN chown -R appuser:appuser /app

USER appuser

COPY ./index.js /app/

HEALTHCHECK NONE

ENTRYPOINT [ "npm", "run", "start:production" ]