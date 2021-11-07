FROM node:10.15-stretch as base

WORKDIR /app
COPY package-lock.json .
COPY package.json .
RUN npm ci --registry=https://registry.npmjs.org

FROM base as builder

WORKDIR /app
COPY src ./src
COPY public ./public
COPY webpack.config.js .
COPY package.json .
RUN npm run build


FROM nginx:alpine as server-image
ARG VERSION='v0.0.0'
COPY docker/default.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/dist/ /usr/share/nginx/html/
COPY public/config.js /usr/share/nginx/html/
COPY docker/startup.sh /startup.sh
RUN echo ${VERSION} > /version.txt

CMD ["/startup.sh"]
