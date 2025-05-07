# Two stage docker file
# Stage 1: builds the client app into a `dist` folder
# Stage 2: copies the `dist` folder into a nginx image
#          for hosting

# Stage 1
FROM node:22 AS builder

WORKDIR /app

ADD floor-heights /app

RUN npm install && npm run build

# Stage 2
FROM nginx:1.28.0 AS nginx

ADD nginx/nginx-www.conf /etc/nginx/nginx.conf
ADD nginx/nginx-www-default.conf /etc/nginx/conf.d/default.conf

# copy files from builder image
COPY --from=builder /app/dist/ /tmp/dist/
# move built client files into location that will be hosted by nginx
RUN rm -rf /usr/share/nginx/html && mv /tmp/dist/ /usr/share/nginx/html
