# Name the node stage "builder"
FROM node:buster AS builder
# Set working directory
WORKDIR /app
# Copy all files from current directory to working dir in image
COPY . .
# install node modules and build assets
RUN yarn install && yarn run build

CMD [ "node", "." ]