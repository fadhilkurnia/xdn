FROM node:10-alpine

# To build the image: 
# docker build --tag=todo .

RUN mkdir -p /home/node/app/node_modules && chown -R node:node /home/node/app
WORKDIR /home/node/app
COPY package*.json ./
RUN npm install 
COPY --chown=node:node . .
EXPOSE 80
CMD [ "npm", "start" ]