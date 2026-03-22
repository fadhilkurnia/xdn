FROM node:22-bookworm-slim AS runtime

WORKDIR /usr/src/app
ENV NODE_ENV=production

COPY package.json ./

RUN --mount=type=cache,target=/usr/src/app/.npm \
  npm set cache /usr/src/app/.npm && \
  npm install --omit=dev

COPY . .

EXPOSE 3000

CMD ["npm", "start"]
