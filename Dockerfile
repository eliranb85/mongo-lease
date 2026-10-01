FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN MONGOMS_DISABLE_POSTINSTALL=1 npm ci --ignore-scripts
COPY tsconfig.json tsconfig.build.json ./
COPY src ./src
COPY examples ./examples
RUN npm run build
CMD ["node", "examples/scheduler.mjs"]
