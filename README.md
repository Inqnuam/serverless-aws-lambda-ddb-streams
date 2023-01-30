## Description

> DynamoDB local Streams for serverless-aws-lambda

# Installation

```bash
yarn add -D serverless-aws-lambda-ddb-streams
# or
npm install -D serverless-aws-lambda-ddb-streams
```

## Usage

```js
// config.js
const { defineConfig } = require("serverless-aws-lambda/defineConfig");
const { dynamoStream } = require("serverless-aws-lambda-ddb-streams");

module.exports = defineConfig({
  plugins: [dynamoStream()],
});
```
