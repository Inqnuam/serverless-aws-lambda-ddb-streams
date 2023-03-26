## Description

> DynamoDB local Streams for serverless-aws-lambda

## Installation

```bash
yarn add -D serverless-aws-lambda-ddb-streams
# or
npm install -D serverless-aws-lambda-ddb-streams
```

## Usage

use [serverless-aws-lambda's](https://github.com/Inqnuam/serverless-aws-lambda) defineConfig to import this plugin

```js
// config.js
const { defineConfig } = require("serverless-aws-lambda/defineConfig");
const { dynamoStream } = require("serverless-aws-lambda-ddb-streams");

module.exports = defineConfig({
  plugins: [dynamoStream()],
});
```

### Configuration

```ts
{
  endpoint?: string; // default "http://localhost:8000"
  region?: string; // based on your serverless.yml or default "eu-west-1"
  waitBeforeInit?: number; // default 25 (secondes)
  watchInterval?: number; // default 2 (secondes)
}
```

- endpoint:  
  local DynamoDB http endpoint
- region:
  AWS Region for dynamodb client
- waitBeforeInit:  
  An error will be thrown if after "waitBeforeInit" the plugin was not able to connect to the Table.
- watchInterval:  
  interval to check for new streamable records.

example:

```js
module.exports = defineConfig({
  plugins: [
    dynamoStream({
      endpoint: "http://localhost:8822",
      waitBeforeInit: 40,
    }),
  ],
});
```

```yaml
# serverless.yml
service: sls-project

frameworkVersion: "3"

plugins:
  - serverless-aws-lambda

custom:
  serverless-aws-lambda:
    configPath: ./config.default

provider:
  name: aws
  runtime: nodejs18.x
  region: eu-west-3

functions:
  myAwsomeLambda:
    handler: src/handlers/lambda.default
    events:
      - stream:
          arn: arn:aws:dynamodb:region:XXXXXX:table/Banana/stream/1970-01-01T00:00:00.000
          batchSize: 3
```

### Supported stream declarations

```yaml
- stream:
    arn: arn:aws:dynamodb:region:XXXXXX:table/Banana/stream/1970-01-01T00:00:00.000
```

```yaml
- stream:
    type: dynamodb
    arn:
      Fn::GetAtt: [MyDynamoDbTable, StreamArn]
```

```yaml
- stream:
    arn:
      Fn::ImportValue: MyExportedDynamoDbStreamArnId
```

```yaml
- stream:
    arn:
      Ref: MyDynamoDbTableStreamArn
```

```yaml
- stream:
    arn: !GetAtt dynamoTable.StreamArn
```

```yaml
functions:
  myAwsomeLambda:
    handler: src/handlers/lambda.default
    events:
      - stream:
          arn: arn:aws:dynamodb:region:XXXXXX:table/Banana/stream/1970-01-01T00:00:00.000
          batchSize: 3
          filterPatterns:
            - eventName: [INSERT]
            - dynamodb:
                NewImage:
                  OrderId:
                    N:
                      - numeric: [">", 4]
```

### Supported configurations

✅ supported  
🌕 planned  
❌ not planned

- ✅ batchSize
- ✅ batchWindow
- ✅ bisectBatchOnFunctionError
- ✅ destinations (requires [AWS Local SNS](https://github.com/Inqnuam/serverless-aws-lambda/blob/main/resources/sns.md) and/or [AWS Local SQS](https://github.com/Inqnuam/serverless-aws-lambda/blob/main/resources/sqs.md))
- ✅ enabled
- ✅ filterPatterns
- ✅ functionResponseType
- ✅ maximumRecordAgeInSeconds
- ✅ maximumRetryAttempts
- ❌ startingPosition (uses always "LATEST")
- ❌ startingPositionTimestamp
- ✅ tumblingWindowInSeconds
- ❌ parallelizationFactor
