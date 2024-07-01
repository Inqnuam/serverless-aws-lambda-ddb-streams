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
import { defineConfig } from "serverless-aws-lambda/defineConfig";
import { dynamoStream } from "serverless-aws-lambda-ddb-streams";

export default defineConfig({
  plugins: [dynamoStream(dynamoDbClientConfig, pluginOptions)],
});
```

### DynamoDB Client Config

[Configuration](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/Package/-aws-sdk-client-dynamodb/Interface/DynamoDBClientConfig/) used to connect to DynamoDB Tables.
By default the plugin uses following configuration:

```json
{
  "endpoint": "http://127.0.0.1:8000",
  "region": "ddblocal",
  "credentials": { "accessKeyId": "test", "secretAccessKey": "test" }
}
```

If after connecting to the table, StreamEnabled is `false`, the plugin will try to enable it.

See [docker-compose.yml](resources/docker-compose.yml) to bootstrap local instance of DynamoDB using Docker.

### Plugin Options

- waitBeforeInit:  
  An error will be thrown if after "waitBeforeInit" (in seconds) the plugin was not able to connect to the Table. default 25.
- watchInterval:  
  interval (in seconds) to check for new streamable records. default 2

### Event Source Mapping

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
  runtime: nodejs20.x
  region: eu-west-1

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
    type: dynamodb
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
