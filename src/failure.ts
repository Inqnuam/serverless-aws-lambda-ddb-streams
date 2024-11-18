import { randomUUID } from "crypto";
import http from "http";
import type { ClientRequest } from "http";

export class StreamFailure {
  TopicArn: string = "";
  QueueUrl: string = "";
  functionArn: string = "";
  getTimeoutBody: (DDBStreamBatchInfo: any, approximateInvokeCount: number) => string;
  getUnhandledBody: (DDBStreamBatchInfo: any, approximateInvokeCount: number) => string;
  createRequest: () => ClientRequest;
  kind: "sns" | "sqs";
  name: string;
  static LOCAL_PORT = 3000;
  static REGION = "eu-west-1";
  constructor({ kind, name }: { kind: "sns" | "sqs"; name: string }, lambdaName: string) {
    this.functionArn = `arn:aws:lambda:${StreamFailure.REGION}:000000000000:function:${lambdaName}`;
    this.kind = kind;
    this.name = name;
    if (kind == "sns") {
      this.TopicArn = `arn:aws:sns:${StreamFailure.REGION}:000000000000:${name}`;
      this.getTimeoutBody = this.timeoutFailureSnsMsg;
      this.getUnhandledBody = this.unhandledFailureSnsMsg;
      this.createRequest = () => {
        return http.request(`http://localhost:${StreamFailure.LOCAL_PORT}/@sns/parsed/`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
        });
      };
    } else {
      this.QueueUrl = `http://localhost:${StreamFailure.LOCAL_PORT}/@sqs/${name}`;

      this.getTimeoutBody = this.timeoutFailureSqsMsg;
      this.getUnhandledBody = this.unhandledFailureSqsMsg;
      this.createRequest = () => {
        return http.request(`http://localhost:${StreamFailure.LOCAL_PORT}/@sqs`, {
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
          },
        });
      };
    }
  }

  genBody = (DDBStreamBatchInfo: any, approximateInvokeCount: number) => {
    return {
      requestContext: {
        requestId: randomUUID(),
        functionArn: this.functionArn,
        condition: "",
        approximateInvokeCount,
      },
      version: "1.0",
      timestamp: new Date().toISOString(),
      DDBStreamBatchInfo,
    };
  };
  getSnsBody = (DDBStreamBatchInfo: any, approximateInvokeCount: number) => {
    return {
      Type: "Notification",
      MessageId: randomUUID(),
      TopicArn: this.TopicArn,
      Subject: null,
      Message: this.genBody(DDBStreamBatchInfo, approximateInvokeCount),
      Timestamp: new Date().toISOString(),
      SignatureVersion: "1",
      Signature: "fake",
      SigningCertUrl: `http://localhost:${StreamFailure.LOCAL_PORT}/@sns/SimpleNotificationService-56e67fcb41f6fec09b0196692625d388.pem`,
      UnsubscribeUrl: `http://localhost:${StreamFailure.LOCAL_PORT}/@sns?Action=Unsubscribe&SubscriptionArn=${this.TopicArn}:${randomUUID()}`,
      MessageAttributes: {},
    };
  };
  timeoutFailureSnsMsg = (DDBStreamBatchInfo: any, approximateInvokeCount: number) => {
    const body: any = this.getSnsBody(DDBStreamBatchInfo, approximateInvokeCount);
    body.Message.requestContext.condition = "RecordAgeExceeded";
    body.Message = JSON.stringify(body.Message);

    return JSON.stringify(body);
  };
  unhandledFailureSnsMsg = (DDBStreamBatchInfo: any, approximateInvokeCount: number) => {
    const body: any = this.getSnsBody(DDBStreamBatchInfo, approximateInvokeCount);
    body.Message.requestContext.condition = "RetryAttemptsExhausted";
    body.Message.responseContext = {
      statusCode: 200,
      executedVersion: "$LATEST",
      functionError: "Unhandled",
    };
    body.Message = JSON.stringify(body.Message);

    return JSON.stringify(body);
  };

  timeoutFailureSqsMsg = (DDBStreamBatchInfo: any, approximateInvokeCount: number) => {
    const MessageBody = this.genBody(DDBStreamBatchInfo, approximateInvokeCount);
    MessageBody.requestContext.condition = "RecordAgeExceeded";

    const query = new URLSearchParams();
    query.append("QueueUrl", this.QueueUrl);
    query.append("Action", "SendMessage");

    query.append("MessageBody", JSON.stringify(MessageBody));

    return query.toString();
  };

  unhandledFailureSqsMsg = (DDBStreamBatchInfo: any, approximateInvokeCount: number) => {
    const MessageBody: any = this.genBody(DDBStreamBatchInfo, approximateInvokeCount);
    MessageBody.requestContext.condition = "RetryAttemptsExhausted";
    MessageBody.responseContext = {
      statusCode: 200,
      executedVersion: "$LATEST",
      functionError: "Unhandled",
    };
    const query = new URLSearchParams();
    query.append("QueueUrl", this.QueueUrl);
    query.append("Action", "SendMessage");

    query.append("MessageBody", JSON.stringify(MessageBody));

    return query.toString();
  };

  timeout(DDBStreamBatchInfo: any) {
    const body = this.getTimeoutBody(DDBStreamBatchInfo, 1);
    const req = this.createRequest();
    console.log(`\x1b[35mDynamoDB Stream expire failure: calling onFailure > ${this.kind.toUpperCase()} ${this.name}\x1b[0m`);
    req.end(body);
  }
  unhandled(DDBStreamBatchInfo: any, approximateInvokeCount: number) {
    const body = this.getUnhandledBody(DDBStreamBatchInfo, approximateInvokeCount);
    const req = this.createRequest();
    console.log(`\x1b[35mDynamoDB Stream unhandled failure: calling onFailure > ${this.kind.toUpperCase()} ${this.name}\x1b[0m`);
    req.end(body);
  }
}
const isValidEventId = (id: any) => typeof id == "string" && id.length;
export const getBatchItemFailures = (records: any[], response?: any) => {
  if (
    typeof response === undefined ||
    response === null ||
    (typeof response == "object" && (response.batchItemFailures === null || (Array.isArray(response.batchItemFailures) && !response.batchItemFailures.length)))
  ) {
    // considered as complete success
    return;
  }

  if (typeof response == "object" && Array.isArray(response.batchItemFailures)) {
    if (response.batchItemFailures.some((x: any) => !x.itemIdentifier || !isValidEventId(x.itemIdentifier) || !records.find((r) => r.eventID == x.itemIdentifier))) {
      throw new Error("ReportBatchItemFailures: complete failure.");
    } else {
      // return failed messages

      const success: string[] = [];
      const failures: any[] = [];
      records.forEach((r) => {
        const foundFailed = response.batchItemFailures.find((x: any) => x.itemIdentifier == r.eventID);

        if (foundFailed) {
          failures.push(r);
        } else {
          success.push(r.eventID);
        }
      });

      return {
        success,
        failures,
      };
    }
  }
};
