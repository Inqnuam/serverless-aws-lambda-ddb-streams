import { DynamoDBStreamsClient, DescribeStreamCommand, GetShardIteratorCommand, GetRecordsCommand, Shard, GetShardIteratorCommandInput, ExpiredIteratorException } from "@aws-sdk/client-dynamodb-streams";
import { DynamoDBClient, DescribeTableCommand, UpdateTableCommand, waitUntilTableExists } from "@aws-sdk/client-dynamodb";
import EventEmitter from "events";

export class DynamoStream extends EventEmitter {
  cli: DynamoDBClient;
  streamCli: DynamoDBStreamsClient;
  watcher?: NodeJS.Timeout;
  TableName: string;
  StreamViewType: string = "NEW_AND_OLD_IMAGES";
  static endpoint: string = "http://localhost:8000";
  static maxWaitTime: number = 25;
  static watchInterval: number = 1;
  static region: string = "eu-west-1";
  constructor(config = { TableName: "", StreamViewType: undefined }) {
    super();
    const conf = {
      endpoint: DynamoStream.endpoint,
      region: DynamoStream.region,
    };
    this.cli = new DynamoDBClient(conf);
    this.streamCli = new DynamoDBStreamsClient(conf);
    this.TableName = config.TableName;

    if (config.StreamViewType) {
      this.StreamViewType = config.StreamViewType;
    }
  }

  async init() {
    await this.#enableStream();

    const LatestStreamArn = await this.getLatestStreamArn();
    const StreamDescription = await this.describeStream(LatestStreamArn!);

    const { Shards, StreamArn } = StreamDescription!;

    if (StreamArn && Shards?.length) {
      await this.watch(Shards[Shards.length - 1], StreamArn);
    }
  }

  async getLatestStreamArn() {
    const { Table } = await this.cli.send(new DescribeTableCommand({ TableName: this.TableName }));

    return Table!.LatestStreamArn;
  }
  async describeStream(StreamArn: string) {
    const cmd = new DescribeStreamCommand({
      StreamArn,
    });

    const { StreamDescription } = await this.streamCli.send(cmd);
    return StreamDescription;
  }
  async getLatestSequenceNumber(Shard: Shard, StreamArn: string) {
    const ShardIterator = await this.getShardInfo(Shard, StreamArn);
    const { Records } = await this.getRecords(ShardIterator);

    if (Records?.length) {
      return Records[Records.length - 1].dynamodb?.SequenceNumber;
    }
  }
  async getShardInfo(Shard: Shard, StreamArn: string, SequenceNumber?: string) {
    let params: GetShardIteratorCommandInput = {
      StreamArn,
      ShardId: Shard.ShardId,
      ShardIteratorType: "AFTER_SEQUENCE_NUMBER",
      SequenceNumber: SequenceNumber ?? Shard.SequenceNumberRange!.EndingSequenceNumber ?? Shard.SequenceNumberRange?.StartingSequenceNumber,
    };
    const shardInfo = new GetShardIteratorCommand(params);

    const res = await this.streamCli.send(shardInfo);
    return res.ShardIterator;
  }
  stop() {
    clearInterval(this.watcher);
  }
  async #enableStream() {
    try {
      await waitUntilTableExists({ client: this.cli, maxWaitTime: DynamoStream.maxWaitTime }, { TableName: this.TableName });
      const enableStream = new UpdateTableCommand({
        TableName: this.TableName,
        StreamSpecification: {
          StreamEnabled: true,
          StreamViewType: this.StreamViewType,
        },
      });
      try {
        await this.cli.send(enableStream);
      } catch (error) {}
    } catch (error) {
      console.log(`Could not connect to table ${this.TableName}`);
    }
  }

  async getRecords(ShardIterator: string | undefined) {
    const recordCmd = new GetRecordsCommand({
      ShardIterator,
    });
    return this.streamCli.send(recordCmd);
  }
  async watch(Shard: Shard, StreamArn: string) {
    const SequenceNumber = await this.getLatestSequenceNumber(Shard, StreamArn);
    const ShardIterator = await this.getShardInfo(Shard, StreamArn, SequenceNumber);
    let iterator = ShardIterator;

    this.watcher = setInterval(async () => {
      try {
        const { NextShardIterator, Records } = await this.getRecords(iterator);
        if (NextShardIterator) {
          iterator = NextShardIterator;
        }

        if (Records?.length) {
          const dummyDate = new Date().toISOString();
          const DDBStreamBatchInfo = {
            shardId: Shard.ShardId,
            startSequenceNumber: Shard.SequenceNumberRange?.StartingSequenceNumber,
            endSequenceNumber: SequenceNumber,
            approximateArrivalOfFirstRecord: dummyDate,
            approximateArrivalOfLastRecord: dummyDate,
            batchSize: Records.length,
            streamArn: StreamArn,
          };
          this.emit("records", Records, DDBStreamBatchInfo);
        }
      } catch (error: any) {
        if (error instanceof ExpiredIteratorException) {
          this.stop();
          await this.init();
        } else {
          console.log(error);
        }
      }
    }, DynamoStream.watchInterval * 1000);
  }
}
