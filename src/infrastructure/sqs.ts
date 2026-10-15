import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import type { Config } from "./config.js";

export class SqsGateway {
  readonly client: SQSClient;
  private queues?: { requests: string; dlq: string; events: string };

  constructor(private readonly config: Config) {
    this.client = new SQSClient({
      endpoint: config.SQS_ENDPOINT,
      region: config.AWS_REGION,
      credentials: {
        accessKeyId: config.AWS_ACCESS_KEY_ID,
        secretAccessKey: config.AWS_SECRET_ACCESS_KEY,
      },
      maxAttempts: 1,
      requestHandler: { connectionTimeout: 2000, requestTimeout: 5000 },
    });
  }

  private localURL(url: string | undefined): string {
    if (!url) throw new Error("SQS did not return a queue URL");
    const endpoint = new URL(this.config.SQS_ENDPOINT);
    const resolved = new URL(url);
    resolved.host = endpoint.host;
    resolved.protocol = endpoint.protocol;
    return resolved.toString();
  }

  async initialize(): Promise<void> {
    const dlqResponse = await this.client.send(
      new CreateQueueCommand({
        QueueName: `${this.config.SQS_QUEUE_PREFIX}wager-transactions-dlq.fifo`,
        Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false" },
      }),
    );
    const dlq = this.localURL(dlqResponse.QueueUrl);
    const attrs = await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: dlq,
        AttributeNames: ["QueueArn"],
      }),
    );
    const requests = await this.client.send(
      new CreateQueueCommand({
        QueueName: `${this.config.SQS_QUEUE_PREFIX}wager-transactions.fifo`,
        Attributes: {
          FifoQueue: "true",
          ContentBasedDeduplication: "false",
          VisibilityTimeout: String(this.config.SQS_VISIBILITY_SECONDS),
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: attrs.Attributes?.QueueArn,
            maxReceiveCount: this.config.SQS_MAX_ATTEMPTS,
          }),
        },
      }),
    );
    const events = await this.client.send(
      new CreateQueueCommand({
        QueueName: `${this.config.SQS_QUEUE_PREFIX}wager-events.fifo`,
        Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false" },
      }),
    );
    this.queues = {
      requests: this.localURL(requests.QueueUrl),
      dlq,
      events: this.localURL(events.QueueUrl),
    };
  }

  get urls() {
    if (!this.queues) throw new Error("SQS not initialized");
    return this.queues;
  }

  async ready(): Promise<void> {
    await Promise.all(
      Object.values(this.urls).map((QueueUrl) =>
        this.client.send(
          new GetQueueAttributesCommand({
            QueueUrl,
            AttributeNames: ["QueueArn"],
          }),
        ),
      ),
    );
  }

  async deadLetterDepth(): Promise<number> {
    const result = await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: this.urls.dlq,
        AttributeNames: [
          "ApproximateNumberOfMessages",
          "ApproximateNumberOfMessagesNotVisible",
        ],
      }),
    );
    return (
      Number(result.Attributes?.ApproximateNumberOfMessages ?? "0") +
      Number(result.Attributes?.ApproximateNumberOfMessagesNotVisible ?? "0")
    );
  }

  async receive(signal?: AbortSignal) {
    return this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: this.urls.requests,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 2,
        VisibilityTimeout: this.config.SQS_VISIBILITY_SECONDS,
        MessageSystemAttributeNames: ["ApproximateReceiveCount"],
      }),
      { abortSignal: signal },
    );
  }

  async ack(receipt: string): Promise<void> {
    await this.client.send(
      new DeleteMessageCommand({
        QueueUrl: this.urls.requests,
        ReceiptHandle: receipt,
      }),
    );
  }

  async visibility(receipt: string, seconds: number): Promise<void> {
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: this.urls.requests,
        ReceiptHandle: receipt,
        VisibilityTimeout: seconds,
      }),
    );
  }

  async deadLetter(body: string, id: string): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.urls.dlq,
        MessageBody: body,
        MessageGroupId: "poison",
        MessageDeduplicationId: id,
      }),
    );
  }

  async publish(
    eventId: string,
    aggregateId: string,
    payload: Record<string, unknown>,
  ): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.urls.events,
        MessageBody: JSON.stringify(payload),
        MessageGroupId: aggregateId,
        MessageDeduplicationId: eventId,
      }),
    );
  }

  close(): void {
    this.client.destroy();
  }
}
