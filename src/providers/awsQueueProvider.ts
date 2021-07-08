import * as Debug from 'debug';
import * as SQS from 'aws-sdk/clients/sqs';
import { AWSError } from 'aws-sdk/lib/error';
import { SQSError } from '../errors';
import {
  IQueueProvider,
  ReceiveMessageOptions,
  ReceiveMessageResult,
  MessageIdentification,
  defaultMessageFields
} from './contracts';

const debug = Debug('sqs-consumer/awsQueueProvider');

type AwsQueueOptions = { sqs?: SQS; region?: string };

export class AwsQueueProvider implements IQueueProvider {
  private sqs: SQS;
  private queueUrl: string;

  constructor(queueUrl: string, options?: AwsQueueOptions) {
    if (!queueUrl) {
      throw new Error("Argument 'queueUrl` is required");
    }

    this.queueUrl = queueUrl;
    this.sqs =
      options.sqs ||
      new SQS({
        region: options?.region || process.env.AWS_REGION || 'eu-west-1'
      });
  }

  public async receiveMessage(options?: ReceiveMessageOptions): Promise<ReceiveMessageResult> {
    try {
      const response = await this.sqs
        .receiveMessage({
          QueueUrl: this.queueUrl,
          MaxNumberOfMessages: options?.maxNumberOfMessages,
          WaitTimeSeconds: options?.waitTimeout,
          VisibilityTimeout: options?.visibilityTimeout,
          ...options?.extraOptions
        })
        .promise();

      return {
        messages:
          response.Messages?.map((item) => ({
            messageId: item.MessageId,
            body: item.Body,
            receiptHandle: item.ReceiptHandle,
            extraFields: Object.keys(item)
              .filter((key) => !defaultMessageFields.includes(key))
              .reduce((current, key) => ({ ...current, [key]: item[key] }), {})
          })) || []
      };
    } catch (err) {
      throw this.toSQSError(err, `SQS receive message failed: ${err.message}`);
    }
  }

  public async deleteMessage(request: MessageIdentification): Promise<void> {
    debug('Deleting message %s', request.messageId);

    try {
      await this.sqs
        .deleteMessage({ QueueUrl: this.queueUrl, ReceiptHandle: request.receiptHandle })
        .promise();
    } catch (err) {
      throw this.toSQSError(err, `SQS delete message failed: ${err.message}`);
    }
  }

  public async deleteMessageBatch(messages: MessageIdentification[]): Promise<void> {
    debug('Deleting messages %s', messages.map((msg) => msg.messageId).join(' ,'));

    try {
      await this.sqs
        .deleteMessageBatch({
          QueueUrl: this.queueUrl,
          Entries: messages.map((message) => ({
            Id: message.messageId,
            ReceiptHandle: message.receiptHandle
          }))
        })
        .promise();
    } catch (err) {
      throw this.toSQSError(err, `SQS delete message failed: ${err.message}`);
    }
  }

  public async changeMessageVisibility(message: MessageIdentification, timeout: number): Promise<void> {
    try {
      await this.sqs
        .changeMessageVisibility({
          QueueUrl: this.queueUrl,
          ReceiptHandle: message.receiptHandle,
          VisibilityTimeout: timeout
        })
        .promise();
    } catch (err) {
      throw this.toSQSError(err, `SQS change visibility message failed: ${err.message}`);
    }
  }

  public async changeMessageVisibilityBatch(
    messages: MessageIdentification[],
    timeout: number
  ): Promise<void> {
    try {
      await this.sqs
        .changeMessageVisibilityBatch({
          QueueUrl: this.queueUrl,
          Entries: messages.map((message) => ({
            Id: message.messageId,
            ReceiptHandle: message.receiptHandle,
            VisibilityTimeout: timeout
          }))
        })
        .promise();
    } catch (err) {
      throw this.toSQSError(err, `SQS change visibility batch message failed: ${err.message}`);
    }
  }

  private toSQSError(err: AWSError, message: string): SQSError {
    return new SQSError(message, err);
  }
}
