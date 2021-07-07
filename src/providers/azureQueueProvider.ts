import * as Debug from 'debug';
import { SQSError } from '../errors';
import { QueueClient, QueueServiceClient } from '@azure/storage-queue';
import {
  IQueueProvider,
  ReceiveMessageOptions,
  ReceiveMessageResult,
  MessageIdentification
} from './contracts';

const debug = Debug('sqs-consumer/awsQueueProvider');

export class AzureQueueProvider implements IQueueProvider {
  private sqs: QueueClient;
  private queueUrl: string;

  constructor(queueUrl: string, connectionString: string) {
    this.queueUrl = queueUrl;
    this.sqs = QueueServiceClient.fromConnectionString(connectionString).getQueueClient(queueUrl);
  }

  public async receiveMessage(options?: ReceiveMessageOptions): Promise<ReceiveMessageResult> {
    try {
      return this.sqs
        .receiveMessages({
          QueueUrl: this.queueUrl,
          MaxNumberOfMessages: options?.maxNumberOfMessages,
          WaitTimeSeconds: options?.waitTimeout,
          VisibilityTimeout: options?.visibilityTimeout
        })
        .then((item) => ({
          messages: item.receivedMessageItems.map((item) => ({
            messageId: item.messageId,
            body: item.messageText,
            receiptHandle: item.popReceipt
          }))
        }));
    } catch (err) {
      throw this.toSQSError(err, `SQS receive message failed: ${err.message}`);
    }
  }

  public async deleteMessage(request: MessageIdentification): Promise<void> {
    debug('Deleting message %s', request.messageId);

    try {
      await this.sqs.deleteMessage(request.messageId, request.receiptHandle);
    } catch (err) {
      throw this.toSQSError(err, `SQS delete message failed: ${err.message}`);
    }
  }

  public async deleteMessageBatch(messages: MessageIdentification[]): Promise<void> {
    debug('Deleting messages %s', messages.map((msg) => msg.messageId).join(' ,'));

    try {
      await Promise.all(messages.map((item) => this.sqs.deleteMessage(item.messageId, item.receiptHandle)));
    } catch (err) {
      throw this.toSQSError(err, `SQS delete message failed: ${err.message}`);
    }
  }

  public async changeMessageVisibility(message: MessageIdentification, timeout: number): Promise<void> {
    try {
      await this.sqs.updateMessage(message.messageId, null, message.receiptHandle, timeout);
    } catch (err) {
      throw this.toSQSError(err, `SQS change visibility message failed: ${err.message}`);
    }
  }

  public async changeMessageVisibilityBatch(
    messages: MessageIdentification[],
    timeout: number
  ): Promise<void> {
    try {
      await Promise.all(messages.map((item) => this.changeMessageVisibility(item, timeout)));
    } catch (err) {
      throw this.toSQSError(err, `SQS change visibility batch message failed: ${err.message}`);
    }
  }

  private toSQSError(err: any, message: string): SQSError {
    const sqsError = new SQSError(message);
    // handle azure exception.
    // sqsError.code = err.code;
    // sqsError.statusCode = err.statusCode;
    // sqsError.region = err.region;
    // sqsError.retryable = err.retryable;
    // sqsError.hostname = err.hostname;
    // sqsError.time = err.time;

    return sqsError;
  }
}
