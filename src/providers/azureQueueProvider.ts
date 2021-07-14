import * as Debug from 'debug';
import { SQSError } from '../errors';
import { QueueClient, QueueReceiveMessageOptions, QueueServiceClient, RestError } from '@azure/storage-queue';
import {
  IQueueProvider,
  ReceiveMessageOptions,
  ReceiveMessageResult,
  MessageIdentification,
  defaultMessageFields
} from './contracts';

const debug = Debug('sqs-consumer/awsQueueProvider');

type AzureQueueOptions = { queueServiceClient?: QueueServiceClient; connectionString?: string };

export class AzureQueueProvider implements IQueueProvider {
  private sqs: QueueClient;

  constructor(queueUrl: string, options: AzureQueueOptions) {
    if (!queueUrl) {
      throw new Error("Argument 'queueUrl' is required");
    }

    if (!options) {
      throw new Error("Argument 'options' is required");
    }

    if (!options.connectionString && !options.queueServiceClient) {
      throw new Error("'options.connectionString' or 'options.queueServiceClient' is required");
    }

    const client =
      options?.queueServiceClient || QueueServiceClient.fromConnectionString(options.connectionString);

    this.sqs = client.getQueueClient(queueUrl);
  }

  public async receiveMessage(options?: ReceiveMessageOptions): Promise<ReceiveMessageResult> {
    try {
      const params: QueueReceiveMessageOptions = {
        visibilityTimeout: options.visibilityTimeout,
        numberOfMessages: options.maxNumberOfMessages,
        ...options?.extraOptions
      };

      const response = await this.sqs.receiveMessages(params);

      return {
        messages: response.receivedMessageItems.map((item) => ({
          messageId: item.messageId,
          body: item.messageText,
          receiptHandle: item.popReceipt,
          extraFields: Object.keys(item)
            .filter((key) => !defaultMessageFields.includes(key))
            .reduce((current, key) => ({ ...current, [key]: item[key] }), {})
        }))
      };
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

  private toSQSError(err: RestError, message: string): SQSError {
    return new SQSError(message, err);
  }
}
