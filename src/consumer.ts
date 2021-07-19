import * as Debug from 'debug';
import { EventEmitter } from 'events';
import { autoBind } from './bind';
import { SQSError, TimeoutError } from './errors';
import {
  MessageList,
  ReceiveMessageResult,
  Message,
  IQueueProvider,
  ReceiveMessageOptions
} from './providers/contracts';

const debug = Debug('sqs-consumer');

const requiredOptions = [
  // only one of handleMessage / handleMessagesBatch is required
  'handleMessage|handleMessageBatch'
];

interface TimeoutResponse {
  timeout: NodeJS.Timeout;
  pending: Promise<void>;
}

function createTimeout(duration: number): TimeoutResponse[] {
  let timeout;
  const pending = new Promise((_, reject) => {
    timeout = setTimeout((): void => {
      reject(new TimeoutError());
    }, duration);
  });
  return [timeout, pending];
}

function assertOptions(options: ConsumerOptions): void {
  requiredOptions.forEach((option) => {
    const possibilities = option.split('|');
    if (!possibilities.find((p) => options[p])) {
      throw new Error(`Missing SQS consumer option [ ${possibilities.join(' or ')} ].`);
    }
  });

  if (options.batchSize > 10 || options.batchSize < 1) {
    throw new Error('SQS batchSize option must be between 1 and 10.');
  }

  if (options.heartbeatInterval && !(options.heartbeatInterval < options.visibilityTimeout)) {
    throw new Error('heartbeatInterval must be less than visibilityTimeout.');
  }
}

function isConnectionError(err: Error): boolean {
  if (err instanceof SQSError) {
    return (
      err.statusCode === 403 ||
      err.code === 'CredentialsError' ||
      err.code === 'UnknownEndpoint' ||
      err.code === 'QueueNotFound' ||
      err.code === 'AuthorizationFailure'
    );
  }
  return false;
}

function hasMessages(response: ReceiveMessageResult): boolean {
  return response.messages && response.messages.length > 0;
}

export interface ConsumerOptions {
  stopped?: boolean;
  batchSize?: number;
  visibilityTimeout?: number;
  authenticationErrorTimeout?: number;
  pollingWaitTimeMs?: number;
  terminateVisibilityTimeout?: boolean;
  heartbeatInterval?: number;
  sqs?: IQueueProvider;
  handleMessageTimeout?: number;
  handleMessage?(message: Message): Promise<void>;
  handleMessageBatch?(messages: Message[]): Promise<void>;
  receiveOptions?: Record<string, unknown>;
}

interface Events {
  response_processed: [];
  empty: [];
  message_received: [Message];
  message_processed: [Message];
  error: [Error, void | Message | Message[]];
  timeout_error: [Error, Message];
  processing_error: [Error, Message];
  stopped: [];
}

export class Consumer extends EventEmitter {
  private handleMessage: (message: Message) => Promise<void>;
  private handleMessageBatch: (message: Message[]) => Promise<void>;
  private handleMessageTimeout: number;
  private extraReceiveOptions: Record<string, unknown>;
  private stopped: boolean;
  private batchSize: number;
  private visibilityTimeout: number;
  private authenticationErrorTimeout: number;
  private pollingWaitTimeMs: number;
  private terminateVisibilityTimeout: boolean;
  private heartbeatInterval: number;
  private sqs: IQueueProvider;

  constructor(queueService: IQueueProvider, options: ConsumerOptions) {
    super();
    assertOptions(options);
    this.sqs = queueService;
    this.handleMessage = options.handleMessage;
    this.handleMessageBatch = options.handleMessageBatch;
    this.handleMessageTimeout = options.handleMessageTimeout;
    this.extraReceiveOptions = options.receiveOptions;
    this.stopped = true;
    this.batchSize = options.batchSize || 1;
    this.visibilityTimeout = options.visibilityTimeout;
    this.terminateVisibilityTimeout = options.terminateVisibilityTimeout || false;
    this.heartbeatInterval = options.heartbeatInterval;
    this.authenticationErrorTimeout = options.authenticationErrorTimeout || 10000;
    this.pollingWaitTimeMs = options.pollingWaitTimeMs || 10000;

    autoBind(this);
  }

  emit<T extends keyof Events>(event: T, ...args: Events[T]) {
    return super.emit(event, ...args);
  }

  on<T extends keyof Events>(event: T, listener: (...args: Events[T]) => void): this {
    return super.on(event, listener);
  }

  once<T extends keyof Events>(event: T, listener: (...args: Events[T]) => void): this {
    return super.once(event, listener);
  }

  public get isRunning(): boolean {
    return !this.stopped;
  }

  public static create(queueService: IQueueProvider, options: ConsumerOptions): Consumer {
    return new Consumer(queueService, options);
  }

  public start(): void {
    if (this.stopped) {
      debug('Starting consumer');
      this.stopped = false;
      this.poll();
    }
  }

  public stop(): void {
    debug('Stopping consumer');
    this.stopped = true;
  }

  private async handleSqsResponse(response: ReceiveMessageResult): Promise<void> {
    debug('Received SQS response');
    debug(response);

    if (response) {
      if (hasMessages(response)) {
        if (this.handleMessageBatch) {
          // prefer handling messages in batch when available
          await this.processMessageBatch(response.messages);
        } else {
          await Promise.all(response.messages.map(this.processMessage));
        }
        this.emit('response_processed');
      } else {
        this.emit('empty');
      }
    }
  }

  private async processMessage(message: Message): Promise<void> {
    this.emit('message_received', message);

    let heartbeat;
    try {
      if (this.heartbeatInterval) {
        heartbeat = this.startHeartbeat(async (elapsedSeconds) => {
          return this.changeVisabilityTimeout(message, elapsedSeconds + this.visibilityTimeout);
        });
      }
      await this.executeHandler(message);
      await this.sqs.deleteMessage(message);
      this.emit('message_processed', message);
    } catch (err) {
      this.emitError(err, message);

      if (this.terminateVisibilityTimeout) {
        await this.changeVisabilityTimeout(message, 0);
      }
    } finally {
      clearInterval(heartbeat);
    }
  }

  private async executeHandler(message: Message): Promise<void> {
    let timeout;
    let pending;
    try {
      if (this.handleMessageTimeout) {
        [timeout, pending] = createTimeout(this.handleMessageTimeout);
        await Promise.race([this.handleMessage(message), pending]);
      } else {
        await this.handleMessage(message);
      }
    } catch (err) {
      if (err instanceof TimeoutError) {
        err.message = `Message handler timed out after ${this.handleMessageTimeout}ms: Operation timed out.`;
      } else {
        err.message = `Unexpected message handler failure: ${err.message}`;
      }
      throw err;
    } finally {
      clearTimeout(timeout);
    }
  }

  private async changeVisabilityTimeout(message: Message, timeout: number): Promise<void> {
    try {
      return this.sqs.changeMessageVisibility(message, timeout);
    } catch (err) {
      this.emit('error', err, message);
    }
  }

  private emitError(err: Error, message: Message): void {
    if (err.name === SQSError.name) {
      this.emit('error', err, message);
    } else if (err instanceof TimeoutError) {
      this.emit('timeout_error', err, message);
    } else {
      this.emit('processing_error', err, message);
    }
  }

  private poll(): void {
    if (this.stopped) {
      this.emit('stopped');
      return;
    }

    debug('Polling for messages');
    const receiveOptions: ReceiveMessageOptions = {
      maxNumberOfMessages: this.batchSize,
      visibilityTimeout: this.visibilityTimeout,
      extraOptions: this.extraReceiveOptions
    };

    let currentPollingTimeout = this.pollingWaitTimeMs;
    this.sqs
      .receiveMessage(receiveOptions)
      .then(this.handleSqsResponse)
      .catch((err) => {
        this.emit('error', err);
        if (isConnectionError(err)) {
          debug('There was an authentication error. Pausing before retrying.');
          currentPollingTimeout = this.authenticationErrorTimeout;
        }
        return;
      })
      .then(() => {
        setTimeout(this.poll, currentPollingTimeout);
      })
      .catch((err) => {
        this.emit('error', err);
      });
  }

  private async processMessageBatch(messages: MessageList): Promise<void> {
    messages.forEach((message) => {
      this.emit('message_received', message);
    });

    let heartbeat;
    try {
      if (this.heartbeatInterval) {
        heartbeat = this.startHeartbeat(async (elapsedSeconds) => {
          return this.changeVisibilityTimeoutBatch(messages, elapsedSeconds + this.visibilityTimeout);
        });
      }
      await this.executeBatchHandler(messages);
      await this.sqs.deleteMessageBatch(messages);
      messages.forEach((message) => {
        this.emit('message_processed', message);
      });
    } catch (err) {
      this.emit('error', err, messages);

      if (this.terminateVisibilityTimeout) {
        await this.changeVisibilityTimeoutBatch(messages, 0);
      }
    } finally {
      clearInterval(heartbeat);
    }
  }

  private async executeBatchHandler(messages: Message[]): Promise<void> {
    try {
      await this.handleMessageBatch(messages);
    } catch (err) {
      err.message = `Unexpected message handler failure: ${err.message}`;
      throw err;
    }
  }

  private async changeVisibilityTimeoutBatch(messages: Message[], timeout: number): Promise<void> {
    try {
      return this.sqs.changeMessageVisibilityBatch(messages, timeout);
    } catch (err) {
      this.emit('error', err, messages);
    }
  }

  private startHeartbeat(heartbeatFn: (elapsedSeconds: number) => void): NodeJS.Timeout {
    const startTime = Date.now();
    return setInterval(() => {
      const elapsedSeconds = Math.ceil((Date.now() - startTime) / 1000);
      heartbeatFn(elapsedSeconds);
    }, this.heartbeatInterval * 1000);
  }
}
