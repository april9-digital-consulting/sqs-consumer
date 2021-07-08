import { assert } from 'chai';
import * as pEvent from 'p-event';

import * as sinon from 'sinon';
import { Consumer } from '../src/index';
import { MessageList, ReceiveMessageOptions, ReceiveMessageResult } from '../src/providers/contracts';

const sandbox = sinon.createSandbox();

const POLLING_TIMEOUT = 100;

function stubResolve(value?: any): any {
  return sandbox.stub().returns(Promise.resolve(value));
}

function stubReject(value?: any): any {
  return sandbox.stub().returns(Promise.reject(value));
}

describe('Consumer', () => {
  let consumer;
  let clock;
  let handleMessage;
  let handleMessageBatch;
  let sqs;
  const response: ReceiveMessageResult = {
    messages: [
      {
        receiptHandle: 'receipt-handle',
        messageId: '123',
        body: 'body'
      }
    ]
  };

  beforeEach(() => {
    clock = sinon.useFakeTimers();
    handleMessage = sandbox.stub().resolves(null);
    handleMessageBatch = sandbox.stub().resolves(null);
    sqs = sandbox.mock();
    sqs.queueUrl = 'some-queue-url';
    sqs.receiveMessage = stubResolve(response);
    sqs.deleteMessage = stubResolve();
    sqs.deleteMessageBatch = stubResolve();
    sqs.changeMessageVisibility = stubResolve();
    sqs.changeMessageVisibilityBatch = stubResolve();

    consumer = new Consumer(sqs, {
      handleMessage,
      authenticationErrorTimeout: 20
    });
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('requires a handleMessage or handleMessagesBatch function to be set', () => {
    assert.throws(() => {
      new Consumer(sqs, {
        handleMessage: undefined
      });
    });
  });

  it('requires the batchSize option to be no greater than 10', () => {
    assert.throws(() => {
      new Consumer(sqs, {
        handleMessage,
        batchSize: 11
      });
    });
  });

  it('requires the batchSize option to be greater than 0', () => {
    assert.throws(() => {
      new Consumer(sqs, {
        handleMessage,
        batchSize: -1
      });
    });
  });

  it('requires visibilityTimeout to be set with heartbeatInterval', () => {
    assert.throws(() => {
      new Consumer(sqs, {
        handleMessage,
        heartbeatInterval: 30
      });
    });
  });

  it('requires heartbeatInterval to be less than visibilityTimeout', () => {
    assert.throws(() => {
      new Consumer(sqs, {
        handleMessage,
        heartbeatInterval: 30,
        visibilityTimeout: 30
      });
    });
  });

  describe('.create', () => {
    it('creates a new instance of a Consumer object', () => {
      const instance = Consumer.create(sqs, {
        batchSize: 1,
        visibilityTimeout: 10,
        waitTimeSeconds: 10,
        handleMessage
      });

      assert.instanceOf(instance, Consumer);
    });
  });

  describe('.start', () => {
    it('fires a timeout event if handler function takes too long', async () => {
      const handleMessageTimeout = 500;
      consumer = new Consumer(sqs, {
        handleMessage: () => new Promise((resolve) => setTimeout(resolve, 1000)),
        handleMessageTimeout,
        authenticationErrorTimeout: 20
      });

      consumer.start();
      const [err]: any = await Promise.all([
        pEvent(consumer, 'timeout_error'),
        clock.tickAsync(handleMessageTimeout)
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(
        err.message,
        `Message handler timed out after ${handleMessageTimeout}ms: Operation timed out.`
      );
    });

    it('handles unexpected exceptions thrown by the handler function', async () => {
      consumer = new Consumer(sqs, {
        handleMessage: () => {
          throw new Error('unexpected parsing error');
        },
        authenticationErrorTimeout: 20
      });

      consumer.start();
      const err: any = await pEvent(consumer, 'processing_error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'Unexpected message handler failure: unexpected parsing error');
    });

    it('fires a `processing_error` event when a non-`SQSError` error occurs processing a message', async () => {
      const processingErr = new Error('Processing error');

      handleMessage.rejects(processingErr);

      consumer.start();
      const [err, message] = await pEvent(consumer, 'processing_error', { multiArgs: true });
      consumer.stop();

      assert.equal(err.message, 'Unexpected message handler failure: Processing error');
      assert.equal(message.messageId, '123');
    });

    it('waits before repolling when a polling timeout is set', async () => {
      consumer = new Consumer(sqs, {
        handleMessage,
        authenticationErrorTimeout: 20,
        pollingWaitTimeMs: 100
      });

      consumer.start();
      await clock.tickAsync(POLLING_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(sqs.receiveMessage);
    });

    it('fires a message_received event when a message is received', async () => {
      consumer.start();
      const message = await pEvent(consumer, 'message_received');
      consumer.stop();

      assert.equal(message, response.messages[0]);
    });

    it('fires a message_processed event when a message is successfully deleted', async () => {
      handleMessage.resolves();

      consumer.start();
      const message = await pEvent(consumer, 'message_received');
      consumer.stop();

      assert.equal(message, response.messages[0]);
    });

    it('calls the handleMessage function when a message is received', async () => {
      consumer.start();
      await pEvent(consumer, 'message_processed');
      consumer.stop();

      sandbox.assert.calledWith(handleMessage, response.messages[0]);
    });

    it('deletes the message when the handleMessage function is called', async () => {
      handleMessage.resolves();

      consumer.start();
      await pEvent(consumer, 'message_processed');
      consumer.stop();

      sandbox.assert.calledWith(sqs.deleteMessage, response.messages[0]);
    });

    it("doesn't delete the message when a processing error is reported", async () => {
      handleMessage.rejects(new Error('Processing error'));

      consumer.start();
      await pEvent(consumer, 'processing_error');
      consumer.stop();

      sandbox.assert.notCalled(sqs.deleteMessage);
    });

    it('consumes another message once one is processed', async () => {
      handleMessage.resolves();

      consumer.start();
      await clock.runToLastAsync();
      consumer.stop();

      sandbox.assert.calledTwice(handleMessage);
    });

    it("doesn't consume more messages when called multiple times", () => {
      sqs.receiveMessage = stubResolve(new Promise((res) => setTimeout(res, 100)));
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.stop();

      sandbox.assert.calledOnce(sqs.receiveMessage);
    });

    it('consumes multiple messages when the batchSize is greater than 1', async () => {
      const messages: MessageList = [
        {
          receiptHandle: 'receipt-handle-1',
          messageId: '1',
          body: 'body-1',
          extraFields: undefined
        },
        {
          receiptHandle: 'receipt-handle-2',
          messageId: '2',
          body: 'body-2',
          extraFields: undefined
        },
        {
          receiptHandle: 'receipt-handle-3',
          messageId: '3',
          body: 'body-3',
          extraFields: undefined
        }
      ];

      sqs.receiveMessage = stubResolve({
        messages
      });

      consumer = new Consumer(sqs, {
        receiveOptions: { MessageAttributeNames: ['attribute-1', 'attribute-2'] },
        handleMessage,
        batchSize: 3
      });

      consumer.start();

      return new Promise((resolve) => {
        handleMessage.onThirdCall().callsFake(() => {
          const options: ReceiveMessageOptions = {
            maxNumberOfMessages: 3,
            waitTimeout: 20,
            visibilityTimeout: undefined,
            extraOptions: { MessageAttributeNames: ['attribute-1', 'attribute-2'] }
          };

          sandbox.assert.calledWith(sqs.receiveMessage, options);
          sandbox.assert.callCount(handleMessage, 3);
          consumer.stop();
          resolve();
        });
      });
    });

    it('fires an emptyQueue event when all messages have been consumed', async () => {
      sqs.receiveMessage = stubResolve({});

      consumer.start();
      await pEvent(consumer, 'empty');
      consumer.stop();
    });

    it('terminate message visibility timeout on processing error', async () => {
      handleMessage.rejects(new Error('Processing error'));

      consumer.terminateVisibilityTimeout = true;

      consumer.start();
      await pEvent(consumer, 'processing_error');
      consumer.stop();

      sandbox.assert.calledWith(sqs.changeMessageVisibility, response.messages[0], 0);
    });

    it('does not terminate visibility timeout when `terminateVisibilityTimeout` option is false', async () => {
      handleMessage.rejects(new Error('Processing error'));
      consumer.terminateVisibilityTimeout = false;

      consumer.start();
      await pEvent(consumer, 'processing_error');
      consumer.stop();

      sandbox.assert.notCalled(sqs.changeMessageVisibility);
    });

    it('fires error event when failed to terminate visibility timeout on processing error', async () => {
      handleMessage.rejects(new Error('Processing error'));

      const sqsError = new Error('Processing error');
      sqsError.name = 'SQSError';
      sqs.changeMessageVisibility = stubReject(sqsError);
      consumer.terminateVisibilityTimeout = true;

      consumer.start();
      await pEvent(consumer, 'error');
      consumer.stop();

      sandbox.assert.calledWith(sqs.changeMessageVisibility, response.messages[0], 0);
    });

    it('fires response_processed event for each batch', async () => {
      const messages: MessageList = [
        {
          receiptHandle: 'receipt-handle-1',
          messageId: '1',
          body: 'body-1'
        },
        {
          receiptHandle: 'receipt-handle-2',
          messageId: '2',
          body: 'body-2'
        }
      ];

      sqs.receiveMessage = stubResolve({
        messages
      });

      handleMessage.resolves(null);

      consumer = new Consumer(sqs, {
        receiveOptions: { messageAttributeNames: ['attribute-1', 'attribute-2'] },
        handleMessage,
        batchSize: 2
      });

      consumer.start();
      await pEvent(consumer, 'response_processed');
      consumer.stop();

      sandbox.assert.callCount(handleMessage, 2);
    });

    it('calls the handleMessagesBatch function when a batch of messages is received', async () => {
      consumer = new Consumer(sqs, {
        receiveOptions: { messageAttributeNames: ['attribute-1', 'attribute-2'] },
        handleMessageBatch,
        batchSize: 2
      });

      consumer.start();
      await pEvent(consumer, 'response_processed');
      consumer.stop();

      sandbox.assert.callCount(handleMessageBatch, 1);
    });

    it('prefers handleMessagesBatch over handleMessage when both are set', async () => {
      consumer = new Consumer(sqs, {
        receiveOptions: { messageAttributeNames: ['attribute-1', 'attribute-2'] },
        handleMessageBatch,
        handleMessage,
        batchSize: 2
      });

      consumer.start();
      await pEvent(consumer, 'response_processed');
      consumer.stop();

      sandbox.assert.callCount(handleMessageBatch, 1);
      sandbox.assert.callCount(handleMessage, 0);
    });

    it('extends visibility timeout for long running handler functions', async () => {
      consumer = new Consumer(sqs, {
        handleMessage: () => new Promise((resolve) => setTimeout(resolve, 75000)),
        visibilityTimeout: 40,
        heartbeatInterval: 30
      });
      const clearIntervalSpy = sinon.spy(global, 'clearInterval');

      consumer.start();
      await Promise.all([pEvent(consumer, 'response_processed'), clock.tickAsync(75000)]);
      consumer.stop();

      sandbox.assert.calledWith(sqs.changeMessageVisibility, response.messages[0], 70);
      sandbox.assert.calledWith(sqs.changeMessageVisibility, response.messages[0], 100);
      sandbox.assert.calledOnce(clearIntervalSpy);
    });

    it('extends visibility timeout for long running batch handler functions', async () => {
      const messages: MessageList = [
        { messageId: '1', receiptHandle: 'receipt-handle-1', body: 'body-1' },
        { messageId: '2', receiptHandle: 'receipt-handle-2', body: 'body-2' },
        { messageId: '3', receiptHandle: 'receipt-handle-3', body: 'body-3' }
      ];

      sqs.receiveMessage = stubResolve({
        messages
      });

      consumer = new Consumer(sqs, {
        handleMessageBatch: () => new Promise((resolve) => setTimeout(resolve, 75000)),
        batchSize: 3,
        visibilityTimeout: 40,
        heartbeatInterval: 30
      });
      const clearIntervalSpy = sinon.spy(global, 'clearInterval');

      consumer.start();
      await Promise.all([pEvent(consumer, 'response_processed'), clock.tickAsync(75000)]);
      consumer.stop();

      sandbox.assert.calledWith(sqs.changeMessageVisibilityBatch, messages, 70);
      sandbox.assert.calledWith(sqs.changeMessageVisibilityBatch, messages, 100);
      sandbox.assert.calledOnce(clearIntervalSpy);
    });
  });

  describe('.stop', () => {
    it('stops the consumer polling for messages', async () => {
      consumer.start();
      consumer.stop();

      await Promise.all([pEvent(consumer, 'stopped'), clock.runAllAsync()]);

      sandbox.assert.calledOnce(handleMessage);
    });

    it('fires a stopped event when last poll occurs after stopping', async () => {
      consumer.start();
      consumer.stop();
      await Promise.all([pEvent(consumer, 'stopped'), clock.runAllAsync()]);
    });

    it('fires a stopped event only once when stopped multiple times', async () => {
      const handleStop = sandbox.stub().returns(null);

      consumer.on('stopped', handleStop);

      consumer.start();
      consumer.stop();
      consumer.stop();
      consumer.stop();
      await clock.runAllAsync();

      sandbox.assert.calledOnce(handleStop);
    });

    it('fires a stopped event a second time if started and stopped twice', async () => {
      const handleStop = sandbox.stub().returns(null);

      consumer.on('stopped', handleStop);

      consumer.start();
      consumer.stop();
      consumer.start();
      consumer.stop();
      await clock.runAllAsync();

      sandbox.assert.calledTwice(handleStop);
    });
  });

  describe('isRunning', async () => {
    it('returns true if the consumer is polling', () => {
      consumer.start();
      assert.isTrue(consumer.isRunning);
      consumer.stop();
    });

    it('returns false if the consumer is not polling', () => {
      consumer.start();
      consumer.stop();
      assert.isFalse(consumer.isRunning);
    });
  });
});
