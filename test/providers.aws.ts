import { SQS } from 'aws-sdk';
import { assert } from 'chai';
import * as pEvent from 'p-event';

import * as sinon from 'sinon';
import { Consumer, AwsQueueProvider } from '../src/index';

const sandbox = sinon.createSandbox();

const AUTHENTICATION_ERROR_TIMEOUT = 20;

function stubResolve(value?: any): any {
  return sandbox.stub().returns({ promise: sandbox.stub().resolves(value) });
}

function stubReject(value?: any): any {
  return sandbox.stub().returns({ promise: sandbox.stub().rejects(value) });
}

class MockSQSError extends Error {
  code: string;
  statusCode: number;
  region: string;
  hostname: string;
  time: Date;
  retryable: boolean;

  constructor(message: string) {
    super(message);
    this.message = message;
  }
}

describe('Aws', () => {
  let consumer;
  let clock;
  let handleMessage;
  let awsQueueProvider;
  const response = {
    Messages: [
      {
        ReceiptHandle: 'receipt-handle',
        MessageId: '123',
        Body: 'body'
      }
    ]
  };

  beforeEach(() => {
    clock = sinon.useFakeTimers();
    handleMessage = sandbox.stub().resolves(null);
    awsQueueProvider = new AwsQueueProvider('some-queue-url', { region: 'region' });

    const sqs = sandbox.mock() as any;
    sqs.receiveMessage = stubResolve(response);
    sqs.deleteMessage = stubResolve();
    sqs.deleteMessageBatch = stubResolve();
    sqs.changeMessageVisibility = stubResolve();
    sqs.changeMessageVisibilityBatch = stubResolve();

    awsQueueProvider['sqs'] = sqs;

    consumer = new Consumer(awsQueueProvider, {
      handleMessage,
      authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT
    });
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('requires a queueUrl to be set', () => {
    assert.throws(() => {
      new AwsQueueProvider(undefined);
    });
  });

  describe('.start', () => {
    it('fires an error event when an error occurs receiving a message', async () => {
      const receiveErr = new Error('Receive error');

      awsQueueProvider['sqs'].receiveMessage = stubReject(receiveErr);

      consumer.start();

      const err: any = await pEvent(consumer, 'error');

      consumer.stop();
      assert.ok(err);
      assert.equal(err.message, 'SQS receive message failed: Receive error');
    });

    it('retains sqs error information', async () => {
      const receiveErr = new MockSQSError('Receive error');
      receiveErr.code = 'short code';
      receiveErr.retryable = false;
      receiveErr.statusCode = 403;
      receiveErr.time = new Date();
      receiveErr.hostname = 'hostname';
      receiveErr.region = 'eu-west-1';

      awsQueueProvider['sqs'].receiveMessage = stubReject(receiveErr);

      consumer.start();
      const err: any = await pEvent(consumer, 'error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'SQS receive message failed: Receive error');
      assert.equal(err.code, receiveErr.code);
      assert.equal(err.statusCode, receiveErr.statusCode);

      assert.equal(err.details.retryable, receiveErr.retryable);
      assert.equal(err.details.time, receiveErr.time);
      assert.equal(err.details.hostname, receiveErr.hostname);
      assert.equal(err.details.region, receiveErr.region);
    });

    it('fires an error event when an error occurs deleting a message', async () => {
      const deleteErr = new Error('Delete error');

      handleMessage.resolves(null);
      awsQueueProvider['sqs'].deleteMessage = stubReject(deleteErr);

      consumer.start();
      const err: any = await pEvent(consumer, 'error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'SQS delete message failed: Delete error');
    });

    it('fires an `error` event when an `SQSError` occurs processing a message', async () => {
      const sqsError = new Error('Processing error');
      sqsError.name = 'SQSError';

      handleMessage.resolves(sqsError);
      awsQueueProvider['sqs'].deleteMessage = stubReject(sqsError);

      consumer.start();
      const [err, message] = await pEvent(consumer, 'error', { multiArgs: true });
      consumer.stop();

      assert.equal(err.message, 'SQS delete message failed: Processing error');
      assert.equal(message.messageId, '123');
    });

    it('waits before repolling when a credentials error occurs', async () => {
      const credentialsErr = {
        code: 'CredentialsError',
        message: 'Missing credentials in config'
      };
      awsQueueProvider['sqs'].receiveMessage = stubReject(credentialsErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(awsQueueProvider['sqs'].receiveMessage);
    });

    it('waits before repolling when a 403 error occurs', async () => {
      const invalidSignatureErr = {
        statusCode: 403,
        message: 'The security token included in the request is invalid'
      };
      awsQueueProvider['sqs'].receiveMessage = stubReject(invalidSignatureErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(awsQueueProvider['sqs'].receiveMessage);
    });

    it('waits before repolling when a UnknownEndpoint error occurs', async () => {
      const unknownEndpointErr = {
        code: 'UnknownEndpoint',
        message:
          'Inaccessible host: `sqs.eu-west-1.amazonaws.com`. This service may not be available in the `eu-west-1` region.'
      };
      awsQueueProvider['sqs'].receiveMessage = stubReject(unknownEndpointErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(awsQueueProvider['sqs'].receiveMessage);
    });

    it("consumes messages with message attribute 'ApproximateReceiveCount'", async () => {
      const messageWithAttr: any = {
        ReceiptHandle: 'receipt-handle-1',
        MessageId: '1',
        Body: 'body-1',
        Attributes: {
          ApproximateReceiveCount: 1
        }
      };

      awsQueueProvider['sqs'].receiveMessage = stubResolve({
        Messages: [messageWithAttr]
      });

      consumer = new Consumer(awsQueueProvider, {
        receiveOptions: { AttributeNames: ['ApproximateReceiveCount'] },
        handleMessage
      });

      consumer.start();
      const message = await pEvent(consumer, 'message_received');
      consumer.stop();

      const options: SQS.ReceiveMessageRequest = {
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20,
        QueueUrl: 'some-queue-url',
        VisibilityTimeout: undefined,
        AttributeNames: ['ApproximateReceiveCount']
      };

      sandbox.assert.calledWith(awsQueueProvider['sqs'].receiveMessage, options);

      const expectedReponse = {
        messageId: '1',
        body: 'body-1',
        receiptHandle: 'receipt-handle-1',
        extraFields: {
          Attributes: {
            ApproximateReceiveCount: 1
          }
        }
      };

      assert.deepEqual(message, expectedReponse);
    });
  });
});
