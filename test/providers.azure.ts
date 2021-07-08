import { assert } from 'chai';
import * as pEvent from 'p-event';

import * as sinon from 'sinon';
import { Consumer, AzureQueueProvider } from '../src/index';

const sandbox = sinon.createSandbox();

const AUTHENTICATION_ERROR_TIMEOUT = 20;

const STORAGE_QUEUE_CONNECTIONSTRING =
  'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://azurite:10001/devstoreaccount1;';

function stubResolve(value?: any): any {
  return sandbox.stub().returns(Promise.resolve(value));
}

function stubReject(value?: any): any {
  return sandbox.stub().returns(Promise.reject(value));
}

class MockRestError extends Error {
  code: string;
  statusCode: number;
  details: unknown;

  constructor(message: string) {
    super(message);
    this.message = message;
  }
}

describe('Azure', () => {
  let consumer;
  let clock;
  let handleMessage;
  let azureQueueProvider;
  const response = {
    receivedMessageItems: [
      {
        popReceipt: 'receipt-handle',
        messageId: '123',
        messageText: 'body'
      }
    ]
  };

  beforeEach(() => {
    clock = sinon.useFakeTimers();
    handleMessage = sandbox.stub().resolves(null);
    azureQueueProvider = new AzureQueueProvider('some-queue-url', {
      connectionString: STORAGE_QUEUE_CONNECTIONSTRING
    });

    const sqs = sandbox.mock() as any;
    sqs.receiveMessages = stubResolve(response);
    sqs.deleteMessage = stubResolve();
    sqs.deleteMessageBatch = stubResolve();
    sqs.changeMessageVisibility = stubResolve();
    sqs.changeMessageVisibilityBatch = stubResolve();

    azureQueueProvider['sqs'] = sqs;

    consumer = new Consumer(azureQueueProvider, {
      handleMessage,
      authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT
    });
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('requires a queueUrl to be set', () => {
    assert.throws(() => {
      new AzureQueueProvider(undefined, { connectionString: STORAGE_QUEUE_CONNECTIONSTRING });
    });
  });

  it('requires a connectionString or instance', () => {
    assert.throws(() => {
      new AzureQueueProvider('some-queue-url', undefined);
    });
  });

  describe('.start', () => {
    it('fires an error event when an error occurs receiving a message', async () => {
      const receiveErr = new Error('Receive error');

      azureQueueProvider['sqs'].receiveMessages = stubReject(receiveErr);

      consumer.start();

      const err: any = await pEvent(consumer, 'error');

      consumer.stop();
      assert.ok(err);
      assert.equal(err.message, 'SQS receive message failed: Receive error');
    });

    it('retains rest error information', async () => {
      const receiveErr = new MockRestError('Receive error');
      receiveErr.code = 'short code';
      receiveErr.statusCode = 403;
      receiveErr.details = {
        errorCode: 'AuthorizationFailure',
        connection: 'keep-alive',
        'content-type': 'application/xml',
        date: 'Thu, 08 Jul 2021 00:47:41 GMT',
        'keep-alive': 'timeout=5',
        server: 'Azurite-Queue/3.13.1',
        'transfer-encoding': 'chunked',
        'x-ms-request-id': 'd920b198-fc97-4284-8023-249fbeb7cc92',
        'x-ms-version': '2020-08-04',
        message:
          'Server failed to authenticate the request.Make sure the value of the Authorization header is formed correctly including the signature.\nRequestId:d920b198-fc97-4284-8023-249fbeb7cc92\nTime:2021-07-08T00:47:41.998Z',
        code: 'AuthorizationFailure'
      };

      azureQueueProvider['sqs'].receiveMessages = stubReject(receiveErr);

      consumer.start();
      const err: any = await pEvent(consumer, 'error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'SQS receive message failed: Receive error');
      assert.equal(err.code, receiveErr.code);
      assert.equal(err.statusCode, receiveErr.statusCode);
      assert.equal(err.details.errorCode, receiveErr.details['errorCode']);
      assert.equal(err.details.connection, receiveErr.details['connection']);
      assert.equal(err.details.date, receiveErr.details['date']);
      assert.equal(err.details['keep-alive'], receiveErr.details['keep-alive']);
    });

    it('fires an `error` event when an `RestError` occurs processing a message', async () => {
      const sqsError = new Error('Processing error');
      sqsError.name = 'SQSError';

      handleMessage.resolves(sqsError);
      azureQueueProvider['sqs'].deleteMessage = stubReject(sqsError);

      consumer.start();
      const [err, message] = await pEvent(consumer, 'error', { multiArgs: true });
      consumer.stop();

      assert.equal(err.message, 'SQS delete message failed: Processing error');
      assert.equal(message.messageId, '123');
    });

    it('waits before repolling when a credentials error occurs', async () => {
      const credentialsErr = { message: 'Server failed to authenticate', code: 'AuthorizationFailure' };

      azureQueueProvider['sqs'].receiveMessages = stubReject(credentialsErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(azureQueueProvider['sqs'].receiveMessages);
    });

    it('waits before repolling when a 403 error occurs', async () => {
      const invalidSignatureErr = {
        statusCode: 403,
        message: 'The security token included in the request is invalid'
      };

      azureQueueProvider['sqs'].receiveMessages = stubReject(invalidSignatureErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(azureQueueProvider['sqs'].receiveMessages);
    });

    it('waits before repolling when a QueueNotFound error occurs', async () => {
      const unknownEndpointErr = {
        code: 'QueueNotFound',
        message: 'The specified queue does not exist'
      };
      azureQueueProvider['sqs'].receiveMessages = stubReject(unknownEndpointErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(azureQueueProvider['sqs'].receiveMessages);
    });
  });
});
