import { assert } from 'chai';
import * as pEvent from 'p-event';

import * as sinon from 'sinon';
import { Consumer, AzureQueueProvider } from '../src/index';

const sandbox = sinon.createSandbox();
const STORAGE_QUEUE_CONNECTIONSTRING =
  'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://azurite:10001/devstoreaccount1;';

function stubResolve(value?: any): any {
  return sandbox.stub().returns(Promise.resolve(value));
}

function stubReject(value?: any): any {
  return sandbox.stub().returns(Promise.reject(value));
}

describe('Azure', () => {
  let consumer;
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
      authenticationErrorTimeout: 20
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
  });
});
