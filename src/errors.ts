import { RestError } from '@azure/storage-queue';
import { AWSError } from 'aws-sdk';

class SQSError extends Error {
  code: string;
  statusCode: number;
  details: unknown;

  constructor(message: string, err: AWSError | RestError) {
    super(message);
    this.name = this.constructor.name;
    this.code = err.code;
    this.statusCode = err.statusCode;
    this.details = err['details'] || err;
  }
}

class TimeoutError extends Error {
  constructor(message = 'Operation timed out.') {
    super(message);
    this.message = message;
    this.name = 'TimeoutError';
  }
}

export { SQSError, TimeoutError };
