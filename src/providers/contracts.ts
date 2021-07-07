export interface MessageIdentification {
  messageId: string;
  receiptHandle: string;
}

export interface Message extends MessageIdentification {
  body?: string;
}

export type MessageList = Message[];

export interface ReceiveMessageResult {
  messages: MessageList;
}

export interface ReceiveMessageOptions {
  visibilityTimeout?: number;
  waitTimeout: number;
  maxNumberOfMessages: number;
}

export interface IQueueProvider {
  receiveMessage(options?: ReceiveMessageOptions): Promise<ReceiveMessageResult>;
  deleteMessage(message: MessageIdentification): Promise<void>;
  deleteMessageBatch(messages: MessageIdentification[]): Promise<void>;
  changeMessageVisibility(message: MessageIdentification, timeout: number): Promise<void>;
  changeMessageVisibilityBatch(messages: MessageIdentification[], timeout: number): Promise<void>;
}
