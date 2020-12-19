export declare type ProducerOptions = {
    serviceName: string;
    username?: string;
    password?: string;
    host?: string;
    port?: number;
};
export declare type ConsumerOptions = {
    serviceName: string;
    exchangeService: string;
    username?: string;
    password?: string;
    host?: string;
    port?: number;
    waitQueueTtl?: number;
    maxRetry?: number;
};
export declare type QueueArguments = {
    'x-dead-letter-exchange'?: string;
    'x-dead-letter-routing-key'?: string;
    'x-message-ttl'?: number;
};
//# sourceMappingURL=types.d.ts.map