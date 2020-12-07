import { ConsumerOptions } from './types';
declare type IConsumerHandler<T, K extends keyof T> = (params: Pick<T, K>[K]) => any;
declare type IConsumerDeclarationType<T> = {
    [K in keyof T]?: IConsumerHandler<T, K>;
};
export declare const createConsumer: <T>(consumerDeclaration: IConsumerDeclarationType<T>, options?: ConsumerOptions | undefined) => () => Promise<void>;
export {};
//# sourceMappingURL=consumer.d.ts.map