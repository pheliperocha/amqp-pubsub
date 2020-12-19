import { ProducerOptions } from './types';
export declare const createProducer: <T>(exchanges: (keyof T)[], options: ProducerOptions) => {
    producer: <J extends keyof T>(exchange: J, params: Pick<T, J>[J]) => Promise<void>;
    defineExchanges: () => Promise<void>;
};
//# sourceMappingURL=producer.d.ts.map