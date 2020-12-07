import { ProducerOptions } from './types';
export declare const createProducer: <T>(options: ProducerOptions) => <J extends keyof T>(exchange: J, params: Pick<T, J>[J]) => Promise<void>;
//# sourceMappingURL=producer.d.ts.map