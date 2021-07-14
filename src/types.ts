export type PublisherOptions = {
  serviceName: string
  username?: string
  password?: string
  host?: string
  port?: number
}

export type ConsumerOptions = {
  serviceName: string
  exchangeService: string
  username?: string
  password?: string
  host?: string
  port?: number
  waitQueueTtl?: number
  maxRetry?: number
}

export type QueueArguments = {
  'x-dead-letter-exchange'?: string,
  'x-dead-letter-routing-key'?: string,
  'x-message-ttl'?: number
}