export type PublisherOptions = {
  serviceName: string
  username?: string
  password?: string
  host?: string
  port?: number
}

export type SubscriptionOptions = {
  serviceName: string
  exchangeService: string
  username?: string
  password?: string
  host?: string
  port?: number
  waitQueueTtl?: number
  maxRetry?: number
}
