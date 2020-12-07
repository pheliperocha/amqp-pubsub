export type ProducerOptions = {
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
  maxDelay?: number
  delayStep?: number
  defaultDelay?: number
}
