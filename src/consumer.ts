import * as amqp from 'amqplib'
import { ConsumeMessage } from 'amqplib'
import { ConsumerOptions } from './types'

const consumerDefaultOptions: Required<ConsumerOptions> = {
  username: 'guest',
  password: 'guest',
  host: 'localhost',
  port: 5672,
  maxDelay: 15 * 60 * 1000,
  defaultDelay: 1 * 60 * 1000,
  delayStep: 3 * 60 * 1000,
  exchangeService: '',
  serviceName: '',
}

type IConsumerHandler<T, K extends keyof T> = (params: Pick<T, K>[K]) => any
type IConsumerDeclarationType<T> = { [K in keyof T]?: IConsumerHandler<T, K> }

export const createConsumer = <T>(consumerDeclaration: IConsumerDeclarationType<T>, options?: ConsumerOptions) => async (): Promise<void> => {
  const exchanges = Object.keys(consumerDeclaration) as (keyof T)[]
  const arr = exchanges.map((exchange) => {
    const handler = consumerDeclaration[exchange]
    return consumer(exchange.toString(), handler, options)
  })

  await Promise.all(arr)
  return
}

const consumer = async <T extends string, J>(exchange: T, externalFn: J, options?: ConsumerOptions) => {
  const consumerOptions: Required<ConsumerOptions> = { ...consumerDefaultOptions, ...options }
  const conn = await amqp.connect(`amqp://${consumerOptions.username}:${consumerOptions.password}@${consumerOptions.host}:${consumerOptions.port}/`)
  const channel = await conn.createChannel()
  try {
    const exchangeName = `${consumerOptions.exchangeService}.${exchange}`
    await channel.assertExchange(exchangeName, 'fanout', { durable: true })

    const queueName = `${consumerOptions.serviceName}.${exchange}`
    const queue = await channel.assertQueue(queueName, { durable: true })
    await channel.bindQueue(queue.queue, exchangeName, '')

    await channel.consume(queueName, consumerHandlerWrapper(channel, exchangeName, queueName, consumerOptions, externalFn))
  } catch (err) {
    console.error(err)
  }
}

const consumerHandlerWrapper = (channel: amqp.Channel, exchangeName: string, queueName: string, options: Required<ConsumerOptions>, handler: any) => async (msg: ConsumeMessage | null) => {
  if (!msg) return

  const content = JSON.parse(Buffer.from(msg.content).toString())

  try {
    await handler(content)
    channel.ack(msg)
  } catch (err) {
    console.log(msg)
    if (msg.fields.redelivered || msg.properties.headers['x-delay']) {
      return await publishDelayedQueue(channel, exchangeName, queueName, options, msg)
    }
    return channel.reject(msg, true)
  }
}

const publishDelayedQueue = async (channel: amqp.Channel, exchangeName: string, queueName: string, options: Required<ConsumerOptions>, msg: ConsumeMessage) => {
  const delayInMillisecond = getDelayInMillisecond(msg.properties.headers['x-delay'], options)
  const delayedExchangeName = `dlx.${exchangeName}`
  await channel.assertExchange(delayedExchangeName, 'x-delayed-message', { durable: true, arguments: { 'x-delayed-type': 'fanout' } })
  await channel.bindQueue(queueName, exchangeName, '')
  channel.publish(delayedExchangeName, '', msg.content, { headers: { 'x-delay': delayInMillisecond }, persistent: true })
  channel.ack(msg)
}

const getDelayInMillisecond = (currentDelay: number | undefined, options: Required<ConsumerOptions>) => {
  if (!currentDelay) return options?.defaultDelay
  if (currentDelay <= options.maxDelay) return options.maxDelay
  return currentDelay + options.delayStep
}