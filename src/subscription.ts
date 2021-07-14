import * as amqp from 'amqplib'
import { ConsumeMessage } from 'amqplib'
import { SubscriptionOptions, QueueArguments } from './types'
import { capitalizeWord } from './utils'

const subscriptionDefaultOptions: Required<SubscriptionOptions> = {
  username: 'guest',
  password: 'guest',
  host: 'localhost',
  port: 5672,
  waitQueueTtl: 1 * 60 * 1000,
  maxRetry: 10,
  exchangeService: '',
  serviceName: '',
}

export type IParsedConsumeMessage<T = any> = ConsumeMessage & { content: T }
type ISubscriptionHandler<T, K extends keyof T> = (params: IParsedConsumeMessage<Pick<T, K>[K]>) => any
type ISubscriptionDeclarationType<T> = { [K in keyof T]?: ISubscriptionHandler<T, K> }

export const createSubscription = <T>(subscriptionDeclaration: ISubscriptionDeclarationType<T>, options?: SubscriptionOptions) => async (): Promise<void> => {
  const exchanges = Object.keys(subscriptionDeclaration) as (keyof T)[]
  const arr = exchanges.map((exchange) => {
    const handler = subscriptionDeclaration[exchange]
    return subscription(exchange.toString(), handler, options)
  })

  await Promise.all(arr)
  return
}

const subscription = async <T extends string, J>(exchange: T, externalFn: J, options?: SubscriptionOptions) => {
  const subscriptionOptions: Required<SubscriptionOptions> = { ...subscriptionDefaultOptions, ...options }
  const conn = await amqp.connect(`amqp://${subscriptionOptions.username}:${subscriptionOptions.password}@${subscriptionOptions.host}:${subscriptionOptions.port}/`)
  const channel = await conn.createChannel()
  try {
    const exchangeName = `${subscriptionOptions.exchangeService}.${exchange}`

    const queueName = `${subscriptionOptions.serviceName}.${subscriptionOptions.exchangeService}${capitalizeWord(exchange)}`
    await assertAndBindingQueues(channel, queueName, exchangeName, options)

    await channel.consume(queueName, subscriptionHandlerWrapper(channel, queueName, subscriptionOptions, externalFn))
  } catch (err) {
    throw err
  }
}

const assertAndBindingQueues = async (channel: amqp.Channel, queueName: string, exchangeName: string, options?: SubscriptionOptions) => {
  const waitQueueName = `${queueName}.wait`
  const parkedQueueName = `${queueName}.parked`

  const [mainQueue, waitQueue] = await Promise.all([
    assertQueue(channel, queueName, {
      'x-dead-letter-exchange': exchangeName,
      'x-dead-letter-routing-key': waitQueueName
    }),
    assertQueue(channel, waitQueueName, {
      'x-dead-letter-exchange': exchangeName,
      'x-dead-letter-routing-key': queueName,
      'x-message-ttl': (options) ? options.waitQueueTtl : subscriptionDefaultOptions.waitQueueTtl
    }),
    assertQueue(channel, parkedQueueName)
  ])
  
  await Promise.all([
    channel.bindQueue(mainQueue.queue, exchangeName, ''),
    channel.bindQueue(mainQueue.queue, exchangeName, queueName),
    channel.bindQueue(waitQueue.queue, exchangeName, waitQueueName)
  ])
}

const assertQueue = (channel: amqp.Channel, queueName: string, queueArguments: QueueArguments = {}) => channel.assertQueue(queueName, {
  durable: true,
  arguments: queueArguments
})

const subscriptionHandlerWrapper = (channel: amqp.Channel, queueName: string, options: Required<SubscriptionOptions>, handler: any) => async (msg: ConsumeMessage | null) => {
  if (!msg) return

  try {
    const content = JSON.parse(Buffer.from(msg.content).toString())

    await handler({
      ...msg,
      content
    })
    channel.ack(msg)
  } catch (err) {
    console.log(`Message ${msg.properties.messageId} failed to deliver with error`)

    if (!msg.fields.redelivered) return channel.reject(msg, true)

    const numberOfRetry = getNumberOfRetry(msg.properties.headers)
    if (numberOfRetry < options.maxRetry) return channel.reject(msg, false)
    
    return sendMessageToParking(channel, queueName, msg, numberOfRetry)
  }
}

const getNumberOfRetry = (headers: amqp.MessagePropertyHeaders): number => {
  return headers['x-death'] ? headers['x-death'][0].count : 1
}

const sendMessageToParking = async (channel: amqp.Channel, queueName: string, msg: ConsumeMessage, numberOfRetry: number) => {
  const parkedQueueName = `${queueName}.parked`
  console.log(`Sending message ${msg.properties.messageId} from ${queueName} to ${parkedQueueName} after ${numberOfRetry} retries`)

  const content = Buffer.from(JSON.stringify(msg.content))
  channel.sendToQueue(parkedQueueName, content, { persistent: true, messageId: msg.properties.messageId })
  channel.ack(msg)
}