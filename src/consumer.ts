import * as amqp from 'amqplib'
import { ConsumeMessage } from 'amqplib'
import { ConsumerOptions, QueueArguments } from './types'
import { capitalizeWord } from './utils'

const consumerDefaultOptions: Required<ConsumerOptions> = {
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
type IConsumerHandler<T, K extends keyof T> = (params: IParsedConsumeMessage<Pick<T, K>[K]>) => any
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

    const queueName = `${consumerOptions.serviceName}.${consumerOptions.exchangeService}${capitalizeWord(exchange)}`
    await assertAndBindingQueues(channel, queueName, exchangeName, options)

    await channel.consume(queueName, consumerHandlerWrapper(channel, queueName, consumerOptions, externalFn))
  } catch (err) {
    throw err
  }
}

const assertAndBindingQueues = async (channel: amqp.Channel, queueName: string, exchangeName: string, options?: ConsumerOptions) => {
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
      'x-message-ttl': (options) ? options.waitQueueTtl : consumerDefaultOptions.waitQueueTtl
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

const consumerHandlerWrapper = (channel: amqp.Channel, queueName: string, options: Required<ConsumerOptions>, handler: any) => async (msg: ConsumeMessage | null) => {
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