import * as amqp from 'amqplib'
import { ProducerOptions } from './types'

const producerDefaultOptions: Partial<ProducerOptions> = {
  username: 'guest',
  password: 'guest',
  host: 'localhost',
  port: 5672
}

export const createProducer = <T>(options: ProducerOptions) => async <J extends keyof T>(exchange: J, params: Pick<T, J>[J]): Promise<void> => {
  const producerOptions = { ...producerDefaultOptions, ...options }

  const conn = await amqp.connect(`amqp://${producerOptions.username}:${producerOptions.password}@${producerOptions.host}:${producerOptions.port}/`)
  const channel = await conn.createChannel()

  try {
    const exchangeName = `${producerOptions.serviceName}.${exchange.toString()}`
    await channel.assertExchange(exchangeName, 'fanout', { durable: true })

    const content = Buffer.from(JSON.stringify(params))
    const sent = channel.publish(exchangeName, '', content, { persistent: true })

    if (!sent) {
      await new Promise((resolve) => channel.once('drain', () => resolve))
    }
  } catch (err) {
    console.error(err)
  } finally {
    console.log('Closing...')
    await channel.close()
    await conn.close()
  }
}