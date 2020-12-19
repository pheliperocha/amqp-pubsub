import * as amqp from 'amqplib'
import { ProducerOptions } from './types'
import { v4 as uuidv4 } from 'uuid'

const producerDefaultOptions: Partial<ProducerOptions> = {
  username: 'guest',
  password: 'guest',
  host: 'localhost',
  port: 5672
}

export const createProducer = <T>(exchanges: (keyof T)[], options: ProducerOptions) => {
  return {
    producer: async <J extends keyof T>(exchange: J, params: Pick<T, J>[J]): Promise<void> => {
      const producerOptions = { ...producerDefaultOptions, ...options }

      const { conn, channel } = await getConnectionAndChannel(options)

      try {
        const exchangeName = `${producerOptions.serviceName}.${exchange.toString()}`
        await assertExchange(channel, exchangeName)

        const content = Buffer.from(JSON.stringify(params))
        const sent = channel.publish(exchangeName, '', content, { persistent: true, messageId: uuidv4() })

        if (!sent) {
          await new Promise((resolve) => channel.once('drain', () => resolve))
        }
      } catch (err) {
        throw err
      } finally {
        await channel.close()
        await conn.close()
      }
    },

    defineExchanges: async () => {
      const { conn, channel } = await getConnectionAndChannel(options)

      try {
        const arrPromises = exchanges.map(exchange => {
          const exchangeName = `${options.serviceName}.${exchange.toString()}`
          return assertExchange(channel, exchangeName)
        })

        await Promise.all(arrPromises)
      } catch (err) {
        throw err
      } finally {
        await channel.close()
        await conn.close()
      }
    }
  }
}

const getConnectionAndChannel = async (options: ProducerOptions) => {
  const conn = await amqp.connect(`amqp://${options.username}:${options.password}@${options.host}:${options.port}/`)
  const channel = await conn.createChannel()
  return { conn, channel }
}

const assertExchange = async (channel: amqp.Channel, exchangeName: string) => channel.assertExchange(exchangeName, 'topic', { durable: true })