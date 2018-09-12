const Kafka = require('node-rdkafka')
const config = require('config')

class KafkaListener {
  constructor (logger) {
    this.log = logger
    this.kafkaGlobalUuid = 'KafkaListenerGlobalUuid'

    let consumerConfig = {
      ...config.kafka.consumer.config,
      ...{
        'enable.auto.commit': false,
        'socket.keepalive.enable': true
      }
    }

    this.log.debug({consumer: consumerConfig}, `Creating consumer`)

    this.consumer = new Kafka.KafkaConsumer(consumerConfig, {'auto.offset.reset': 'earliest'})
  }

  getConsumer () {
    return this.consumer
  }

  /**
   * Connect the listener to kafka's configured bus.
   * @param {String} uuid
   * @returns {Promise<null>} Return resolve() if the connection was successful, reject(err) otherwise.
   */
  connect (uuid) {
    // Kafka global uuid is the same as the server for ease of search
    this.kafkaGlobalUuid = uuid

    // To use "this" in callbacks
    let self = this

    // Bus is ready and message(s) can be consumed
    this.consumer.on('ready', function () {
      self.log.info({uuid: self.kafkaGlobalUuid}, 'Kafka consumer ready')
      self.consumer.setDefaultConsumeTimeout(5000)
      // self.consumer.subscribe(topics)
    })

    this.consumer.on('event.stats', function (stats) {
      self.log.trace({uuid: self.kafkaGlobalUuid}, 'Stats on Kafka listener: ', stats)
    })

    this.consumer.on('event.throttle', function (throttle) {
      self.log.warn({uuid: self.kafkaGlobalUuid}, 'Throttle on Kafka listener: ', throttle)
    })

    // Errors received from the bus
    this.consumer.on('event.error', function (error) {
      self.log.error({uuid: self.kafkaGlobalUuid, err: error}, `Error on Kafka listener: ${error.message}`)
    })

    return new Promise((resolve, reject) => {
      this.log.info({uuid: this.kafkaGlobalUuid}, 'Starting Kafka consumer')

      this.consumer.connect(null, (err, metadata) => {
        if (err) {
          self.log.error({uuid: self.kafkaGlobalUuid, err: err}, 'Consumer error')
          return reject(err)
        } else {
          self.log.info({uuid: self.kafkaGlobalUuid, metadata: metadata}, 'Consumer connected')
          return resolve()
        }
      })
    })
  }

  /**
   * Disconnect from Kafka.
   * @returns {Promise<null>} Return resolve() when done.
   */
  disconnect () {
    // To use "this" in callbacks
    let self = this

    return new Promise((resolve) => {
      this.log.debug({uuid: this.kafkaGlobalUuid}, 'Disconnecting listener')
      this.consumer.disconnect((params, metrics) => {
        self.log.info({uuid: self.kafkaGlobalUuid, params: params, metrics: metrics}, `Disconnected from listener`)
        resolve()
      })
    })
  }

  /**
   * Unsubscribe then subscribe to the given topics
   * @param {Array<String>} topics
   */
  subscribe (topics) {
    this.consumer.unsubscribe()
    this.consumer.subscribe(topics)
  }

  listen (number, uuid) {
    // To use "this" in callbacks
    let self = this

    return new Promise((resolve, reject) => {
      this.consumer.consume(number, (err, messages) => {
        self.log.debug({uuid: uuid}, 'assignments', self.consumer.assignments())
        self.log.debug({uuid: uuid}, 'position', self.consumer.position())

        this.consumer.committed(undefined, 5000, (err, topicPartitions) => {
          self.log.debug({uuid: uuid, err: err}, 'Committed before: ', topicPartitions)
        })

        if (err) {
          reject(err)
        } else {
          self.consumer.commit()
          self.consumer.committed(undefined, 5000, (err, topicPartitions) => {
            self.log.debug({uuid: uuid, err: err}, 'Committed offset: ', topicPartitions)
          })

          resolve(messages)
        }
      })
    })
  }
}

module.exports = KafkaListener
