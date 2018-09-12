const Kafka = require('node-rdkafka')
const config = require('config')

class KafkaProducer {
  constructor (logger) {
    this.log = logger
    this.kafkaGlobalUuid = 'KafkaProducerGlobalUuid'

    let producerConfig = {
      ...config.kafka.producer.config,
      ...{
        'client.id': 'relationConsumer',
        'dr_cb': true,
        'socket.keepalive.enable': true
      }
    }

    this.log.debug({ producer: producerConfig }, `Creating producer`)

    this.producer = new Kafka.Producer(producerConfig, {})
  }

  /**
   * Connect the producer to kafka's configured bus.
   * @returns {Promise<>} Return resolve() if the connection was successful, reject(err) otherwise.
   */
  connect (uuid) {
    // Kafka global uuid is the same as the server for ease of search
    this.kafkaGlobalUuid = uuid
    // Poll for events every 1000 ms
    this.producer.setPollInterval(1000)

    // To use "this" in callbacks
    let self = this

    this.producer.on('delivery-report', function (err, report) {
      // Report of delivery statistics here:
      self.log.debug({ uuid: self.kafkaGlobalUuid, report: report, err: err }, 'Delivery report')
    })

    // Bus is ready and message(s) can be pushed
    this.producer.on('ready', function () {
      self.log.info({ uuid: self.kafkaGlobalUuid }, 'Kafka producer ready')
    })

    // Errors received from the bus
    this.producer.on('event.error', function (error) {
      self.log.error({ uuid: self.kafkaGlobalUuid, err: error }, `Error on Kafka producer: ${error.message}`)
    })

    return new Promise((resolve, reject) => {
      this.log.info({ uuid: this.kafkaGlobalUuid }, 'Starting Kafka producer')

      this.producer.connect(null, (err, metadata) => {
        if (err) {
          self.log.error({ uuid: self.kafkaGlobalUuid, err: err }, 'Producer error while connecting')
          return reject(err)
        } else {
          self.log.info({ uuid: self.kafkaGlobalUuid }, 'Producer connected', metadata)
          return resolve()
        }
      })
    })
  }

  /**
   * Disconnect from Kafka.
   */
  disconnect () {
    return new Promise((resolve) => {
      this.log.debug({ uuid: this.kafkaGlobalUuid }, 'Disconnecting producer')
      this.producer.poll()
      let self = this
      this.producer.disconnect(5000, (params, metrics) => {
        self.log.info({ uuid: self.kafkaGlobalUuid, params: params, metrics: metrics }, `Disconnected from producer`)
        resolve()
      })
    })
  }

  /**
   * Send given message to Kafka.
   * @param {String} topic The message will be pushed to this topic (topic will be created if it does not exist).
   * @param {Buffer} message Must be a Buffer (new Buffer(...))
   * @param {int} timestamp
   */
  sendMessage (topic, message, timestamp) {
    let fullTopic = config.kafka.producer.topicsPrefix + topic
    this.producer.produce(fullTopic, 0, message, 'backup', timestamp, 0)
  }
}

module.exports = KafkaProducer
