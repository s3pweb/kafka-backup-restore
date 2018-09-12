const fs = require('fs')
const uuid = require('uuid/v4')

const KafkaProducer = require('./lib/kafka/kafkaProducer')
const log = require('./lib/logger/logger')('Main')

const processorGlobalId = uuid()

let producer = new KafkaProducer(log)

async function main () {
  await producer.connect(processorGlobalId)

  fs.readdirSync('./backup/').forEach(file => {
    // console.log(file)
    log.debug({ uuid: processorGlobalId }, `Processing ${file}`)
    let fileContent = fs.readFileSync('./backup/' + file, 'utf8')
    // console.log(fileContent)
    producer.sendMessage('kafkaBackup', Buffer.from(fileContent), fileContent.split('_')[1])
  })

  await producer.disconnect()
}

main()
  .catch((err) => {
    log.error({ err: err }, `Application error.`)
    producer.disconnect()
  })
