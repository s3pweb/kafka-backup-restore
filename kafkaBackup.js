const fs = require('fs')
const uuid = require('uuid/v4')

const KafkaListener = require('./lib/kafka/kafkaListener')
const log = require('./lib/logger/logger')('Main')

const processorGlobalId = uuid()

let listener = new KafkaListener(log)
let readCount = 0

async function main () {
  await listener.connect(processorGlobalId)

  let topics = []
  listener.getConsumer()._metadata.topics.forEach((topic) => {
    topics.push(topic.name)
  })

  log.debug('Will connect to this topics: ', topics)
  listener.subscribe(topics)

  let continueLoop = true

  while (continueLoop) {
    let uid = uuid()
    let messages = await listener.listen(1000, uid)

    log.info({ uuid: uid }, `Got ${messages.length} message(s)`)

    for (let event of messages) {
      readCount++
      let filepath = `./backup/${event.topic}_${event.timestamp}_${event.offset}.json`
      // log.debug({uuid: uid}, `Backup event to ${filepath}`)
      await fs.writeFileSync(filepath, event.value.toString(), 'utf8')
    }

    if (messages.length > 0) {
      log.info({ uuid: processorGlobalId }, `Read ${readCount}  events from the beginning.`)
    } else {
      log.info({ uuid: processorGlobalId }, 'No events received!')
      continueLoop = false
    }
  }

  await listener.disconnect()
}

main()
  .catch((err) => {
    log.error({ err: err }, `Application error.`)
    listener.disconnect()
  })
