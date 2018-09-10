const { PostgresPubSub } = require('graphql-postgres-subscriptions')

function NewPostgresPubSub (config) {
  const client = new PostgresPubSub(config)

  this.publish = function publish ({ topic, compiledPayload }, context) {
    let payload = compiledPayload(context)
    client.publish(topic, payload)
  }

  this.getAsyncIterator = function getAsyncIterator (topic) {
    return client.asyncIterator(topic)
  }
}

module.exports = NewPostgresPubSub
