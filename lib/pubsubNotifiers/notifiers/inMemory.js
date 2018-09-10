const PubSub = require('graphql-subscriptions').PubSub

function InMemoryPubSub (config) {
  const client = new PubSub()

  this.publish = function publish ({ topic, compiledPayload }, context) {
    let payload = compiledPayload(context)
    // The InMemory pubsub implementation wants an object
    // Whereas the postgres one would expect a string
    try {
      payload = JSON.parse(payload)
      client.publish(topic, payload)
    } catch (error) {
    }
  }

  this.getAsyncIterator = function getAsyncIterator (topic) {
    return client.asyncIterator(topic)
  }
}

module.exports = InMemoryPubSub
