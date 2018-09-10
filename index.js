const PubSub = require('./lib/pubsubNotifiers/pubsubNotifier')
const _ = require('lodash')
const { ApolloServer } = require('apollo-server-express')
const queryDepthLimit = require('graphql-depth-limit')
const { createComplexityLimitRule } = require('graphql-validation-complexity')
const buildSchema = require('./buildSchema')
const schemaListenerCreator = require('./lib/schemaListeners/schemaListenerCreator')
const SecurityService = require('./security/SecurityService')

class GraphqlExecutor {
  constructor (app, server, config) {
    this.config = config
    this.server = server
    this.app = app
    this.models = null
  }

  async initialize () {
    let { pubsubConfig, postgresConfig } = this.config
    this.pubsub = PubSub(pubsubConfig)
    this.models = require('./sequelize/models')(postgresConfig)
    await this.models.sequelize.sync({
      logging: false
    })

    const {
      graphQLConfig,
      playgroundConfig,
      schemaListenerConfig,
      securityServiceConfig,
      serverSecurity
    } = this.config

    let securityService
    let schemaDirectives

    if (securityServiceConfig.type && securityServiceConfig.config) {
      securityService = new SecurityService(securityServiceConfig)
      schemaDirectives = securityService.getSchemaDirectives()
    }

    const serverConfig = {
      serverSecurity: serverSecurity,
      securityService: securityService,
      schemaDirectives: schemaDirectives,
      graphQLConfig: graphQLConfig,
      playgroundConfig: playgroundConfig,
      schemaListenerConfig: schemaListenerConfig
    }

    this.serverConfig = serverConfig

    const { schema, dataSources } = await buildSchema(this.models, this.pubsub, this.serverConfig.schemaDirectives)
    this.schema = schema
    this.dataSources = dataSources

    await this.connectDataSources(this.dataSources)

    this.apolloServer = this.newApolloServer(this.app, this.schema, this.server, this.serverConfig.tracing, this.serverConfig.playgroundConfig, this.serverConfig.graphQLConfig.graphqlEndpoint, this.serverConfig.securityService, this.serverConfig.serverSecurity)

    this.schemaListener = schemaListenerCreator(this.serverConfig.schemaListenerConfig)
    this.debouncedOnSchemaRebuild = _.debounce(this.onSchemaChangedNotification, 500).bind(this)
    this.schemaListener.start(this.debouncedOnSchemaRebuild)
  }

  newApolloServer (app, schema, httpServer, tracing, playgroundConfig, graphqlEndpoint, securityService, serverSecurity) {
    let AuthContextProvider = null

    if (securityService) {
      AuthContextProvider = securityService.getAuthContextProvider()
    }

    let apolloServer = new ApolloServer({
      schema,
      validationRules: [
        queryDepthLimit(serverSecurity.queryDepthLimit),
        createComplexityLimitRule(serverSecurity.complexityLimit)
      ],
      context: async ({
        req
      }) => {
        const context = {
          request: req
        }
        if (AuthContextProvider) {
          context.auth = new AuthContextProvider(req)
        }
        return context
      },
      tracing,
      playground: {
        settings: {
          'editor.theme': 'light',
          'editor.cursorShape': 'block'
        },
        tabs: [{
          endpoint: playgroundConfig.endpoint,
          query: playgroundConfig.query,
          variables: JSON.stringify(playgroundConfig.variables)
        }]
      }
    })
    apolloServer.applyMiddleware({
      app,
      disableHealthCheck: true,
      path: graphqlEndpoint
    })
    apolloServer.installSubscriptionHandlers(httpServer)

    return apolloServer
  }

  async connectDataSources (dataSources) {
    for (let key of Object.keys(dataSources)) {
      const dataSource = dataSources[key]
      try {
        await dataSource.connect()
      } catch (error) {
        throw (error)
      }
    }
  }

  async disconnectDataSources (dataSources) {
    for (let key of Object.keys(dataSources)) {
      const dataSource = dataSources[key]
      try {
        await dataSource.disconnect()
      } catch (error) {
        // swallow
      }
    }
  }

  async onSchemaChangedNotification () {
    let newSchema
    try {
      newSchema = await buildSchema(this.models, this.pubsub, this.serverConfig.schemaDirectives)
    } catch (ex) {
    }

    if (newSchema) {
      // first do some cleaning up
      this.apolloServer.subscriptionServer.close()
      this.server.removeListener('request', this.app)
      // reinitialize the server objects
      this.schema = newSchema.schema
      this.newServer()

      try {
        await this.disconnectDataSources(this.dataSources) // disconnect existing ones first
      } catch (ex) {
      }

      try {
        await this.connectDataSources(newSchema.dataSources)
        this.dataSources = newSchema.dataSources
      } catch (ex) {
        try {
          await this.connectDataSources(this.dataSources)
        } catch (ex) {
        }
      }
    }
  }
}

module.exports = GraphqlExecutor
