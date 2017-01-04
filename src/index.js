#!/usr/bin/env node

const APP_NAME = 'xo-server'

// -------------------------------------------------------------------

const { info, warn } = require('log').default('bootstrap')

process.on('unhandledRejection', reason => {
  warn('possibly unhandled rejection', reason)
})

;(({ prototype }) => {
  const { emit } = prototype
  prototype.emit = function (event, error) {
    (event === 'error' && !this.listenerCount(event))
      ? warn('unhandled error event', error)
      : emit.apply(this, arguments)
  }
})(require('events').EventEmitter)

const Bluebird = require('bluebird')
Bluebird.longStackTraces()
global.Promise = Bluebird

// -------------------------------------------------------------------

;(async args => {
  info('starting')

  const config = await require('app-conf').load(APP_NAME)

  const webServer = new (require('http-server-plus'))()

  const readFile = Bluebird.promisify(require('fs').readFile)
  await Promise.all(require('lodash/map')(
    config.http.listen,
    async ({
      certificate,

      // The properties was called `certificate` before.
      cert = certificate,

      key,
      ...opts
    }) => {
      if (cert && key) {
        [ opts.cert, opts.key ] = await Promise.all([
          readFile(cert),
          readFile(key)
        ])
      }

      try {
        const niceAddress = await webServer.listen(opts)
        info(`Web server listening on ${niceAddress}`)
      } catch (error) {
        if (error.niceAddress) {
          warn(`Web server could not listen on ${error.niceAddress}`)

          const { code } = error
          if (code === 'EACCES') {
            warn('  Access denied.')
            warn('  Ports < 1024 are often reserved to privileges users.')
          } else if (code === 'EADDRINUSE') {
            warn('  Address already in use.')
          }
        } else {
          warn('Web server could not listen', error)
        }
      }
    }
  ))

  try {
    const { group, user } = config
    group != null && process.setgid(group)
    user != null && process.setuid(user)
  } catch (error) {
    warn('failed to change group/user', error)
  }

  global.Observable = require('zen-observable')

  const app = new (require('./app').default)({ // eslint-disable-line new-cap
    appName: APP_NAME,
    config,
    safeMode: require('lodash/includes')(args, '--safe-mode'),
    webServer
  })
  await app.start()

  // Gracefully shutdown on signals.
  //
  // TODO: implements a timeout? (or maybe it is the services launcher
  // responsibility?)
  require('lodash/forEach')([ 'SIGINT', 'SIGTERM' ], signal => {
    let alreadyCalled = false

    process.on(signal, () => {
      if (alreadyCalled) {
        warn('forced exit')
        process.exit(1)
      }
      alreadyCalled = true

      info(`${signal} caught, closing…`)
      app.stop()
    })
  })

  return require('event-to-promise')(app, 'stopped')
})(process.argv.slice(2)).then(
  () => info('bye :-)'),
  error => warn('fatal error', error)
)
