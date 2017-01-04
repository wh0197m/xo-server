import createLogger from 'log'
import generateToken from 'generate-token'
import { fromCallback } from 'promise-toolbox'
import { once } from 'lodash'

const HTTP_REQUEST_VALIDITY = 1e3 * 60 * 60
const { warn } = createLogger('web-server')

export default class WebServer {
  constructor (app, { webServer }) {
    app.on('stop', () => fromCallback(cb => webServer.close(cb)))

    const handlers = this._handlers = { __proto__: null }

    webServer.on('request', (req, res) => {
      const handler = handlers[req.url]
      if (!handler) {
        return
      }

      const { method } = handler
      if (!(method && method === req.method)) {
        return
      }

      if (handler.once) {
        handler.unregister()
      }

      try {
        handler.handler.call(app, req, res, handler.data)
      } catch (error) {
        warn('handler error', { error })
      }
    })
  }

  registerHttpHandler (handler, {
    data,
    method,
    once: once_ = false,
    path,
    ttl
  }) {
    const handlers = this._handlers

    if (handlers[path]) {
      throw new Error(`there is already an HTTP handler for ${path}`)
    }

    const unregister = once(() => {
      delete handlers[path]
    })

    handlers[path] = {
      data,
      handler,
      method: method && method.toUpperCase(),
      once: once_,
      path,
      unregister
    }

    if (ttl) {
      setTimeout(unregister, ttl)
    }

    return unregister
  }

  registerHttpRequest (handler, {
    data,
    path,
    method = 'GET',
    suffix
  }) {
    return generateToken().then(token => {
      let path = `/${token}`
      if (suffix) {
        path += `/${encodeURI(token)}`
      }

      this.registerHttpHandler(handler, {
        data,
        method,
        once: true,
        path,
        ttl: HTTP_REQUEST_VALIDITY
      })

      return path
    })
  }
}
