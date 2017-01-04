import createLogger from 'log'
import { forEach, map, startsWith } from 'lodash'
import { join } from 'path'
import { readdir } from 'fs-promise'

const { info, warn } = createLogger('plugins')

// ===================================================================

const LOOKUP_PATHS = [
  '/usr/local/lib/node_modules',
  join(__dirname, '../../../node_modules')
]

// -------------------------------------------------------------------

export default class Plugins {
  constructor (app, { appName, safeMode }) {
    this._prefix = `${appName}-`
    this._plugins = {}

    app.on('start', () => {
      if (!safeMode) {
        return this.discoverPlugins()
      }
    })
  }

  async discoverPlugins () {
    return Promise.all(map(await this._listPlugins(), async (plugin, name) => {

    }))
  }

  _listPlugins () {
    const plugins = { __proto__: null }

    const prefix = this._prefix
    const prefixLength = prefix.length

    return Promise.all(map(LOOKUP_PATHS, path => readdir(path).then(
      basenames => forEach(basenames, basename => {
        if (startsWith(basename, prefix)) {
          const name = basename.slice(prefixLength)
          const path = join(path, basename)

          const previous = plugins[name]
          if (name in plugins) {
            warn(`duplicate plugins ${name}`, {
              name,
              paths: [
                previous.path,
                path
              ].sort()
            })

            return
          }

          let plugin
          try {
            plugin = require(path)
            info(`successfully imported plugin ${name}`, {
              name,
              path
            })
          } catch (error) {
            warn(`failed to import plugin ${name}`, {
              error,
              name,
              path
            })
            return
          }

          let version
          try {
            ({ version } = require(join(path, 'package.json')))
          } catch (_) {}

          // Supports both “normal” CommonJS and Babel's ES2015 modules.
          const {
            default: factory = plugin,
            configurationSchema,
            configurationPresets
          } = plugin

          plugins[name] = {
            configurationPresets,
            configurationSchema,
            factory,
            version
          }
        }
      }),
      error => {
        if (error.code !== 'ENOENT') {
          warn('plugins', 'failed to read directory', {
            error,
            path
          })
        }
      }
    )))
  }
}
