import forEach from 'lodash/forEach'
import isArray from 'lodash/isArray'
import isPlainObject from 'lodash/isPlainObject'

import match from './match'

const createPredicate = pattern => pattern == null
  ? () => false
  : value => match(pattern, value)

const applyPatch = (value, patch) => {
  if (isPlainObject(patch)) {
    if (isArray(value)) {
      const toRemove = createPredicate(patch['-'])
      let tmp = []
      forEach(value, (v, i) => {
        if (i in patch) {
          const p = patch[i]
          if (p === null) {
            return
          }
          tmp.push(applyPatch(v, p))
        } else if (!toRemove(v)) {
          tmp.push(v)
        }
      })
      const toAdd = patch['+']
      if (toAdd) {
        tmp.push.apply(tmp, toAdd)
      }
      return tmp
    }

    if (isPlainObject(value)) {
      value = { ...value }
      forEach(patch, (v, k) => {
        if (v === null) {
          delete value[k]
        } else {
          value[k] = applyPatch(value[k], v)
        }
      })
      return value
    }

    value = {}
    forEach(patch, (v, k) => {
      if (v !== null) {
        value[k] = applyPatch(null, v)
      }
    })
    return patch
  }

  return patch
}
export { applyPatch as default }
