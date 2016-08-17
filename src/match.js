import every from 'lodash/every'
import isArray from 'lodash/isArray'
import isPlainObject from 'lodash/isPlainObject'
import size from 'lodash/size'
import some from 'lodash/some'

const match = (pattern, value) => {
  if (isPlainObject(pattern)) {
    if (size(pattern) === 1) {
      if (pattern.__or) {
        return some(pattern.__or, subpattern => match(subpattern, value))
      }
      if (pattern.__not) {
        return !match(pattern.__not, value)
      }
    }

    return isPlainObject(value) && every(pattern, (subpattern, key) => (
      value[key] !== undefined && match(subpattern, value[key])
    ))
  }

  if (isArray(pattern)) {
    return isArray(value) && every(pattern, subpattern =>
      some(value, subvalue => match(subpattern, subvalue))
    )
  }

  return pattern === value
}
export { match as default }
