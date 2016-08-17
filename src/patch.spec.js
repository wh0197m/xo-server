/* eslint-env jest */

import patch from './patch'

// ===================================================================

describe('patch', () => {
  it('can patch arrays', () => {
    expect(patch(
      [ 'foo', 'bar', 'quuz' ],
      { 0: null, '-': 'quuz', '+': [ 'baz' ] }
    )).toEqual(
      [ 'bar', 'baz' ]
    )
  })

  it('can patch objects', () => {
    expect(patch(
      { foo: 1, bar: 2 },
      { foo: null, bar: 3, baz: 4 }
    )).toEqual(
      { bar: 3, baz: 4 }
    )
  })
})
