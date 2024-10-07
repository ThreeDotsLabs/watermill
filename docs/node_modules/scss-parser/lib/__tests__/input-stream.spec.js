// Copyright (c) 2015-present, salesforce.com, inc. All rights reserved
// Licensed under BSD 3-Clause - see LICENSE.txt or git.io/sfdc-license

/* eslint-env jest */

const createInputStream = require('../input-stream')

it('returns an new InputStream', () => {
  let i = createInputStream()
  expect(i).toMatchSnapshot()
})

describe('#position', () => {
  it('defaults the position to 0', () => {
    let p = createInputStream().position()
    expect(Object.isFrozen(p)).toBe(true)
    expect(p).toMatchSnapshot()
  })
})

describe('#peek', () => {
  it('returns the current character', () => {
    let i = createInputStream('hello')
    expect(i.peek()).toEqual('h')
  })
  it('returns the current character with an offset', () => {
    let i = createInputStream('hello')
    expect(i.peek(1)).toEqual('e')
  })
})

describe('#next', () => {
  it('consumes and returns the next character', () => {
    let i = createInputStream('hello')
    expect(i.next()).toEqual('h')
  })
  it('advances the cursor', () => {
    let i = createInputStream('hello')
    expect(i.next()).toEqual('h')
    expect(i.position().cursor).toEqual(1)
    expect(i.position().line).toEqual(1)
    expect(i.position().column).toEqual(1)
  })
  it('advances the line', () => {
    let i = createInputStream('h\ni')
    expect(i.next()).toEqual('h')
    expect(i.next()).toEqual('\n')
    expect(i.next()).toEqual('i')
    expect(i.position().cursor).toEqual(3)
    expect(i.position().line).toEqual(2)
    expect(i.position().column).toEqual(1)
  })
})

describe('#eof', () => {
  it('returns false if there are more characters', () => {
    let i = createInputStream('hello')
    expect(i.eof()).toEqual(false)
  })
  it('returns true if there are no more characters', () => {
    let i = createInputStream('hi')
    expect(i.eof()).toEqual(false)
    i.next()
    i.next()
    expect(i.eof()).toEqual(true)
  })
})

describe('#err', () => {
  it('throws an error', () => {
    let i = createInputStream('hello')
    i.next()
    i.next()
    expect(() => {
      i.err('Whoops')
    }).toThrow(/Whoops \(1:2\)/)
  })
})
