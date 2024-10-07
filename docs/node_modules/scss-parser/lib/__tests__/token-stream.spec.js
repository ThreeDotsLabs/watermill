// Copyright (c) 2015-present, salesforce.com, inc. All rights reserved
// Licensed under BSD 3-Clause - see LICENSE.txt or git.io/sfdc-license

/* eslint-env jest */

const createInputStream = require('../input-stream')
const createTokenStream = require('../token-stream')

it('requires an InputStream', () => {
  expect(() => {
    createTokenStream()
  }).toThrow(/InputStream/)
})

it('returns an TokenStream', () => {
  let t = createTokenStream(createInputStream())
  expect(t).toMatchSnapshot()
})

describe('#all', () => {
  it('tokenizes all characters in the InputStream', () => {
    let t = createTokenStream(createInputStream('hello'))
    expect(t.all().length).toEqual(1)
    expect(t.eof()).toEqual(true)
  })
})

describe('#peek', () => {
  it('returns the current token', () => {
    let t = createTokenStream(createInputStream('hello'))
    expect(t.peek()).toMatchSnapshot()
  })
  it('returns the current token with an offset', () => {
    let t = createTokenStream(createInputStream('hello world'))
    expect(t.peek(1)).toMatchSnapshot()
  })
})

describe('#next', () => {
  it('consumes returns and the next token', () => {
    let t = createTokenStream(createInputStream('hello world'))
    expect(t.next()).toMatchSnapshot()
    expect(t.peek()).toMatchSnapshot()
  })
  describe('tokens', () => {
    describe('space', () => {
      it('single space', () => {
        let t = createTokenStream(createInputStream(' '))
        expect(t.next()).toMatchSnapshot()
      })
      it('multiple spaces', () => {
        let t = createTokenStream(createInputStream('    hello'))
        expect(t.next()).toMatchSnapshot()
      })
      it('whitespace characters', () => {
        let t = createTokenStream(createInputStream('\n\n\t  hello'))
        expect(t.next()).toMatchSnapshot()
      })
      it('carriage return character', () => {
        let t = createTokenStream(createInputStream('\r\n  hello'))
        expect(t.next()).toMatchSnapshot()
      })
    })
    describe('comment', () => {
      it('single comment', () => {
        let t = createTokenStream(createInputStream('// Hello\nWorld'))
        expect(t.next()).toMatchSnapshot()
      })
      it('single comment', () => {
        let t = createTokenStream(createInputStream('/** Hello World */'))
        expect(t.next()).toMatchSnapshot()
      })
    })
    describe('number', () => {
      it('integer', () => {
        let t = createTokenStream(createInputStream('3'))
        expect(t.next()).toMatchSnapshot()
      })
      it('float', () => {
        let t = createTokenStream(createInputStream('3.0'))
        expect(t.next()).toMatchSnapshot()
      })
      it('float (leading decimal)', () => {
        let t = createTokenStream(createInputStream('.3'))
        expect(t.next()).toMatchSnapshot()
      })
    })
    describe('hex', () => {
      it('6 digit lowercase', () => {
        let t = createTokenStream(createInputStream('#ff0099'))
        expect(t.next()).toMatchSnapshot()
      })
      it('6 digit uppercase', () => {
        let t = createTokenStream(createInputStream('#FF0099'))
        expect(t.next()).toMatchSnapshot()
      })
      it('3 digit lowercase', () => {
        let t = createTokenStream(createInputStream('#ff0'))
        expect(t.next()).toMatchSnapshot()
      })
      it('3 digit uppercase', () => {
        let t = createTokenStream(createInputStream('#FF0'))
        expect(t.next()).toMatchSnapshot()
      })
      it('3 digit (trailing invalid)', () => {
        let t = createTokenStream(createInputStream('#FF0;'))
        expect(t.next()).toMatchSnapshot()
      })
      it('6 digit numbers', () => {
        let t = createTokenStream(createInputStream('#000000'))
        expect(t.next()).toMatchSnapshot()
      })
    })
    describe('atkeyword', () => {
      it('works', () => {
        let t = createTokenStream(createInputStream('@mixin'))
        expect(t.next()).toMatchSnapshot()
      })
    })
    describe('puctuation', () => {
      it('{', () => {
        let t = createTokenStream(createInputStream('{'))
        expect(t.next()).toMatchSnapshot()
      })
    })
    describe('operator', () => {
      it('+', () => {
        let t = createTokenStream(createInputStream('+'))
        expect(t.next()).toMatchSnapshot()
      })
      it('repeatable', () => {
        let t = createTokenStream(createInputStream('&&'))
        expect(t.next()).toMatchSnapshot()
      })
      it('non-repeatable', () => {
        let t = createTokenStream(createInputStream('++'))
        expect(t.next()).toMatchSnapshot()
      })
      it('repeatable followed by non-repeatable', () => {
        let t = createTokenStream(createInputStream('&++'))
        expect(t.next()).toMatchSnapshot()
      })
    })
    describe('identifier', () => {
      it('checks for valid starting characters', () => {
        let t = createTokenStream(createInputStream('_hello world'))
        expect(t.next()).toMatchSnapshot()
      })
      it('ignores invalid starting characters', () => {
        let t = createTokenStream(createInputStream('0hello world'))
        expect(t.next()).toMatchSnapshot()
      })
    })
    describe('string', () => {
      it('single quotes', () => {
        let t = createTokenStream(createInputStream('\'hello\''))
        expect(t.next()).toMatchSnapshot()
      })
      it('double quotes', () => {
        let t = createTokenStream(createInputStream('"hello"'))
        expect(t.next()).toMatchSnapshot()
      })
      it('escaped characters', () => {
        let t = createTokenStream(createInputStream('"hello \\"world\\""'))
        expect(t.next()).toMatchSnapshot()
      })
      it('preserves escape characters', () => {
        let t = createTokenStream(createInputStream('token(\'\'+myVar+\'font(\\\'world\\\')\')'))
        expect(t.all()).toMatchSnapshot()
      })
    })
    describe('variable', () => {
      it('works', () => {
        let t = createTokenStream(createInputStream('$size'))
        expect(t.next()).toMatchSnapshot()
      })
    })
    describe('sink', () => {
      it('1', () => {
        let t = createTokenStream(createInputStream('($var)'))
        expect(t.all()).toMatchSnapshot()
      })
      it('2', () => {
        let t = createTokenStream(createInputStream('// ($var)\n@mixin myMixin'))
        expect(t.all()).toMatchSnapshot()
      })
    })
  })
})

describe('#eof', () => {
  it('returns false if there are more tokens', () => {
    let t = createTokenStream(createInputStream('hello'))
    expect(t.eof()).toEqual(false)
  })
  it('returns true if there are no more tokens', () => {
    let t = createTokenStream(createInputStream('hello world'))
    expect(t.eof()).toEqual(false)
    t.next()
    t.next()
    t.next()
    expect(t.eof()).toEqual(true)
  })
})

describe('#err', () => {
  it('throws an error', () => {
    let t = createTokenStream(createInputStream('hello world'))
    t.next()
    t.next()
    expect(() => {
      t.err('Whoops')
    }).toThrow(/Whoops \(1:6\)/)
  })
})
