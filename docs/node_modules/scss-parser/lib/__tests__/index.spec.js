// Copyright (c) 2015-present, salesforce.com, inc. All rights reserved
// Licensed under BSD 3-Clause - see LICENSE.txt or git.io/sfdc-license

/* eslint-env jest */

const { parse, stringify } = require('../')

it('parse', () => {
  expect(parse('.a { background: red; }')).toMatchSnapshot()
})

it('parse', () => {
  const scss = `.a { background: red; }`
  expect(stringify(parse(scss))).toEqual(scss)
})
