// Copyright (c) 2015-present, salesforce.com, inc. All rights reserved
// Licensed under BSD 3-Clause - see LICENSE.txt or git.io/sfdc-license

/* eslint-env jest */

const createInputStream = require('../input-stream')
const createTokenStream = require('../token-stream')
const parse = require('../parse')
const stringify = require('../stringify')

const createAST = (input) =>
  parse(createTokenStream(createInputStream(input)))

it('class', () => {
  const css = '.a {}'
  const ast = createAST(css)
  expect(stringify(ast)).toEqual(css)
})

it('atkeyword', () => {
  const css = '@mixin myMixin {}'
  const ast = createAST(css)
  expect(stringify(ast)).toEqual(css)
})

it('pseudo_class', () => {
  const css = '.a:hover:active:#{focus} {}'
  const ast = createAST(css)
  expect(stringify(ast)).toEqual(css)
})

it('sink 1', () => {
  const css = `
    .a {
      .b {
        color: red;
      }
    }
  `
  const ast = createAST(css)
  expect(stringify(ast)).toEqual(css)
})

it('sink 2', () => {
  const css = `
    /// Casts a string into a number (integer only)
    ///
    /// @param {String} $value - Value to be parsed
    ///
    /// @return {Number}
    /// @author @HugoGiraudel - Simplified by @kaelig to only convert unsigned integers
    /// @see http://hugogiraudel.com/2014/01/15/sass-string-to-number/
    /// @access private
    @function _d-to-number($value) {
      $result: 0;
      $digits: 0;
      $numbers: ('0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9);

      @for $i from 1 through str-length($value) {
        $character: str-slice($value, $i, $i);

        @if $digits == 0 {
          $result: $result * 10 + map-get($numbers, $character);
        } @else {
          $digits: $digits * 10;
          $result: $result + map-get($numbers, $character) / $digits;
        }
      }

      @return $result;
    }
  `
  const ast = createAST(css)
  expect(stringify(ast)).toEqual(css)
})

it('sink 3', () => {
  const css = `
    *,
    *:before,
    *:after {
      box-sizing: border-box;
    }
  `
  const ast = createAST(css)
  expect(stringify(ast)).toEqual(css)
})

it('sink 4', () => {
  const css = `
    li:hover:active {
      color:red;
    }
  `
  const ast = createAST(css)
  expect(stringify(ast)).toEqual(css)
})
