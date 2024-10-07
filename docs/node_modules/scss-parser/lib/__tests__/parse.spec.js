// Copyright (c) 2015-present, salesforce.com, inc. All rights reserved
// Licensed under BSD 3-Clause - see LICENSE.txt or git.io/sfdc-license

/* eslint-env jest */

const createInputStream = require('../input-stream')
const createTokenStream = require('../token-stream')
const parse = require('../parse')

const createAST = (input) =>
  parse(createTokenStream(createInputStream(input)))

it('requires an InputStream', () => {
  expect(() => {
    parse()
  }).toThrow(/TokenStream/)
})

it('returns an AST', () => {
  const ast = createAST('a')
  expect(ast).toMatchSnapshot()
})

describe('function', () => {
  it('no args', () => {
    const ast = createAST('fn()')
    expect(ast).toMatchSnapshot()
  })
  it('1 arg', () => {
    const ast = createAST('fn($a)')
    expect(ast).toMatchSnapshot()
  })
  it('2 args', () => {
    const ast = createAST('fn($a, $b)')
    expect(ast).toMatchSnapshot()
  })
  it('function as the caller', () => {
    const ast = createAST('hello(world($a))')
    expect(ast).toMatchSnapshot()
  })
  it('interpolation as the caller', () => {
    const ast = createAST('#{hello}($a)')
    expect(ast).toMatchSnapshot()
  })
})
describe('interpolation', () => {
  it('1 var', () => {
    const ast = createAST('#{$a}')
    expect(ast).toMatchSnapshot()
  })
  it('nested', () => {
    const ast = createAST('#{#{$a}}')
    expect(ast).toMatchSnapshot()
  })
})
describe('parentheses', () => {
  it('1 var', () => {
    const ast = createAST('($a)')
    expect(ast).toMatchSnapshot()
  })
  it('nested', () => {
    const ast = createAST('(($a))')
    expect(ast).toMatchSnapshot()
  })
})
describe('attribute', () => {
  it('1 var', () => {
    const ast = createAST('[$a]')
    expect(ast).toMatchSnapshot()
  })
  it('nested', () => {
    const ast = createAST('[[$a]]')
    expect(ast).toMatchSnapshot()
  })
})
describe('class', () => {
  it('identifier', () => {
    const ast = createAST('.hello')
    expect(ast).toMatchSnapshot()
  })
  it('identifier + interpolation', () => {
    const ast = createAST('.hello-#{$a}')
    expect(ast).toMatchSnapshot()
  })
  it('identifier + interpolation + identifier', () => {
    const ast = createAST('.hello-#{$a}-world')
    expect(ast).toMatchSnapshot()
  })
  it('identifier + id', () => {
    const ast = createAST('.hello#world')
    expect(ast).toMatchSnapshot()
  })
})
describe('id', () => {
  it('identifier', () => {
    const ast = createAST('#hello')
    expect(ast).toMatchSnapshot()
  })
  it('identifier + interpolation', () => {
    const ast = createAST('#hello-#{$a}')
    expect(ast).toMatchSnapshot()
  })
  it('interpolation', () => {
    const ast = createAST('##{$a}')
    expect(ast).toMatchSnapshot()
  })
})
describe('declaration', () => {
  it('simple', () => {
    const ast = createAST('$color: red;')
    expect(ast).toMatchSnapshot()
  })
  it('complex', () => {
    const ast = createAST('$map: ("foo": "bar", "hello": rgba($a));')
    expect(ast).toMatchSnapshot()
  })
  it('trailing', () => {
    const ast = createAST('.a { padding: 1px { top: 2px; } }')
    expect(ast).toMatchSnapshot()
  })
  it('trailing 2', () => {
    const ast = createAST('padding: 1px { top: 2px; }')
    expect(ast).toMatchSnapshot()
  })
})
describe('rule', () => {
  it('1 selector', () => {
    const ast = createAST('.a {}')
    expect(ast).toMatchSnapshot()
  })
  it('1 selector 1 declaration', () => {
    const ast = createAST('.a { color: red; }')
    expect(ast).toMatchSnapshot()
  })
  it('1 selector 1 declaration 1 nested selector 1 declaration', () => {
    const ast = createAST('.a { color: red; .b { color: blue; } }')
    expect(ast).toMatchSnapshot()
  })
  it('trailing ";"', () => {
    const ast = createAST('.a {}; .b {}')
    expect(ast).toMatchSnapshot()
  })
  it('1 pseudo class', () => {
    const ast = createAST(':hover {}')
    expect(ast).toMatchSnapshot()
  })
  it('1 class 2 pseudo classes', () => {
    const ast = createAST('.a:hover:active {}')
    expect(ast).toMatchSnapshot()
  })
  it('1 class 2 pseudo classes 1 interpolation', () => {
    const ast = createAST('.a:hover:#{active} {}')
    expect(ast).toMatchSnapshot()
  })
  it('2 classes 2 pseudo classes', () => {
    const ast = createAST('.a:hover, .a:active {}')
    expect(ast).toMatchSnapshot()
  })
  it('2 classes 2 pseudo classes', () => {
    const ast = createAST('li:hover[data-foo=bar] {}')
    expect(ast).toMatchSnapshot()
  })
  it('nested pseudo classes', () => {
    const ast = createAST('li:a(:b) {}')
    expect(ast).toMatchSnapshot()
  })
  it('nested pseudo classes (with identifier)', () => {
    const ast = createAST('li:a(item:b) {}')
    expect(ast).toMatchSnapshot()
  })
  it('1 pseudo class 1 declaration (no space)', () => {
    const ast = createAST('li:hover { color:red; }')
    expect(ast).toMatchSnapshot()
  })
  it('1 pseudo class 1 declaration 1 nested declaration', () => {
    const ast = createAST('li:hover { color: red { alt: blue; } }')
    expect(ast).toMatchSnapshot()
  })
  it('1 pseudo class 1 declaration (no space) 1 nested declaration', () => {
    const ast = createAST('li:hover { color:red { alt:blue; } }')
    expect(ast).toMatchSnapshot()
  })
})
describe('atrule', () => {
  it('include 0 args', () => {
    const ast = createAST('@include myMixin;')
    expect(ast).toMatchSnapshot()
  })
  it('include 1 required arg', () => {
    const ast = createAST('@include myMixin($a);')
    expect(ast).toMatchSnapshot()
  })
  it('include 1 required arg 1 optional arg', () => {
    const ast = createAST('@include myMixin($a, $b: null);')
    expect(ast).toMatchSnapshot()
  })
  it('include 1 required arg 1 optional arg (complex)', () => {
    const ast = createAST('@include myMixin($a, $b: rgba($c) + 1);')
    expect(ast).toMatchSnapshot()
  })
  it('mixin 0 args', () => {
    const ast = createAST('@mixin myMixin { }')
    expect(ast).toMatchSnapshot()
  })
  it('mixin 0 args 1 declaration', () => {
    const ast = createAST('@mixin myMixin { color: red; }')
    expect(ast).toMatchSnapshot()
  })
  it('mixin 1 required arg 1 declaration', () => {
    const ast = createAST('@mixin myMixin($a) { color: red; }')
    expect(ast).toMatchSnapshot()
  })
  it('mixin 1 required arg 1 optional arg 1 declaration', () => {
    const ast = createAST('@mixin myMixin($a, $b: null) { color: red; }')
    expect(ast).toMatchSnapshot()
  })
})
describe('sink', () => {
  it('works', () => {
    const ast = createAST(`
    $base-font-family: 'ProximaNova' !default;
    @mixin font-face($font-family, $file-name, $baseurl, $weight: 500, $style: normal ){
      @font-face {
        font: {
          family: $font-family;
          weight: $weight;
          style: $style;
        }
        src: url( $baseurl + $file-name + '.eot');
        src: url( $baseurl + $file-name + '.eot?#iefix') format('embedded-opentype');
        src: url( $baseurl + $file-name + '.woff') format('woff'),
               url( $baseurl + $file-name + '.woff2') format('woff2'),
               url( $baseurl + $file-name + '.ttf') format('truetype'),
               url( $baseurl + $file-name + '.svg' + '#' + $file-name) format('svg');
      }
    }
    `)
    expect(ast).toMatchSnapshot()
  })
})
