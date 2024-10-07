const { parse } = require('scss-parser')
const parse2 = require('gonzales-pe').parse
const { readFileSync } = require('fs')
const globAll = require('glob-all')

const shouldParse = ['rule', 'selector', 'block']
const shouldKeep = ['id', 'class', 'attribute']
const shouldKeep2 = ['id', 'class', 'attribute', 'typeSelector']

/*
  The pcss file extension is a PostCSS extension.
  They currently have no official documentation around it so
  using it with this tool is completely at your own risk.
  I mean, villages won't burn if you use it but your code might error out. YMMV.
*/
const exts = ['css', 'sass', 'scss', 'less', 'pcss']

function makeWhitelist(filenames) {
  filenames = sanitizeArgs(filenames)
  if (!filenames.length) return []

  // Create a deep array, each level containing a list of selectors.
  const deepArray = filenames.reduce((acc, filename) => {
    // Do nothing for non-style files.
    const ext = filename.split('.').pop()
    if (!exts.includes(ext)) return acc

    // File contents.
    const fileContents = readFileSync(filename, 'utf-8')

    // 1st try `scss-parser`.
    try {
      const parsedData = parse(fileContents).value
      const selectors = parseStyleAST(parsedData)
      return acc.concat(selectors)

    // 2nd try `gonzales-pe`.
    } catch(e) {
      const parsed = parse2(fileContents, { syntax: ext })
      const nodes = []

      // Built-in traversal method, no need to recursively
      // traverse the tree, cherry pick, and flatten the results!
      parsed.traverse(node => {
        if (shouldKeep2.includes(node.type)) {
          const thing = node.content.find(({ type }) => type === 'ident')
          if (thing) nodes.push(thing.content)
        }
      })

      return [...acc, ...new Set(nodes)]
    }
  }, [])

  // Flatten the array.
  const flattenedArray = flattenArray(deepArray)

  // Return an array of unique selectors in alphabetical order.
  return [...new Set(flattenedArray)].sort()
}

function sanitizeArgs(arr) {
  arr = (Array.isArray(arr) ? arr : [arr]).filter(Boolean)

  // Avoids errors if an empty array, no arguments, or falsey things are passed.
  if (!arr.length) {
    console.log('\n\nNo items for processing. Moving right along...\n\n')
    return []
  }

  // Each thing in the array must be a string.
  if (arr.some(s => typeof s !== 'string')) throw `Oops! Something passed wasn't a string.`

  // Ensure absolute paths for filenames, especially if globs were passed.
  arr = globAll.sync(arr, { absolute: true })

  // If, at the end of it all, we have nothing, leave empty-handed.
  if (!arr.length) {
    console.log('\n\nNo matching files found.\n\n')
    return []
  }

  return arr
}

function parseStyleAST(arr) {
  return arr.reduce((acc, { type, value }) => {
    // Trigger recursion for types that need it.
    if (shouldParse.includes(type)) {
      return acc.concat(parseStyleAST(value))

    // Iterate through a type's values to extract selectors.
    } else if (shouldKeep.includes(type)) {
      return value.reduce((acc, { type, value }) => (
        (type === 'identifier' && !!value) ?  acc.concat(value) : acc
      ), acc)

    // Concatenate a type's value if no iteration is needed.
    } else if (type === 'identifier' && !!value) {
      return acc.concat(value)

    // No matches - acc is unchanged.
    // This allows us to skip filtering out falsy's later.
    } else {
      return acc
    }
  }, [])
}

function flattenArray(arr) {
  if (!Array.isArray(arr)) return arr

  return arr.reduce((acc, thing) => {
    return Array.isArray(thing) ? acc.concat(flattenArray(thing)) : acc.concat(thing)
  }, [])
}

module.exports = makeWhitelist
