// @ts-check
const { createRequire } = require('node:module')
const { pathToFileURL } = require('node:url')

const TS_EXT_RE = /\.[mc]?ts$/

let tsx

let jiti

let importError

/**
 * @param {string} name
 * @param {string} rootFile
 * @returns {Promise<any>}
 */
async function req(name, rootFile = __filename) {
  let url = createRequire(rootFile).resolve(name)

  try {
    return (await import(`${pathToFileURL(url)}?t=${Date.now()}`)).default
  } catch (err) {
    if (!TS_EXT_RE.test(url)) {
      /* c8 ignore start */
      throw err
    }
  }

  if (tsx === undefined) {
    tsx = await import('tsx/cjs/api').catch(error => {
      importError = error
    })
  }

  if (tsx) {
    let loaded = tsx.require(name, rootFile)
    return loaded && '__esModule' in loaded ? loaded.default : loaded
  }

  if (jiti === undefined) {
    jiti = await import('jiti').then(
      m => m.default,
      error => {
        importError = importError ?? error
      }
    )
  }

  if (jiti) {
    return jiti(rootFile, { interopDefault: true })(name)
  }

  throw new Error(
    `'tsx' or 'jiti' is required for the TypeScript configuration files. Make sure it is installed\nError: ${importError.message}`
  )
}

module.exports = req
