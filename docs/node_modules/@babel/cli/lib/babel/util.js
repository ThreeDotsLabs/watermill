"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.addSourceMappingUrl = addSourceMappingUrl;
exports.alphasort = alphasort;
exports.chmod = chmod;
exports.compile = compile;
exports.debounce = debounce;
exports.deleteDir = deleteDir;
exports.hasDataSourcemap = hasDataSourcemap;
exports.isCompilableExtension = isCompilableExtension;
exports.readdir = readdir;
exports.readdirForCompilable = readdirForCompilable;
exports.transformRepl = transformRepl;
exports.withExtension = withExtension;
function _fsReaddirRecursive() {
  const data = require("fs-readdir-recursive");
  _fsReaddirRecursive = function () {
    return data;
  };
  return data;
}
function babel() {
  const data = require("@babel/core");
  babel = function () {
    return data;
  };
  return data;
}
function _path() {
  const data = require("path");
  _path = function () {
    return data;
  };
  return data;
}
function _fs() {
  const data = require("fs");
  _fs = function () {
    return data;
  };
  return data;
}
var watcher = require("./watcher.js");
function asyncGeneratorStep(n, t, e, r, o, a, c) { try { var i = n[a](c), u = i.value; } catch (n) { return void e(n); } i.done ? t(u) : Promise.resolve(u).then(r, o); }
function _asyncToGenerator(n) { return function () { var t = this, e = arguments; return new Promise(function (r, o) { var a = n.apply(t, e); function _next(n) { asyncGeneratorStep(a, r, o, _next, _throw, "next", n); } function _throw(n) { asyncGeneratorStep(a, r, o, _next, _throw, "throw", n); } _next(void 0); }); }; }
function chmod(src, dest) {
  try {
    _fs().chmodSync(dest, _fs().statSync(src).mode);
  } catch (_) {
    console.warn(`Cannot change permissions of ${dest}`);
  }
}
function alphasort(a, b) {
  return a.localeCompare(b, "en");
}
function readdir(dirname, includeDotfiles, filter) {
  {
    return _fsReaddirRecursive()("", (filename, index, currentDirectory) => {
      const stat = _fs().statSync(_path().join(currentDirectory, filename));
      if (stat.isDirectory()) return true;
      return (includeDotfiles || filename[0] !== ".") && (!filter || filter(filename));
    }, [], dirname);
  }
}
function readdirForCompilable(dirname, includeDotfiles, altExts) {
  return readdir(dirname, includeDotfiles, function (filename) {
    return isCompilableExtension(filename, altExts);
  });
}
function isCompilableExtension(filename, altExts) {
  const exts = altExts || babel().DEFAULT_EXTENSIONS;
  const ext = _path().extname(filename);
  return exts.includes(ext);
}
function addSourceMappingUrl(code, loc) {
  return code + "\n//# sourceMappingURL=" + _path().basename(loc);
}
function hasDataSourcemap(code) {
  const pos = code.lastIndexOf("\n", code.length - 2);
  return pos !== -1 && code.lastIndexOf("//# sourceMappingURL") < pos;
}
const CALLER = {
  name: "@babel/cli"
};
function transformRepl(filename, code, opts) {
  opts = Object.assign({}, opts, {
    caller: CALLER,
    filename
  });
  return new Promise((resolve, reject) => {
    babel().transform(code, opts, (err, result) => {
      if (err) reject(err);else resolve(result);
    });
  });
}
function compile(_x, _x2) {
  return _compile.apply(this, arguments);
}
function _compile() {
  _compile = _asyncToGenerator(function* (filename, opts) {
    opts = Object.assign({}, opts, {
      caller: CALLER
    });
    const result = yield new Promise((resolve, reject) => {
      babel().transformFile(filename, opts, (err, result) => {
        if (err) reject(err);else resolve(result);
      });
    });
    if (result) {
      {
        if (!result.externalDependencies) return result;
      }
      watcher.updateExternalDependencies(filename, result.externalDependencies);
    }
    return result;
  });
  return _compile.apply(this, arguments);
}
function deleteDir(path) {
  (_fs().rmSync || function d(p) {
    if (_fs().existsSync(p)) {
      _fs().readdirSync(p).forEach(function (f) {
        const c = p + "/" + f;
        if (_fs().lstatSync(c).isDirectory()) {
          d(c);
        } else {
          _fs().unlinkSync(c);
        }
      });
      _fs().rmdirSync(p);
    }
  })(path, {
    force: true,
    recursive: true
  });
}
process.on("uncaughtException", function (err) {
  console.error(err);
  process.exitCode = 1;
});
function withExtension(filename, ext = ".js") {
  const newBasename = _path().basename(filename, _path().extname(filename)) + ext;
  return _path().join(_path().dirname(filename), newBasename);
}
function debounce(fn, time) {
  let timer;
  function debounced() {
    clearTimeout(timer);
    timer = setTimeout(fn, time);
  }
  debounced.flush = () => {
    clearTimeout(timer);
    fn();
  };
  return debounced;
}

//# sourceMappingURL=util.js.map
