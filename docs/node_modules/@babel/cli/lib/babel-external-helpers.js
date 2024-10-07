"use strict";

function commander() {
  const data = _interopRequireWildcard(require("commander"), true);
  commander = function () {
    return data;
  };
  return data;
}
function _core() {
  const data = require("@babel/core");
  _core = function () {
    return data;
  };
  return data;
}
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function (e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != typeof e && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && {}.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
const program = commander().default.program;
function collect(value, previousValue) {
  if (typeof value !== "string") return previousValue;
  const values = value.split(",");
  if (previousValue) {
    previousValue.push(...values);
    return previousValue;
  }
  return values;
}
program.option("-l, --whitelist [whitelist]", "Whitelist of helpers to ONLY include", collect);
program.option("-t, --output-type [type]", "Type of output (global|umd|var)", "global");
program.usage("[options]");
program.parse(process.argv);
const opts = program.opts();
console.log((0, _core().buildExternalHelpers)(opts.whitelist, opts.outputType));

//# sourceMappingURL=babel-external-helpers.js.map
