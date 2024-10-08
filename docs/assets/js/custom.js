// Put your custom JS code here

// a bit hacky way to force dark mode by default
// it sets local storage item used by docs/node_modules/@thulite/doks-core/assets/js/color-mode.js
if (!localStorage.getItem('theme')) {
  localStorage.setItem('theme', 'dark');
}
