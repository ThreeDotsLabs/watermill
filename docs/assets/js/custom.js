// Put your custom JS code here

// a bit hacky way to force dark mode by default
// it sets local storage item used by docs/node_modules/@thulite/doks-core/assets/js/color-mode.js
if (!localStorage.getItem('theme')) {
  localStorage.setItem('theme', 'dark');
}

import { render } from 'github-buttons';

let renderGitHubButton= () => {
  let oldButton = document.getElementById("github-button");
  if (oldButton) {
    oldButton.remove();
  }

  let options = {
    "href": "https://github.com/ThreeDotsLabs/watermill",
    "data-show-count": true,
    "data-size": "large",
    "data-color-scheme": localStorage.getItem('theme'),
  }

  render(options, function (el) {
    let menu = document.getElementById("offcanvasNavMain").querySelector(".offcanvas-body");
    let searchToggle = document.getElementById("searchToggleDesktop");

    el.setAttribute("id", "github-button");
    el.classList.add("nav-link", "px-2", "mx-auto");
    el.setAttribute("style", "margin-top: 12px;");


    menu.insertBefore(el, searchToggle);
  })
}
renderGitHubButton()
