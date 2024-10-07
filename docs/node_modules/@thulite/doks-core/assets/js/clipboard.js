/*!
 * clipboard.js for Bootstrap based Thulite sites
 * Copyright 2021-2024 Thulite
 * Licensed under the MIT License
 */

import Clipboard from 'clipboard';

(() => {
    'use strict';

    var cb = document.getElementsByClassName('highlight');

    for (var i = 0; i < cb.length; ++i) {
        var element = cb[i];
        element.insertAdjacentHTML('afterbegin', '<div class="copy"><button title="Copy to clipboard" class="btn-copy" aria-label="Clipboard button"><div></div></button></div>');
    }

    var clipboard = new Clipboard('.btn-copy', {
        target: function (trigger) {
            return trigger.parentNode.nextElementSibling;
        }
    });

    clipboard.on('success', function (e) {
        /*
      console.info('Action:', e.action);
      console.info('Text:', e.text);
      console.info('Trigger:', e.trigger);
      */

        e.clearSelection();
    });

    clipboard.on('error', function (e) {
        console.error('Action:', e.action);
        console.error('Trigger:', e.trigger);
    });
})();
