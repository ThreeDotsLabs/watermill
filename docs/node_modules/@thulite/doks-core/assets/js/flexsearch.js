/*!
 * FlexSearch for Bootstrap based Thulite sites
 * Copyright 2021-2024 Thulite
 * Licensed under the MIT License
 * Based on https://github.com/frjo/hugo-theme-zen/blob/main/assets/js/search.js
 */

/* eslint-disable no-undef, guard-for-in */

/**
 * @file
 * A JavaScript file for flexsearch.
 */

// import * as FlexSearch from 'flexsearch';
import Index from 'flexsearch';

(function () {

  'use strict';

  // const index = new FlexSearch.Document({
  const index = new Index.Document({
    tokenize: 'forward',
    document: {
      id: 'id',
      index: [
        {
          field: 'title'
        },
        {
          field: 'tags'
        },
        {
          field: {{ if site.Params.doks.indexSummary }}'summary'{{ else }}'content'{{ end }}
        },
        {
          field:  'date',
          tokenize: 'strict',
          encode: false
        }
      ],
      store: ['title','summary','date','permalink']
    }
  });

  function showResults(items) {
    const template = document.querySelector('template').content;
    const fragment = document.createDocumentFragment();

    const results = document.querySelector('.search-results');
    results.textContent = '';

    const itemsLength = Object.keys(items).length;

    // Show/hide "No recent searches" and "No search results" messages
    if ((itemsLength === 0) && (query.value === '')) {
      // Hide "No search results" message
      document.querySelector('.search-no-results').classList.add('d-none');
      // Show "No recent searches" message
      document.querySelector('.search-no-recent').classList.remove('d-none');
    } else if ((itemsLength === 0) && (query.value !== '')) {
      // Hide "No recent searches" message
      document.querySelector('.search-no-recent').classList.add('d-none');
      // Show "No search results" message
      const queryNoResults = document.querySelector('.query-no-results');
      queryNoResults.innerText = query.value;
      document.querySelector('.search-no-results').classList.remove('d-none');
    } else {
      // Hide both "No recent searches" and "No search results" messages
      document.querySelector('.search-no-recent').classList.add('d-none');
      document.querySelector('.search-no-results').classList.add('d-none');
    }

    for (const id in items) {
      const item = items[id];
      const result = template.cloneNode(true);
      const a = result.querySelector('a');
      const time = result.querySelector('time');
      const content = result.querySelector('.content');
      a.innerHTML = item.title;
      a.href = item.permalink;
      time.innerText = item.date;
      content.innerHTML = item.summary;
      fragment.appendChild(result);
    }

    results.appendChild(fragment);
  }

  function doSearch() {
    const query = document.querySelector('.search-text').value.trim();
    const limit = {{ .searchLimit }};
    const results = index.search({
      query: query,
      enrich: true,
      limit: limit,
    });
    const items = {};

    results.forEach(function (result) {
      result.result.forEach(function (r) {
        items[r.id] = r.doc;
      });
    });

    showResults(items);
  }

  function enableUI() {
    const searchform = document.querySelector('.search-form');
    searchform.addEventListener('submit', function (e) {
      e.preventDefault();
      doSearch();
    });
    searchform.addEventListener('input', function () {
      doSearch();
    });
    document.querySelector('.search-loading').classList.add('d-none');
    document.querySelector('.search-input').classList.remove('d-none');
    document.querySelector('.search-text').focus();
  }

  function buildIndex() {
    document.querySelector('.search-loading').classList.remove('d-none');
    fetch("{{ site.LanguagePrefix }}/search-index.json")
      .then(function (response) {
        return response.json();
      })
      .then(function (data) {
        data.forEach(function (item) {
          index.add(item);
        });
      });
  }

  buildIndex();
  enableUI();
})();
