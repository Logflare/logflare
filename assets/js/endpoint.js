export async function query(query, params) {
  let queryResult = document.getElementById("queryResult")
  let url = new URL(`/endpoints/query/${query}`, window.location.origin)
  url.search = new URLSearchParams(params)
  fetch(url, {
      headers: {
          'Content-Type': 'application/json'
      }
  })
      .then(response => response.json())
      .then(data => {
          if (typeof data.error !== 'undefined') {
             queryResult.innerHTML = `<div style="color: red">${data.error.message}</div>`
          } else {
             queryResult.innerHTML = `<code><pre class="p-2":w
             >${JSON.stringify(data.result, null, '\t')}</pre></code>`
          }
      })
}