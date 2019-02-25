import {Socket} from "phoenix"

let socket = new Socket("/socket", {params: {token: window.userToken, public_token: window.publicToken}})

socket.connect()

const createSocket = (sourceToken) => {
  let channel = socket.channel(`source:${sourceToken}`, {})
  channel.join()
    .receive("ok", resp => { console.log("Source socket joined successfully", resp) })
    .receive("error", resp => { console.log("Unable to join", resp) })

  channel.on(`source:${sourceToken}:new`, renderLog);
};

function renderLog(event) {
  const renderedLog = logTemplate(event)

  document.querySelector('#no-logs-warning').innerHTML = '';
  document.querySelector('.list-unstyled').innerHTML += renderedLog;
  dateAddNew();
  if (window.scrollTracker == true) {
    scrollBottom();
  }
}

function logTemplate(event) {
  return `
    <li>
      <mark class="new-log">${event.timestamp}</mark> ${event.log_message}
    </li>
  `;
}

window.createSocket = createSocket;

function createDashboardSocket(sourceToken) {
  let channel = socket.channel(`dashboard:${sourceToken}`, {})
  channel.join()
    .receive("ok", resp => { console.log("Dashboard socket joined successfully", resp) })
    .receive("error", resp => { console.log("Unable to join", resp) })

  channel.on(`dashboard:${sourceToken}:update`, swapBadge);
};

function swapBadge(event) {
  var badge = document.getElementById(event.source_token);

  badge.innerHTML = `<small class="my-badge fade-in">${event.log_count}</small>`;
}

window.createDashboardSocket = createDashboardSocket;

function createEveryoneSocket() {
  let channel = socket.channel(`everyone`, {})
  channel.join()
    .receive("ok", resp => { console.log("Everyone socket joined successfully", resp) })
    .receive("error", resp => { console.log("Unable to join", resp) })

  channel.on(`everyone:update`, swapCount);
};

function swapCount(event) {
  var counter = document.getElementById('total-logged');

  counter.innerHTML = `${event.total_logs_logged}`;
}

window.createEveryoneSocket = createEveryoneSocket;
