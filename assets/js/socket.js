import {Socket} from "phoenix"

let socket = new Socket("/socket", {params: {token: window.userToken}})

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
  stayScrolledBottom();
}

function logTemplate(event) {
  return `
    <li>
      <mark class="new-log">${event.timestamp}</mark> ${event.log_message}
    </li>
  `;
}

window.createSocket = createSocket;

function createDashboardSocket() {
  let channel = socket.channel(`dashboard`, {})
  channel.join()
    .receive("ok", resp => { console.log("Dashboard socket joined successfully", resp) })
    .receive("error", resp => { console.log("Unable to join", resp) })

  channel.on(`dashboard:update`, swapBadge);
};

function swapBadge(event) {
  var badge = document.getElementById(event.source_token);

  badge.innerHTML = `<small class="my-badge fade-in">${event.log_count}</small>`;

  console.log("Updated source count: " + event.source_token)
}

window.createDashboardSocket = createDashboardSocket;
