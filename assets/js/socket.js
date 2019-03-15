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
  document.getElementById('logs-list').appendChild(renderedLog)

  dateAddNew();
  if (window.scrollTracker == true) {
    scrollBottom();
  }
}

function logTemplate(event) {
  var timestamp = `${event.timestamp}`
  var logMessage = ` ${event.log_message}`
  var newLi = document.createElement("li");
  var newMark = document.createElement("mark");
  var newTimestamp = document.createTextNode(timestamp);
  var newLogMessage = document.createTextNode(logMessage);

  newMark.appendChild(newTimestamp)
  newMark.className = 'new-log';
  newLi.appendChild(newMark)
  newLi.appendChild(newLogMessage)

  return newLi
}

window.createSocket = createSocket;

function createDashboardSocket(sourceToken) {
  let channel = socket.channel(`dashboard:${sourceToken}`, {})
  channel.join()
    .receive("ok", resp => { console.log("Dashboard socket joined successfully", resp) })
    .receive("error", resp => { console.log("Unable to join", resp) })

  channel.on(`dashboard:${sourceToken}:log_count`, swapBadge);
  channel.on(`dashboard:${sourceToken}:rate`, swapRate);
};

function swapBadge(event) {
  var badge = document.getElementById(event.source_token);

  badge.innerHTML = `<small class="my-badge fade-in">${event.log_count}</small>`;
}

function swapRate(event) {
  var rate = document.getElementById(`${event.source_token}-rate`);
  var avgRate = document.getElementById(`${event.source_token}-avg-rate`);
  var peakRate = document.getElementById(`${event.source_token}-max-rate`);

  rate.innerHTML = `${event.rate}`
  avgRate.innerHTML = `${event.average_rate}`
  peakRate.innerHTML = `${event.max_rate}`
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

  counter.innerHTML = `<span class="flash-text">${event.total_logs_logged}</span>`;
}

window.createEveryoneSocket = createEveryoneSocket;
