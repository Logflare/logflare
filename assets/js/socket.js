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
  var logMessage = ` ${event.log_message} `
  var newLi = document.createElement("li");
  var newMark = document.createElement("mark");
  var newDataExpand = document.createElement("a");
  var newTimestamp = document.createTextNode(timestamp);
  var newLogMessage = document.createTextNode(logMessage);

  newMark.appendChild(newTimestamp)
  newMark.className = 'new-log'

  newLi.appendChild(newMark)
  newLi.appendChild(newLogMessage)

  if (event.metadata != undefined) {
    var logData = JSON.stringify(event.metadata, null, 2)
    var logDataLink = `metadata`
    var newLogDataLink = document.createTextNode(logDataLink);
    var newLogData = document.createTextNode(logData);

    var expandId = makeid(10);

    var newCode = document.createElement("code");
    var newPre = document.createElement("pre");
    newPre.className = 'pre-metadata';
    var newDiv = document.createElement("div");
    newDiv.className = 'collapse metadata';
    newDiv.id = `${expandId}`
    newCode.appendChild(newLogData)
    newPre.appendChild(newCode)
    newDiv.appendChild(newPre)

    newDataExpand.appendChild(newLogDataLink)
    newDataExpand.href = `#${expandId}`
    newDataExpand.className = 'metadata-link'
    newDataExpand.dataset.toggle = 'collapse'

    newLi.appendChild(newDataExpand)
    newLi.appendChild(newDiv)
  }

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
  channel.on(`dashboard:${sourceToken}:buffer`, swapBuffer);
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

function swapBuffer(event) {
  var buffer = document.getElementById(`${event.source_token}-buffer`);

  buffer.innerHTML = `${event.buffer}`
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

function makeid(length) {
  var text = "";
  var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  for (var i = 0; i < length; i++)
    text += possible.charAt(Math.floor(Math.random() * possible.length));

  return text;
}
