import {Socket} from "phoenix"

let socket = new Socket("/socket", {params: {token: window.userToken}})

socket.connect()

const createSocket = (sourceToken) => {
  let channel = socket.channel(`source:${sourceToken}`, {})
  channel.join()
    .receive("ok", resp => { console.log("Joined successfully", resp) })
    .receive("error", resp => { console.log("Unable to join", resp) })

  channel.on(`source:${sourceToken}:new`, renderLog);
};

function renderLog(event) {
  const renderedLog = logTemplate(event)

  document.querySelector('#no-logs-warning').innerHTML = '';
  document.querySelector('.list-unstyled').innerHTML += renderedLog;
}

function logTemplate(event) {
  return `
    <li class="collection-item">
      <mark>${event.timestamp}</mark> ${event.log_message}
    </li>
  `;
}

window.createSocket = createSocket;
