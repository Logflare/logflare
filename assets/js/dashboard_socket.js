import {Socket} from "phoenix"

let dashboardSocket = new Socket("/socket", {params: {token: window.userToken}})

dashboardSocket.connect()

function createDashboardSocket() {
  let channel = dashboardSocket.channel(`dashboard`, {})
  channel.join()
    .receive("ok", resp => { console.log("Dashboard socket joined successfully", resp) })
    .receive("error", resp => { console.log("Unable to join", resp) })

  channel.on(`dashboard:update`, swapBadge);
};

function swapBadge(event) {
  var badge = document.getElementById(event.source_token);

  badge.innerHTML = `<small>${event.log_count}</small>`;

  console.log("Updated source count: " + event.source_token)
}

window.createDashboardSocket = createDashboardSocket;
