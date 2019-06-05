import {Socket} from "phoenix"

let socket = new Socket("/socket", {params: {token: window.userToken, public_token: window.publicToken}})

socket.connect()

function createEveryoneSocket() {
    let channel = socket.channel(`everyone`, {})
    channel.join()
        .receive("ok", resp => {
            console.log("Everyone socket joined successfully", resp)
        })
        .receive("error", resp => {
            console.log("Unable to join", resp)
        })

    channel.on(`everyone:update`, swapCount)
}

function swapCount(event) {
    var counter = document.getElementById("total-logged")

    counter.innerHTML = `<span class="flash-text">${event.total_logs_logged}</span>`
}

window.createEveryoneSocket = createEveryoneSocket

export default socket
