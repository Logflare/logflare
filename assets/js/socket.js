import { Socket } from "phoenix"

let socket = new Socket("/socket", {
    params: { token: window.userToken, public_token: window.publicToken },
})

socket.connect()

export default socket
