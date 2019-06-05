import $ from "jquery"
import * as userConfig from "./user-config-storage"
import socket from "./socket"

console.log(window.userSources)

class Dashboard {
    showingApiKey = false
    apiKey = null
    sourceTokens = []


    constructor({apiKey, sourceTokens = []}) {
        this.apiKey = apiKey
        Dashboard.initSourceTokenClipboards()
        Dashboard.initApiClipboard()
        this.dateAdd()
        this.sourceTokens = sourceTokens

        for (let token of sourceTokens) {
            this.joinSourceChannel(token)
        }
    }

    static initSourceTokenClipboards = () => {
        const clipboard = new ClipboardJS(".copy-token")

        clipboard.on("success", (e) => {
            alert("Copied: " + e.text)
            e.clearSelection()
        })

        clipboard.on("error", (e) => {
            e.clearSelection()
        })
    }

    static initApiClipboard = () => {
        const clipboard = new ClipboardJS("#api-key")
        clipboard.on("success", (e) => {
            this.showApiKey()
            alert("Copied: " + e.text)
            e.clearSelection()
        })

        clipboard.on("error", (e) => {
            this.showApiKey()
            e.clearSelection()
        })
    }

    showApiKey = () => {
        this.showingApiKey = !this.showingApiKey
        $("#api-key").html(this.showingApiKey ? this.apiKey : `CLICK ME`)
    }

    dateSwap = async () => {
        await userConfig.flipUseLocalTime()
        $("#swap-date > svg").toggleClass("fa-toggle-off").toggleClass("fa-toggle-on")

        const utcs = $(".utc")

        for (let utc of utcs) {
            utc.classList.toggle("d-none")
        }

        const local_times = $(".local-time")

        for (let lt of local_times) {
            lt.classList.toggle("d-none")
        }
    }

    formatLocalTime = (date) => `${dateFns.distanceInWordsToNow(date)} ago`

    dateAdd = async () => {
        const timestamps = $(".log-datestamp")

        for (let time of timestamps) {
            const timestamp = $(time).html() / 1000
            if (await userConfig.useLocalTime()) {
                let local_time = this.formatLocalTime(timestamp)
                $(time).html(`<span class="local-time">${local_time}</span>`)
            } else {
                let utc_time = new Date((timestamp) + (new Date).getTimezoneOffset() * 60 * 1000)
                $(time).html(`<span class="utc d-none">${this.formatLocalTime(utc_time)} UTC</span>`)
            }
        }
    }

    joinSourceChannel = (sourceToken) => {
        let channel = socket.channel(`dashboard:${sourceToken}`, {})

        channel.join()
            .receive("ok", resp => {
                console.log("Dashboard socket joined successfully", resp)
            })
            .receive("error", resp => {
                console.log("Unable to join", resp)
            })

        const sourceSelector = `#${sourceToken}`

        channel.on(`dashboard:${sourceToken}:log_count`, (event) => {
            $(`${sourceSelector}-latest`).html(this.formatLocalTime(new Date()))
            $(sourceSelector).html(`<small class="my-badge fade-in">${event.log_count}</small>`)
        })

        channel.on(`dashboard:${sourceToken}:rate`, (event) => {
            $(`${sourceSelector}-rate`).html(`${event.rate}`)
            $(`${sourceSelector}-avg-rate`).html(`${event.average_rate}`)
            $(`${sourceSelector}-max-rate`).html(`${event.max_rate}`)

        })

        channel.on(`dashboard:${sourceToken}:buffer`, (event) => {
            $(`${sourceSelector}-buffer`).html(`${event.buffer}`)
        })
    }


}


export default Dashboard
