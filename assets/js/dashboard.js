import $ from "jquery"
import * as userConfig from "./user-config-storage"

class Dashboard {
    showingApiKey = false
    apiKey = null

    constructor({apiKey}) {
        this.apiKey = apiKey
        this.initApiClipboard()
        this.dateAdd()
    }

    initApiClipboard = () => {
        const clipboard = new ClipboardJS("#api-key")
        clipboard.on("success", (e) => {
            this.showApiKey()
            alert("Copied: " + apiKey)
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
        const useLocalTime = await userConfig.useLocalTime()
        this.timeToggle(useLocalTime)

        const utcs = $(".utc")

        for (let utc of utcs) {
            utc.classList.toggle("d-none")
        }

        const local_times = $(".local-time")

        for (let lt of local_times) {
            lt.classList.toggle("d-none")
        }
    }

    timeToggle = (bool) => {
        const swapDate = $("#swap-date > svg")
        return bool ?
            swapDate.addClass("fa-toggle-off").removeClass("fa-toggle-on")
            :
            swapDate.removeClass("fa-toggle-off").addClass("fa-toggle-on")
    }

    dateAdd = async () => {
        if (await userConfig.useLocalTime()) {
            const timestamps = $(".log-datestamp")

            for (let time of timestamps) {
                const timestamp = $(time).html() / 1000
                let local_time = formatLocalTime(timestamp)
                let utc_time = (date = new Date()) => new Date((timestamp) + date.getTimezoneOffset() * 60 * 1000)
                $(time).html(`<span class="local-time">${local_time}</span><span class="utc d-none">${formatLocalTime(utc_time())} UTC</span>`)
            }
        } else {
            const timestamps = document.getElementsByClassName("log-datestamp")

            for (let time of timestamps) {
                const timestamp = $(time).html() / 1000
                let local_time = formatLocalTime(timestamp)
                let utc_time = (date = new Date()) => new Date((timestamp) + date.getTimezoneOffset() * 60 * 1000)
                $(time).html(`<span class="local-time d-none">${local_time}</span><span class="utc">${formatLocalTime(utc_time())} UTC</span>`)
            }
        }
    }
}


function formatLocalTime(date) {
    return dateFns.format(date, "ddd MMM D YYYY hh:mm:ssa")
}

export default Dashboard
