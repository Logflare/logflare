import socket from "../socket"
import {useLocalTime} from "../user-config-storage"

const joinSourceChannel = (sourceToken) => {
    let channel = socket.channel(`source:${sourceToken}`, {})
    channel.join()
        .receive("ok", resp => {
            console.log("Source socket joined successfully", resp)
        })
        .receive("error", resp => {
            console.log("Unable to join", resp)
        })

    channel.on(`source:${sourceToken}:new`, renderLog)
}

async function dateAddNew() {
    var timestamps = document.getElementsByClassName("new-log")
    var last = timestamps[timestamps.length - 1]
    const local_time = formatLocalTime(last.innerHTML / 1000)
    const utc_time = (date = new Date()) => {
        return new Date((last.innerHTML / 1000) + date.getTimezoneOffset() * 60 * 1000)
    }

    if (await useLocalTime()) {
        last.innerHTML = `<span class="local-time"> ${local_time}</span><span class="utc d-none">${formatLocalTime(utc_time())} UTC </span>`
    } else {
        last.innerHTML = `<span class="local-time d-none"> ${local_time}</span> <span class="utc">${formatLocalTime(utc_time())} UTC </span>`

    }
}


function formatLocalTime(date) {
    return dateFns.format(date, "ddd MMM D YYYY hh:mm:ssa")
}


function renderLog(event) {
    const renderedLog = logTemplate(event)

    document.querySelector("#no-logs-warning").innerHTML = ""
    document.getElementById("logs-list").appendChild(renderedLog)

    dateAddNew()
    if (window.scrollTracker) {
        scrollBottom()
    }
}

function scrollBottom() {
    const y = document.body.clientHeight

    window.scrollTo(0, y)
}


function logTemplate(event) {
    var timestamp = `${event.timestamp}`
    var logMessage = ` ${event.log_message} `
    var newLi = document.createElement("li")
    var newMark = document.createElement("mark")
    var newDataExpand = document.createElement("a")
    var newTimestamp = document.createTextNode(timestamp)
    var newLogMessage = document.createTextNode(logMessage)

    newMark.appendChild(newTimestamp)
    newMark.className = "new-log"

    newLi.appendChild(newMark)
    newLi.appendChild(newLogMessage)

    if (event.metadata) {
        var logData = JSON.stringify(event.metadata, null, 2)
        var logDataLink = `metadata`
        var newLogDataLink = document.createTextNode(logDataLink)
        var newLogData = document.createTextNode(logData)

        var expandId = makeid(10)

        var newCode = document.createElement("code")
        var newPre = document.createElement("pre")
        newPre.className = "pre-metadata"
        var newDiv = document.createElement("div")
        newDiv.className = "collapse metadata"
        newDiv.id = `${expandId}`
        newCode.appendChild(newLogData)
        newPre.appendChild(newCode)
        newDiv.appendChild(newPre)

        newDataExpand.appendChild(newLogDataLink)
        newDataExpand.href = `#${expandId}`
        newDataExpand.className = "metadata-link"
        newDataExpand.dataset.toggle = "collapse"

        newLi.appendChild(newDataExpand)
        newLi.appendChild(newDiv)
    }

    return newLi
}

function makeid(length) {
    var text = ""
    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

    for (var i = 0; i < length; i++)
        text += possible.charAt(Math.floor(Math.random() * possible.length))

    return text
}

const Source = {
    joinSourceChannel
}

export default Source
