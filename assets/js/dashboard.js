class Dashboard {
    showingApiKey = false
    localTime = false
    apiKey = null

    constructor({apiKey}) {
        this.apiKey = apiKey
        this.initApiClipboard()
        this.dateAdd()
        this.dateSwap()
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
        document.getElementById("api-key").innerHTML = this.toggleApiKey(this.showingApiKey)
    }

    toggleApiKey = (bool) => {
        return bool ? `CLICK ME` : this.apiKey
    }

    dateSwap = () => {
        this.localTime = !this.localTime
        document.getElementById("swap-date").innerHTML = this.timeToggle(this.localTime)

        var utcs = document.getElementsByClassName("utc")

        for (var i = 0; i < utcs.length; i++) {
            utcs[i].classList.toggle("d-none")
        }

        var local_times = document.getElementsByClassName("local-time")

        for (var i = 0; i < local_times.length; i++) {
            local_times[i].classList.toggle("d-none")
        }
    }

    timeToggle = (bool) => {
        switch (bool) {
            case true:
                return `<span id="swap-date"><i class="fa fa-toggle-on pointer-cursor" aria-hidden="true"></i></span>`
                break
            case false:
                return `<span id="swap-date"><i class="fa fa-toggle-off pointer-cursor" aria-hidden="true"></i></span>`
        }
    }


    dateAdd = () => {
        switch (this.localTime) {
            case true:
                var timestamps = document.getElementsByClassName("log-datestamp")

                for (var i = 0; i < timestamps.length; i++) {
                    let time = timestamps[i]
                    let local_time = formatLocalTime(time.innerHTML / 1000)
                    let utc_time = (date = new Date()) => {
                        return new Date((time.innerHTML / 1000) + date.getTimezoneOffset() * 60 * 1000)
                    }
                    timestamps[i].innerHTML = `<span class="local-time">${local_time}</span><span class="utc d-none">${formatLocalTime(utc_time())} UTC</span>`
                }
                break
            case false:
                var timestamps = document.getElementsByClassName("log-datestamp")

                for (var i = 0; i < timestamps.length; i++) {
                    let time = timestamps[i]
                    let local_time = formatLocalTime(time.innerHTML / 1000)
                    let utc_time = (date = new Date()) => {
                        return new Date((time.innerHTML / 1000) + date.getTimezoneOffset() * 60 * 1000)
                    }
                    timestamps[i].innerHTML = `<span class="local-time d-none">${local_time}</span><span class="utc">${formatLocalTime(utc_time())} UTC</span>`
                }
        }
    }


}

export default Dashboard
