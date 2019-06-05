class Dashboard {
    showingApiKey = false
    localTime = false
    apiKey = null

    constructor({apiKey}) {
        this.apiKey = apiKey
        this.initApiClipboard()
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
}

export default Dashboard
