import JSONFormatter from "json-formatter-js"

let hooks = {}

hooks.MetadataJsonViewer = {
  mounted() {
    const json = JSON.parse(this.el.innerText)
    const formatter = new JSONFormatter(json, 3, {theme: "dark"})
    document.getElementById("metadata-viewer").appendChild(formatter.render())
  },
}

export default hooks
