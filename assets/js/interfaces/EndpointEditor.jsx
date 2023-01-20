import { useState } from "react"

const EndpointEditor = ({
  pushEvent,
  endpoint = {},
  defaultValues = {},
  queryResult = null,
}) => {
  const [queryParams, setQueryParams] = useState({
    name: endpoint.name || defaultValues.name,
    query: endpoint.query
  })
  const handleSubmit = () => {
    pushEvent("save-endpoint", {
      endpoint: queryParams,
    })
  }
  const handleCancel = () =>
    pushEvent("show-endpoint", {endpoint_id: endpoint.id})

  const handleRunQuery = (query)=>{
    pushEvent("run-query", {query})
  }
  return (
    <section>
      <h3>{endpoint.name}</h3>
      <form onSubmit={e=> e.preventDefault()}>
        <label for="name">Name</label>
        <input
          name="name"
          type="text"
          value={queryParams.name}
          onChange={e => setQueryParams(prev => ({...prev, name: e.target.value}))}
        />
        <label for="query">SQL Query</label>
        <textarea name="query" value={queryParams.query} onChange={e => setQueryParams(prev => ({...prev, query: e.target.value}))} />
        <div>
          <button type="button" onClick={handleCancel}>
            Cancel
          </button>
          <button type="button" onClick={()=>{
            handleRunQuery(queryParams.query)
          }}>Run Query</button>
          <button onClick={handleSubmit}>Save</button>
        </div>
      </form>

      {/* params */}

      {/* result */}
      {queryResult && (
        <div>
          <h5>Result</h5>
          <pre>{JSON.stringify(queryResult, false, 2)}</pre>
        </div>
      )}
    </section>
  )
}
export default EndpointEditor
