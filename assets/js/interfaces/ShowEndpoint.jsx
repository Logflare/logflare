const ShowEndpoint = ({pushEvent, endpoint}) => {
    const handleEditQuery = () => {
      pushEvent("edit-endpoint", {endpoint_id: endpoint.id})
    }
    const handleRunQuery = ()=>{
      pushEvent("run-query", {query: endpoint.query})
    }
    return (
      <div>
          <h3>{endpoint.name}</h3>
          <pre>
              {endpoint.query}
          </pre>
          <button onClick={handleRunQuery}>Run Query</button>
          <button onClick={handleEditQuery}>Edit Query</button>
      </div>
    )
  }
  export default ShowEndpoint