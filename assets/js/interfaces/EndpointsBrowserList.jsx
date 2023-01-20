const EndpointsBrowserList = ({pushEvent, selectedEndpoint, endpoints}) => {
  const handleNewEndpoint = () => {
    pushEvent("new-endpoint", {})
  }
  console.log(endpoints)
  const handleSelect = (id) => {
    pushEvent("show-endpoint", {endpoint_id: id})
  }
  return (
    <div>
      <button onClick={handleNewEndpoint}>New Endpoint</button>
      <ul>
        {endpoints.map((endpoint) => (
          <li
            style={{
              fontWeight:
                endpoint.id === selectedEndpoint?.id ? "bold" : undefined,
            }}
          >
            <button
              onClick={(e) => {
                handleSelect(endpoint.id)
              }}
            >
              {endpoint.name}
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}

export default EndpointsBrowserList
