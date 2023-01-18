const EndpointsEditor = ({pushEvent, pushEventTo, handleEvent, endpoint}) => {
  const handleSubmit = (e) => {
    pushEvent("save-endpoint", {
      endpoint: {
        query: e.target.query.value,
      },
    })
  }
  const handleCancel = () => pushEvent("cancel-endpoint")

  return (
    <section>
      <h3>{endpoint.name}</h3>
      <form onSubmit={handleSubmit}>
        <textarea name="query" defaultValue={endpoint.query} />
        <div>
          <button type="button" onClick={handleCancel}>
            Cancel
          </button>
          <button type="button">Run Query</button>
          <button type="submit">Save</button>
        </div>
      </form>

      {/* params */}
      
      {/* results */}
    </section>
  )
}
export default EndpointsEditor
