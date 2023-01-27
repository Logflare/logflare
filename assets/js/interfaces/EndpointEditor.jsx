import {useState} from "react"
import Form from "react-bootstrap/Form"
import Button from "react-bootstrap/Button"
import JsonResults from "./endpoints/JsonResults.jsx"

const EndpointEditor = ({
  pushEvent,
  endpoint = {},
  defaultValues = {},
  queryResult = null,
}) => {
  const [queryParams, setQueryParams] = useState({
    name: endpoint.name || defaultValues.name || "",
    query: endpoint.query || "",
  })
  const handleSubmit = (e) => {
    e.preventDefault()
    pushEvent("save-endpoint", {
      endpoint: queryParams,
    })
  }
  const handleCancel = () =>
    pushEvent("show-endpoint", {endpoint_id: endpoint.id})

  const handleRunQuery = () => {
    pushEvent("run-query", {query: queryParams.query})
  }

  const handleChange = async (e) => {
    const name = e.target.name
    const value = e.target.value
    setQueryParams((prev) => ({...prev, [name]: value}))
  }
  
  return (
    <section>
      <h3>
        Editing <code>{endpoint.name}</code>
      </h3>

      <section className="tw-flex tw-flex-row tw-gap-4 tw-mt-4">
        <Form onSubmit={handleSubmit} className="tw-w-3/5">
          <Form.Group controlId="name">
            <Form.Label>Endpoint Name</Form.Label>
            <Form.Control
              name="name"
              type="text"
              value={queryParams.name}
              onChange={handleChange}
            />
          </Form.Group>
          <Form.Group controlId="query">
            <Form.Label>SQL Query</Form.Label>
            <Form.Control
              name="query"
              as="textarea"
              rows={4}
              value={queryParams.query}
              onChange={handleChange}
            />
          </Form.Group>
          <Button variant="light" onClick={handleCancel}>
            Cancel
          </Button>
          <Button variant="secondary" onClick={handleRunQuery}>
            Run Query
          </Button>
          <Button variant="primary" type="submit">
            Submit
          </Button>
        </Form>
        <div className="tw-w-2/5">
          <h5>Query Results</h5>

          {queryResult && <JsonResults className="mt-4" data={queryResult} />}
          {!queryResult && (
            <div>
              <p>No results yet.</p>
              <p>Run the query to get test results.</p>
            </div>
          )}
        </div>
      </section>
    </section>
  )
}
export default EndpointEditor
