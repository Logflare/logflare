import {useEffect, useState} from "react"
import Form from "react-bootstrap/Form"
import Button from "react-bootstrap/Button"
import QueryTester from "./endpoints/QueryTester.jsx"
import Alert from "react-bootstrap/Alert"

const EndpointEditor = ({
  pushEvent,
  endpoint,
  defaultValues = {},
  queryResultRows,
  declaredParams,
  parseErrorMessage,
}) => {
  const [queryParams, setQueryParams] = useState({
    name: endpoint?.name || defaultValues?.name || "",
    query: endpoint?.query || "",
    language: "bq_sql"
  })
  const [testParams, setTestParams] = useState({})
  const handleSubmit = (e) => {
    e.preventDefault()
    pushEvent("save-endpoint", {
      endpoint: queryParams,
    })
  }
  const handleCancel = () => {
    if (endpoint) {
      pushEvent("show-endpoint", {endpoint_id: endpoint.id})
    } else {
      pushEvent("list-endpoints", {})
    }
  }

  const handleRunQuery = () => {
    pushEvent("run-query", {
      query_string: queryParams.query,
      query_params: testParams,
    })
  }

  const handleChange = async (e) => {
    const name = e.target.name
    const value = e.target.value
    setQueryParams((prev) => ({...prev, [name]: value}))
  }

  const handleQuickRun = (e) => {
    if (e.ctrlKey && e.key == "Enter") {
      handleRunQuery(testParams)
    }
  }
  useEffect(() => {
    if (pushEvent) pushEvent("parse-query", {query_string: queryParams.query})
  }, [queryParams.query])

  return (
    <section className="tw-h-full" onKeyDown={handleQuickRun}>
      <h3>
        {endpoint ? (
          <>
            Editing <code>{endpoint?.name}</code>
          </>
        ) : (
          <>New Endpoint</>
        )}
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

          {parseErrorMessage && (
            <Alert variant="warning" className="mb-4">
              {parseErrorMessage}
            </Alert>
          )}
          <Button variant="light" onClick={handleCancel}>
            Cancel
          </Button>
          <Button variant="primary" type="submit">
            Save
          </Button>
        </Form>

        <QueryTester
          className=""
          onRunQuery={handleRunQuery}
          declaredParams={declaredParams}
          onParametersChange={setTestParams}
          queryResultRows={queryResultRows}
          parameters={testParams}
          showQuickRunPrompt
        />
      </section>
    </section>
  )
}
export default EndpointEditor
