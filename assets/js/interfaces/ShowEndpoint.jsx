import Button from "react-bootstrap/Button"
import Alert from "react-bootstrap/Alert"
import Card from "react-bootstrap/Card"
import Tab from "react-bootstrap/Tab"
import Tabs from "react-bootstrap/Tabs"
import JsonResults from "./endpoints/JsonResults.jsx"

const ShowEndpoint = ({pushEvent, endpoint, queryResult}) => {
  const handleEditQuery = () => {
    pushEvent("edit-endpoint", {endpoint_id: endpoint.id})
  }
  const handleRunQuery = () => {
    pushEvent("run-query", {query: endpoint.query})
  }
  return (
    <div className="tw-px-4 tw-flex tw-flex-col tw-gap-4">
      {!endpoint.enable_auth && (
        <Alert variant="warning">
          <strong>Authentication Not Enabled!</strong> Authentication has not
          been enabled for this endpoint, and may pose a security risk.
        </Alert>
      )}
      <h3>{endpoint.name}</h3>

      <Tabs defaultActiveKey="query" id="endpoint-query-tabs">
        {/* query tab */}
        <Tab eventKey="query" title="Query">
          <Card>
            <Card.Body>
              <Card.Text as="code">{endpoint.query}</Card.Text>
            </Card.Body>
            <Card.Footer className="tw-flex tw-flex-row tw-justify-end">
              <Button onClick={handleEditQuery}>Edit</Button>
            </Card.Footer>
          </Card>

          <div className="mt-4">
            <h4>Test Your Endpoint</h4>
            <Button onClick={handleRunQuery}>Run Query</Button>
            {queryResult && <JsonResults className="mt-4" data={queryResult} />}
          </div>

          
        </Tab>
        {/* settings tab */}
        <Tab eventKey="settings" title="Settings">
          UUID: {endpoint.token}
          Max limit: {endpoint.max_limit} rows Authentication:{" "}
          {endpoint.enable_auth}
          cache duration: {endpoint.cache_duration_seconds}s Proactive
          Requerying Seconds: {endpoint.proactive_requerying_seconds}s Sandbox
          Mode: {endpoint.sandboxable}
        </Tab>
      </Tabs>
    </div>
  )
}

export default ShowEndpoint
