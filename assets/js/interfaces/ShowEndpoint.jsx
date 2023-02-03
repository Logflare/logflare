import Button from "react-bootstrap/Button"
import Alert from "react-bootstrap/Alert"
import Card from "react-bootstrap/Card"
import Tab from "react-bootstrap/Tab"
import Tabs from "react-bootstrap/Tabs"
import QueryTester from "./endpoints/QueryTester.jsx"
import SettingsManager from "./endpoints/SettingsManager.jsx"
import ApiCallExamples from "./endpoints/ApiCallExamples.jsx"
import {useState} from "react"

const ShowEndpoint = ({
  pushEvent,
  endpoint,
  queryResultRows,
  declaredParams,
  baseUrl,
}) => {
  const [testParams, setTestParams] = useState({})
  const handleEditQuery = () => {
    pushEvent("edit-endpoint", {endpoint_id: endpoint.id})
  }
  const handleRunQuery = (params) => {
    pushEvent("run-query", {query_string: endpoint.query, query_params: params})
  }
  const handleSettingsUpdate = (key, value) => {
    pushEvent("save-endpoint", {endpoint: {[key]: value}})
  }
  const handleDeleteEndpoint = () => {
    const check = confirm("Are you sure that you want to delete this endpoint?")
    if (check) {
      pushEvent("delete-endpoint", {endpoint_id: endpoint.id})
    }
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

          <div className="tw-mt-4 tw-flex tw-flex-col tw-gap-4">
            <h4>Call Your Endpoint</h4>
            <QueryTester
              variant="horizontal"
              onRunQuery={handleRunQuery}
              declaredParams={declaredParams}
              queryResultRows={queryResultRows}
              onParametersChange={setTestParams}
              parameters={testParams}
            />

            <ApiCallExamples
              className="tw-mt-4"
              baseUrl={baseUrl}
              declaredParams={declaredParams}
              endpoint={endpoint}
              parameters={testParams}
            />
          </div>
        </Tab>
        {/* settings tab */}
        <Tab eventKey="settings" title="Settings">
          <SettingsManager
            endpoint={endpoint}
            onUpdate={handleSettingsUpdate}
          />
          <div>
            <Card bg="warning">
              <Card.Body>
                <Card.Title className="tw-text-black">
                  Delete this endpoint
                </Card.Title>
                <Card.Text className="tw-text-black">
                  Deleting the endpoint is <strong>irreversible</strong>. All
                  subsequent API calls will be rejected.
                </Card.Text>
                <Button variant="danger" onClick={handleDeleteEndpoint}>
                  Delete
                </Button>
              </Card.Body>
            </Card>
          </div>
        </Tab>
      </Tabs>
    </div>
  )
}

export default ShowEndpoint
