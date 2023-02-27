import Tab from "react-bootstrap/Tab"
import Tabs from "react-bootstrap/Tabs"

const ApiCallExamples = ({
  baseUrl,
  declaredParams,
  endpoint,
  parameters,
  className,
}) => (
  <Tabs className={className} variant="pills" defaultActiveKey="curl">
    <Tab eventKey="curl" title="cURL">
      <pre className="tw-p-4 tw-rounded-lg">
        {`curl "${baseUrl}/endpoints/query/${endpoint.token}"
\ -H 'X-API-KEY: YOUR-ACCESS-TOKEN' 
\ -H 'Content-Type: application/json; charset=utf-8'
${
  declaredParams.length > 0
    ? `\ -G${declaredParams.map(
        (key) => ` -d "${key}=${parameters[key] || "VALUE"}"`
      )}`
    : ""
}
`}
      </pre>
    </Tab>
  </Tabs>
)

export default ApiCallExamples
