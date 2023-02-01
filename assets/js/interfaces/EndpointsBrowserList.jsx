import Button from "react-bootstrap/Button"
import ListGroup from "react-bootstrap/ListGroup"
import Card from "react-bootstrap/Card"
const EndpointsBrowserList = ({pushEvent, selectedEndpoint, endpoints}) => {
  const handleNewEndpoint = () => {
    pushEvent("new-endpoint", {})
  }
  const handleSelect = (id) => {
    pushEvent("show-endpoint", {endpoint_id: id})
  }
  return (
    <div className="tw-p-4">
      <Card>
        <Card.Header>
          <Button varian  t="secondary" onClick={handleNewEndpoint}>
            New Endpoint
          </Button>
        </Card.Header>

        <ListGroup variant="flush">
          {endpoints.map((endpoint) => (
            <ListGroup.Item key={endpoint.id}>
              <Button
                size="sm"
                variant="link"
                onClick={() => handleSelect(endpoint.id)}
              >
                {endpoint.name}
              </Button>
            </ListGroup.Item>
          ))}
        </ListGroup>
      </Card>
    </div>
  )
}

export default EndpointsBrowserList
