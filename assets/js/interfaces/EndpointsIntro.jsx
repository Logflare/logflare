import Jumbotron from "react-bootstrap/Jumbotron"
import Button from "react-bootstrap/Button"
const EndpointsIntro = ({pushEvent}) => (
  <Jumbotron>
    <h1>LogflareEndpoints</h1>
    <p className="tw-my-10">
      Logflare Endpoints are GET JSON API endpoints that run SQL queries on your
      event data.
    </p>

    <a href="https://docs.logflare.app/endpoints" target="_blank">
      <Button
        variant="link"
        onClick={() => {
          pushEvent("new-endpoint", {})
        }}
      >
        Explore the Docs
      </Button>
    </a>

    <Button
      variant="primary"
      onClick={() => {
        pushEvent("new-endpoint", {})
      }}
    >
      Create an Endpoint
    </Button>
  </Jumbotron>
)

export default EndpointsIntro
