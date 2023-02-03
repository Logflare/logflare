import Jumbotron from "react-bootstrap/Jumbotron"
import Button from "react-bootstrap/Button"
const EndpointsIntro = ({pushEvent}) => (
  <Jumbotron>
    <h1 className="tw-text-white">Logflare Endpoints</h1>
    <p className="tw-my-10">
      Logflare Endpoints are GET JSON API endpoints that run ANSI SQL queries on your
      event data.

      With Endpoints you can integrated queries over your ingested events and integrate your data into an end-user facing application.
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
