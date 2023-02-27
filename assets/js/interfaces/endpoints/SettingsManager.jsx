import {useState} from "react"
import Table from "react-bootstrap/Table"
import Button from "react-bootstrap/Button"
import Form from "react-bootstrap/Form"

const SettingsManager = ({endpoint, onUpdate}) => {
  const [editing, setEditing] = useState(null)
  const [editingValue, setEditingValue] = useState(null)
  return (
    <div>
      <Table striped="columns" variant="dark">
        <tbody>
          {[
            {key: "token", label: "UUID", readOnly: true},
            {
              key: "max_limit",
              label: "Max limit",
              formatter: (value) => `${value} rows`,
              type: "number",
            },
            {
              key: "enable_auth",
              label: "Authentication",
              formatter: (value) => (value ? "Enabled" : "Disabled"),
              type: "boolean",
              description: "Require an access token to query this endpoint",
              docsLink: "https://docs.logflare.app/endpoints#authentication",
            },
            {
              key: "cache_duration_seconds",
              label: "Cache duration",
              formatter: (value) => `${value} seconds`,
              type: "number",
              description: "Cache TTL. Zero disables caching.",
              docsLink: "https://docs.logflare.app/endpoints#cache",
            },
            {
              key: "proactive_requerying_seconds",
              label: "Cache Proactive Requerying",
              formatter: (value) => `${value} seconds`,
              type: "number",
              description: "Updates the cached results at a given interval",

              docsLink:
                "https://docs.logflare.app/endpoints#proactive-requerying",
            },
            {
              key: "sandboxable",
              label: "Sandbox Mode",
              formatter: (value) => (value ? "Enabled" : "Disabled"),
              type: "boolean",
              description: "Sandbox the query using CTEs",
              docsLink: "https://docs.logflare.app/endpoints#query-sandboxing",
            },
          ].map(
            ({
              key,
              readOnly,
              label,
              type = "text",
              formatter = (value) => value,
              description,
              docsLink,
            }) => (
              <tr key={key}>
                <td className="tw-flex tw-flex-col tw-gap-1">
                  <span>
                    {label}{" "}
                    {docsLink ? (
                      <a href={docsLink} target="_blank">
                        <i
                          className="fa fa-question-circle"
                          aria-hidden="true"
                        ></i>
                      </a>
                    ) : null}
                  </span>
                  <span className="tw-text-xs tw-text-gray-300">
                    {description}
                  </span>
                </td>
                {editing === key ? (
                  <td colSpan={2}>
                    <Form
                      onSubmit={(e) => {
                        e.preventDefault()
                        onUpdate(key, editingValue)
                        setEditing(null)
                      }}
                    >
                      <Form.Group controlId={key}>
                        {type === "number" && (
                          <Form.Control
                            name={key}
                            size="sm"
                            type={"number"}
                            value={editingValue}
                            onChange={(e) => setEditingValue(e.target.value)}
                          />
                        )}
                        {type === "boolean" && (
                          <Form.Check
                            id={key}
                            name={key}
                            size="sm"
                            type="switch"
                            label={`Enable`}
                            checked={editingValue}
                            onChange={(e) => setEditingValue(e.target.checked)}
                          />
                        )}
                      </Form.Group>
                      <Button type="submit">Save</Button>
                      <Button
                        variant="secondary"
                        onClick={() => setEditing(null)}
                      >
                        Cancel
                      </Button>
                    </Form>
                  </td>
                ) : (
                  <>
                    <td>{formatter(endpoint[key])} </td>
                    <td>
                      {!readOnly && (
                        <Button
                          variant="outline-secondary"
                          size="sm"
                          onClick={() => {
                            setEditingValue(endpoint[key])
                            setEditing(key)
                          }}
                        >
                          Edit
                        </Button>
                      )}
                    </td>
                  </>
                )}
              </tr>
            )
          )}
        </tbody>
      </Table>
    </div>
  )
}

export default SettingsManager
