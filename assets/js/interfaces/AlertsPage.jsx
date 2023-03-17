import { useState } from "react";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import Accordion from "react-bootstrap/Accordion";
import Card from "react-bootstrap/Card";

const DEFAULT_ATTRS = {
  query:
    "select event_message, count(id)\nfrom `my.source` \nwhere regexp_contains(event_message, '[Ee]rror')",
  cron: "*/10 * * * *",
};

const AlertsPage = ({ alerts, pushEvent }) => {
  const [showingId, setShowingId] = useState(null);
  const [creating, setCreating] = useState(false);
  const [editing, setEditing] = useState(false);
  const [editingAttrs, setEditingAttrs] = useState({});
  const handleClose = () => {
    setShowingId(null);
    setCreating(false);
    setEditingAttrs({});
  };
  const handleCreate = async () => {
    console.log("editingAttrs", editingAttrs);
    await pushEvent("create-alert", { alert_query: editingAttrs });
    setCreating(false);
    setEditingAttrs({});
  };

  const handleDelete = async (id) => {
    await pushEvent("delete-alert", { id });
    setCreating(false);
  };

  const handleUpdate = async () => {
    if (!editingAttrs) return;
    console.log('attrs', editingAttrs)
    await pushEvent("update-alert", {
      id: editingAttrs.id,
      alert_query: editingAttrs,
    });
    setEditing(false);
    setEditingAttrs({});
  };

  const handleFormChange = async (name, value) => {
    setEditingAttrs((prev) => ({
      ...prev,
      [name]: value,
    }));
  };

  return (
    <div className="tw-w-full">
      <div>
        <Button
          variant="primary"
          onClick={() => {
            setCreating(true);
            setEditingAttrs(DEFAULT_ATTRS);
          }}
        >
          Create alert
        </Button>
      </div>
      <Modal show={creating} onHide={handleClose}>
        <Modal.Header closeButton>
          <Modal.Title>{alert.name}</Modal.Title>
        </Modal.Header>

        <Modal.Body>
          <AlertForm values={editingAttrs} onChange={handleFormChange} />
        </Modal.Body>

        <Modal.Footer>
          <Button variant="secondary" onClick={handleClose}>
            Cancel
          </Button>
          <Button variant="primary" onClick={handleCreate}>
            Save
          </Button>
        </Modal.Footer>
      </Modal>

      <Modal show={editing} onHide={handleClose}>
        <Modal.Header closeButton>
          <Modal.Title>Editing {alert.name}</Modal.Title>
        </Modal.Header>

        <Modal.Body>
          <AlertForm values={editingAttrs} onChange={handleFormChange} />
        </Modal.Body>

        <Modal.Footer>
          <Button variant="secondary" onClick={handleClose}>
            Cancel
          </Button>
          <Button variant="primary" onClick={handleUpdate}>
            Save
          </Button>
        </Modal.Footer>
      </Modal>

      <Accordion className="tw-w-full">
        {alerts.map((alert) => (
          <Card className="tw-w-full">
            <Card.Header className="tw-w-76">
              <Accordion.Toggle as={Button} variant="link" eventKey={alert.id}>
                <span>{alert.name}</span>
              </Accordion.Toggle>
              <code>{alert.cron}</code>
            </Card.Header>
            <Accordion.Collapse eventKey={alert.id}>
              <Card.Body className="">
                <code className="tw-whitespace-pre-wrap">{alert.query}</code>
              <Card.Footer>
                <Button variant="danger" onClick={() => handleDelete(alert.id)}>
                  Delete
                </Button>
                <Button
                  variant="secondary"
                  onClick={() => {
                    setEditing(true);
                    setEditingAttrs(alert)
                  }}
                >
                  Edit
                </Button>
              </Card.Footer>
              </Card.Body>
            </Accordion.Collapse>
          </Card>
        ))}
      </Accordion>
    </div>
  );
};

const AlertForm = ({ values, onChange }) => {
  const handleInputChange = async (e) => {
    if (!onChange) return;
    onChange(e.target.name, e.target.value || e.target.checked);
  };
  return (
    <Form>
      <Form.Group className="mb-3" controlId="name">
        <Form.Label>Name</Form.Label>
        <Form.Control
          type="text"
          placeholder="api.my-api"
          value={values.name || ""}
          name="name"
          onChange={handleInputChange}
        />
      </Form.Group>

      <Form.Group className="mb-3" controlId="active">
        <Form.Check
          type="checkbox"
          checked={values.active || true}
          label="Active"
          onChange={handleInputChange}
        />
      </Form.Group>

      <Form.Group className="mb-3" controlId="query">
        <Form.Label>SQL Query</Form.Label>
        <Form.Control
          as="textarea"
          rows={3}
          value={values.query}
          name="query"
          onChange={handleInputChange}
        />
      </Form.Group>
      <Form.Group className="mb-3" controlId="cron">
        <Form.Label>Cron</Form.Label>
        <Form.Control
          type="text"
          value={values.cron}
          name="cron"
          onChange={handleInputChange}
        />
      </Form.Group>
      <Form.Group className="mb-3" controlId="slack_hook_url">
        <Form.Label>Slack URL</Form.Label>
        <Form.Control
          type="text"
          placeholder="https://..."
          value={values.slack_hook_url || ""}
          name="slack_hook_url"
          onChange={handleInputChange}
        />
        <Form.Text className="text-muted">
          Optional, for sending to a specific slack channel
        </Form.Text>
      </Form.Group>

      <Form.Group className="mb-3" controlId="webhook_notification_url">
        <Form.Label>Webhook URL</Form.Label>
        <Form.Control
          type="text"
          placeholder="https://..."
          value={values.webhook_notification_url || ""}
          name="webhook_notification_url"
          onChange={handleInputChange}
        />
        <Form.Text className="text-muted">
          Optional, will receive the results of the alert query
        </Form.Text>
      </Form.Group>
    </Form>
  );
};

export default AlertsPage;
