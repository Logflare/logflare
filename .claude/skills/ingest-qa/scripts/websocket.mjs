// Usage: node websocket.mjs <ws_url> <source_token> <batch_json>
// Joins the source's LogChannel over the /logs socket and pushes one batch.
const [url, token, batch] = process.argv.slice(2);
const topic = `logs:${token}`;
const ws = new WebSocket(url);

const fail = (reason) => {
  console.log(`websocket FAIL ${reason}`);
  process.exit(1);
};

setTimeout(() => fail("timeout"), 10_000);
ws.onerror = (event) => fail(event.message);
ws.onopen = () => ws.send(JSON.stringify(["1", "1", topic, "phx_join", {}]));
ws.onmessage = (message) => {
  const [, ref, , event, payload] = JSON.parse(message.data);
  if (event !== "phx_reply" || ref !== "1") return;
  if (payload.status !== "ok") fail(`join ${JSON.stringify(payload)}`);

  ws.send(JSON.stringify(["1", "2", topic, "batch", { batch: JSON.parse(batch) }]));
  setTimeout(() => {
    console.log("websocket ok");
    process.exit(0);
  }, 1_500);
};
