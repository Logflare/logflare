# Routing targets for the QA source. Each run sends one "error", "warn" and "info"
# event per channel; `expect` lists the kinds each target must receive and no others.
# Add a backend here to route to more backends; setup.exs and verify.exs both read this file.
%{
  main: "qa_ingest_main",
  channels: ["http_token", "http_name", "websocket", "grpc"],
  kinds: ["error", "warn", "info"],
  targets: [
    %{type: :source, name: "qa_ingest_sink", lql: "error", expect: ["error"]},
    %{type: :backend, name: "qa_ingest_drain_a", lql: "error", expect: ["error"]},
    %{type: :backend, name: "qa_ingest_drain_b", lql: "warn", expect: ["warn"]},
    %{
      type: :backend,
      name: "qa_ingest_drain_c",
      lql: ~s|~"error\|warn"|,
      expect: ["error", "warn"]
    }
  ]
}
