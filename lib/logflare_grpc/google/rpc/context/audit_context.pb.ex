defmodule Google.Rpc.Context.AuditContext do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :audit_log, 1, type: :bytes, json_name: "auditLog"
  field :scrubbed_request, 2, type: Google.Protobuf.Struct, json_name: "scrubbedRequest"
  field :scrubbed_response, 3, type: Google.Protobuf.Struct, json_name: "scrubbedResponse"
  field :scrubbed_response_item_count, 4, type: :int32, json_name: "scrubbedResponseItemCount"
  field :target_resource, 5, type: :string, json_name: "targetResource"
end
