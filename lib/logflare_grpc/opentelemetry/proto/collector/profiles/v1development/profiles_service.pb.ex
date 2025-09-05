defmodule Opentelemetry.Proto.Collector.Profiles.V1development.ExportProfilesServiceRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :resource_profiles, 1,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.ResourceProfiles,
    json_name: "resourceProfiles"

  field :dictionary, 2, type: Opentelemetry.Proto.Profiles.V1development.ProfilesDictionary
end

defmodule Opentelemetry.Proto.Collector.Profiles.V1development.ExportProfilesServiceResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :partial_success, 1,
    type: Opentelemetry.Proto.Collector.Profiles.V1development.ExportProfilesPartialSuccess,
    json_name: "partialSuccess"
end

defmodule Opentelemetry.Proto.Collector.Profiles.V1development.ExportProfilesPartialSuccess do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field :rejected_profiles, 1, type: :int64, json_name: "rejectedProfiles"
  field :error_message, 2, type: :string, json_name: "errorMessage"
end

defmodule Opentelemetry.Proto.Collector.Profiles.V1development.ProfilesService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "opentelemetry.proto.collector.profiles.v1development.ProfilesService",
    protoc_gen_elixir_version: "0.14.1"

  rpc(
    :Export,
    Opentelemetry.Proto.Collector.Profiles.V1development.ExportProfilesServiceRequest,
    Opentelemetry.Proto.Collector.Profiles.V1development.ExportProfilesServiceResponse
  )
end

defmodule Opentelemetry.Proto.Collector.Profiles.V1development.ProfilesService.Stub do
  @moduledoc false

  use GRPC.Stub,
    service: Opentelemetry.Proto.Collector.Profiles.V1development.ProfilesService.Service
end
