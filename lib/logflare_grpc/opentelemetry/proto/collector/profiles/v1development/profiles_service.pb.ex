defmodule Opentelemetry.Proto.Collector.Profiles.V1development.ExportProfilesServiceRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExportProfilesServiceRequest",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "resource_profiles",
          extendee: nil,
          number: 1,
          label: :LABEL_REPEATED,
          type: :TYPE_MESSAGE,
          type_name: ".opentelemetry.proto.profiles.v1development.ResourceProfiles",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "resourceProfiles",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :resource_profiles, 1,
    repeated: true,
    type: Opentelemetry.Proto.Profiles.V1development.ResourceProfiles,
    json_name: "resourceProfiles"
end

defmodule Opentelemetry.Proto.Collector.Profiles.V1development.ExportProfilesServiceResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExportProfilesServiceResponse",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "partial_success",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_MESSAGE,
          type_name:
            ".opentelemetry.proto.collector.profiles.v1development.ExportProfilesPartialSuccess",
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "partialSuccess",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :partial_success, 1,
    type: Opentelemetry.Proto.Collector.Profiles.V1development.ExportProfilesPartialSuccess,
    json_name: "partialSuccess"
end

defmodule Opentelemetry.Proto.Collector.Profiles.V1development.ExportProfilesPartialSuccess do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.DescriptorProto{
      name: "ExportProfilesPartialSuccess",
      field: [
        %Google.Protobuf.FieldDescriptorProto{
          name: "rejected_profiles",
          extendee: nil,
          number: 1,
          label: :LABEL_OPTIONAL,
          type: :TYPE_INT64,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "rejectedProfiles",
          proto3_optional: nil,
          __unknown_fields__: []
        },
        %Google.Protobuf.FieldDescriptorProto{
          name: "error_message",
          extendee: nil,
          number: 2,
          label: :LABEL_OPTIONAL,
          type: :TYPE_STRING,
          type_name: nil,
          default_value: nil,
          options: nil,
          oneof_index: nil,
          json_name: "errorMessage",
          proto3_optional: nil,
          __unknown_fields__: []
        }
      ],
      nested_type: [],
      enum_type: [],
      extension_range: [],
      extension: [],
      options: nil,
      oneof_decl: [],
      reserved_range: [],
      reserved_name: [],
      __unknown_fields__: []
    }
  end

  field :rejected_profiles, 1, type: :int64, json_name: "rejectedProfiles"
  field :error_message, 2, type: :string, json_name: "errorMessage"
end

defmodule Opentelemetry.Proto.Collector.Profiles.V1development.ProfilesService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "opentelemetry.proto.collector.profiles.v1development.ProfilesService",
    protoc_gen_elixir_version: "0.13.0"

  def descriptor do
    # credo:disable-for-next-line
    %Google.Protobuf.ServiceDescriptorProto{
      name: "ProfilesService",
      method: [
        %Google.Protobuf.MethodDescriptorProto{
          name: "Export",
          input_type:
            ".opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest",
          output_type:
            ".opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceResponse",
          options: %Google.Protobuf.MethodOptions{
            deprecated: false,
            idempotency_level: :IDEMPOTENCY_UNKNOWN,
            uninterpreted_option: [],
            __pb_extensions__: %{},
            __unknown_fields__: []
          },
          client_streaming: false,
          server_streaming: false,
          __unknown_fields__: []
        }
      ],
      options: nil,
      __unknown_fields__: []
    }
  end

  rpc :Export,
      Opentelemetry.Proto.Collector.Profiles.V1development.ExportProfilesServiceRequest,
      Opentelemetry.Proto.Collector.Profiles.V1development.ExportProfilesServiceResponse
end

defmodule Opentelemetry.Proto.Collector.Profiles.V1development.ProfilesService.Stub do
  @moduledoc false

  use GRPC.Stub,
    service: Opentelemetry.Proto.Collector.Profiles.V1development.ProfilesService.Service
end