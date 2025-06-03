defmodule Google.Rpc.ErrorInfo.MetadataEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Rpc.ErrorInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :reason, 1, type: :string
  field :domain, 2, type: :string
  field :metadata, 3, repeated: true, type: Google.Rpc.ErrorInfo.MetadataEntry, map: true
end

defmodule Google.Rpc.RetryInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :retry_delay, 1, type: Google.Protobuf.Duration, json_name: "retryDelay"
end

defmodule Google.Rpc.DebugInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :stack_entries, 1, repeated: true, type: :string, json_name: "stackEntries"
  field :detail, 2, type: :string
end

defmodule Google.Rpc.QuotaFailure.Violation.QuotaDimensionsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Rpc.QuotaFailure.Violation do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :subject, 1, type: :string
  field :description, 2, type: :string
  field :api_service, 3, type: :string, json_name: "apiService"
  field :quota_metric, 4, type: :string, json_name: "quotaMetric"
  field :quota_id, 5, type: :string, json_name: "quotaId"

  field :quota_dimensions, 6,
    repeated: true,
    type: Google.Rpc.QuotaFailure.Violation.QuotaDimensionsEntry,
    json_name: "quotaDimensions",
    map: true

  field :quota_value, 7, type: :int64, json_name: "quotaValue"
  field :future_quota_value, 8, proto3_optional: true, type: :int64, json_name: "futureQuotaValue"
end

defmodule Google.Rpc.QuotaFailure do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :violations, 1, repeated: true, type: Google.Rpc.QuotaFailure.Violation
end

defmodule Google.Rpc.PreconditionFailure.Violation do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :type, 1, type: :string
  field :subject, 2, type: :string
  field :description, 3, type: :string
end

defmodule Google.Rpc.PreconditionFailure do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :violations, 1, repeated: true, type: Google.Rpc.PreconditionFailure.Violation
end

defmodule Google.Rpc.BadRequest.FieldViolation do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :field, 1, type: :string
  field :description, 2, type: :string
  field :reason, 3, type: :string
  field :localized_message, 4, type: Google.Rpc.LocalizedMessage, json_name: "localizedMessage"
end

defmodule Google.Rpc.BadRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :field_violations, 1,
    repeated: true,
    type: Google.Rpc.BadRequest.FieldViolation,
    json_name: "fieldViolations"
end

defmodule Google.Rpc.RequestInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :request_id, 1, type: :string, json_name: "requestId"
  field :serving_data, 2, type: :string, json_name: "servingData"
end

defmodule Google.Rpc.ResourceInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource_type, 1, type: :string, json_name: "resourceType"
  field :resource_name, 2, type: :string, json_name: "resourceName"
  field :owner, 3, type: :string
  field :description, 4, type: :string
end

defmodule Google.Rpc.Help.Link do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :description, 1, type: :string
  field :url, 2, type: :string
end

defmodule Google.Rpc.Help do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :links, 1, repeated: true, type: Google.Rpc.Help.Link
end

defmodule Google.Rpc.LocalizedMessage do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :locale, 1, type: :string
  field :message, 2, type: :string
end
