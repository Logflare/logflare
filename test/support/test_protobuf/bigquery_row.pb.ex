defmodule Logflare.TestProtobuf.BigQueryRow.KeyValue do
  @moduledoc false

  use Protobuf, syntax: :proto2

  field :key, 1, required: true, type: :string
  field :value, 2, optional: true, type: :string
end

defmodule Logflare.TestProtobuf.BigQueryRow do
  @moduledoc false

  use Protobuf, syntax: :proto2

  field :id, 1, required: true, type: :string
  field :severity, 2, optional: true, type: :int64
  field :score, 3, optional: true, type: :double
  field :ok, 4, optional: true, type: :bool
  field :ts, 5, optional: true, type: :int64
  field :attributes, 6, repeated: true, type: Logflare.TestProtobuf.BigQueryRow.KeyValue
  field :counts, 7, repeated: true, type: :int64
  field :bounds, 8, repeated: true, type: :double
  field :names, 9, repeated: true, type: :string
  field :events, 10, repeated: true, type: :string
end
