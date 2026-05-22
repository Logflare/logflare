defmodule Prometheus.Label do
  @moduledoc false

  use Protobuf, syntax: :proto3

  field :name, 1, type: :string
  field :value, 2, type: :string
end

defmodule Prometheus.Sample do
  @moduledoc false

  use Protobuf, syntax: :proto3

  field :value, 1, type: :double
  field :timestamp, 2, type: :int64
end

defmodule Prometheus.TimeSeries do
  @moduledoc false

  use Protobuf, syntax: :proto3

  field :labels, 1, repeated: true, type: Prometheus.Label
  field :samples, 2, repeated: true, type: Prometheus.Sample
end

defmodule Prometheus.WriteRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3

  field :timeseries, 1, repeated: true, type: Prometheus.TimeSeries, json_name: "timeseries"
end
