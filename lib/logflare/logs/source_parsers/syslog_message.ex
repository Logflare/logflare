defmodule Logflare.Syslog.Message do
  @moduledoc """
  Parsed syslog message format.
  """
  @type t :: %__MODULE__{
          ip: binary,
          port: integer,
          priority: integer,
          facility: integer,
          severity: integer,
          timestamp: NaiveDateTime.t(),
          host_name: binary,
          process: binary,
          process_id: binary,
          message: binary,
          message_raw: binary,
          key_values: map,
          message_json: map,
          data_ids: list(map)
        }

  defstruct [
    :ip,
    :port,
    :priority,
    :facility,
    :severity,
    :timestamp,
    :host_name,
    :process,
    :process_id,
    :message,
    :message_raw,
    :key_values,
    :message_json,
    :data_ids,
    :data
  ]
end
