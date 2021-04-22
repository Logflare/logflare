defmodule Logflare.Logs.SyslogMessage do
  @moduledoc """
  Parsed syslog message format.
  """
  use TypedStruct

  typedstruct do
    field :sd, list(map)
    field :logfmt, map()

    field :priority, integer()
    field :facility, integer()
    field :severity, integer()
    field :hostname, binary()
    field :appname, binary()
    field :process_id, binary()

    field :message, binary()
    field :message_id, binary()
    field :message_raw, binary()
    field :message_text, binary()

    field :timestamp, NaiveDateTime.t()
  end
end
