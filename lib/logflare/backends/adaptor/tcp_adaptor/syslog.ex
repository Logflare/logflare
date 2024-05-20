defmodule Logflare.Backends.Adaptor.TCPAdaptor.Syslog do
  @moduledoc """
  Implementation of [RFC5424 The Syslog Protocol][]

  It uses [octet-counting framing][].

  [RFC5424]: https://www.rfc-editor.org/rfc/rfc5424
  [octet-counting framing]: https://www.rfc-editor.org/rfc/rfc6587#section-3.4.1
  """

  alias Logflare.LogEvent

  # TODO: Change it to real value
  @pen 62137

  def format(%LogEvent{} = le, options) do
    msg = [
      header(le, options),
      " ",
      structured_data(le, options),
      " ",
      Jason.encode!(le.body),
      "\n"
    ]

    # TODO: Add support for non-transparent framing
    [to_string(IO.iodata_length(msg)), ?\s, msg]
  end

  defp header(%LogEvent{} = le, options) do
    level = to_level(le.body["level"] || le.body["metadata"]["level"])
    facility = options[:facility] || 16

    ingested_at = DateTime.from_naive!(le.ingested_at, "Etc/UTC")

    id = Ecto.UUID.dump!(le.id) |> Base.encode32(case: :lower, padding: false)

    [
      # Level and facility
      "<#{facility * 8 + level}>1 ",
      DateTime.to_iso8601(ingested_at),
      # XXX: Unknown hostname?
      " -",
      " ",
      le.source.name,
      # Unknown procname
      " -",
      " ",
      id
    ]
  end

  defp structured_data(%LogEvent{} = le, _options) do
    [
      "[source@#{@pen} name=#{inspect(le.source.name)} id=\"#{le.source.id}\"]"
    ]
  end

  @levels Map.new(
            Enum.with_index(~w[emergency alert critical error warning notice informational debug])
          )
  @shorhands %{
    "emer" => @levels["emergency"],
    "crit" => @levels["critical"],
    "err" => @levels["error"],
    "warn" => @levels["warning"],
    "info" => @levels["informational"]
  }

  @default @levels["notice"]

  defp to_level(level) when level in 0..7, do: level

  defp to_level(str) when is_binary(str) do
    str = String.downcase(str)
    # Unquote there to force compile time evaluation
    @levels[str] || @shorhands[str] || @default
  end

  defp to_level(_), do: @default
end
