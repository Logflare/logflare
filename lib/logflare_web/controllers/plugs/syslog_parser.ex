defmodule Plug.Parsers.SYSLOG do
  @moduledoc """
  Parse syslog request bodies.
  """
  require Logger

  @behaviour Plug.Parsers

  alias Logflare.Logs.SyslogParser
  alias Logflare.Logs.SyslogMessage

  def init(opts) do
    {body_reader, opts} = Keyword.pop(opts, :body_reader, {Plug.Conn, :read_body, []})
    {body_reader, opts}
  end

  def parse(conn, "application", "logplex-1", _headers, {{mod, fun, args}, _opts}) do
    conn
    |> then(&apply(mod, fun, [&1 | args]))
    |> decode()
  end

  @doc false
  def parse(conn, _type, _subtype, _headers, _opts) do
    {:next, conn}
  end

  def decode({:ok, <<>>, conn}) do
    {:ok, %{}, conn}
  end

  def decode({:ok, body, conn}) do
    body = body |> String.split("\n", trim: true)

    opts =
      case conn.request_path do
        "/logs/logplex" -> [dialect: :heroku]
        "/logs/syslogs" -> []
        # Deprecate after September 1, 2020
        "/logs/syslog" -> [dialect: :heroku]
      end

    batch =
      for syslog_message_string <- body do
        case SyslogParser.parse(syslog_message_string, opts) do
          {:ok, syslog_message} ->
            to_log_params(syslog_message)

          {:error, error} ->
            Logger.error(
              "Syslog message parsing error: #{error}, message: |#{syslog_message_string}|, source: #{conn.params["source"]}"
            )

            nil
        end
      end
      |> Enum.reject(&is_nil(&1))

    {:ok, %{"batch" => batch}, conn}
  rescue
    e ->
      reraise Plug.Parsers.ParseError, [exception: e], __STACKTRACE__
  end

  def decode({:more, _, conn}) do
    {:error, :too_large, conn}
  end

  def decode({:error, :timeout}) do
    raise Plug.TimeoutError
  end

  def decode({:error, _}) do
    raise Plug.BadRequestError
  end

  def to_log_params(%SyslogMessage{} = syslog_msg) do
    {message, metadata} =
      syslog_msg
      |> Map.from_struct()
      |> Map.pop!(:message)

    {timestamp, metadata} = Map.pop!(metadata, :timestamp)
    metadata = Map.put(metadata, :level, metadata[:severity])

    %{
      "event_message" => message,
      "timestamp" => timestamp,
      "metadata" => MapKeys.to_strings(metadata)
    }
  end
end
