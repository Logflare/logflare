defmodule Logflare.QA.Ingest.Channels do
  @moduledoc "Sends a batch to the QA server over each ingest channel, as an external client would."

  alias Logflare.QA.Config
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias Opentelemetry.Proto.Collector.Logs.V1.LogsService.Stub
  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.InstrumentationScope
  alias Opentelemetry.Proto.Logs.V1.LogRecord
  alias Opentelemetry.Proto.Logs.V1.ResourceLogs
  alias Opentelemetry.Proto.Logs.V1.ScopeLogs
  alias Opentelemetry.Proto.Resource.V1.Resource

  @timeout 10_000

  @spec http(String.t(), [String.t()]) :: :ok | {:error, term()}
  def http(query, messages) do
    {:ok, _} = Application.ensure_all_started(:inets)
    body = Jason.encode!(%{batch: Enum.map(messages, &%{message: &1})})
    headers = [{~c"x-api-key", String.to_charlist(Config.public_token())}]
    request = {~c"#{Config.url()}/api/logs?#{query}", headers, ~c"application/json", body}

    case :httpc.request(:post, request, [timeout: @timeout], []) do
      {:ok, {{_, 200, _}, _, _}} -> :ok
      {:ok, {{_, status, _}, _, body}} -> {:error, {status, to_string(body)}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Joins the source's `LogChannel` over the `/logs` socket and pushes one batch."
  @spec websocket(String.t(), [String.t()]) :: :ok | {:error, term()}
  def websocket(source_token, messages) do
    {:ok, _} = Application.ensure_all_started(:gun)
    %URI{host: host, port: port} = URI.parse(Config.url())
    {:ok, conn} = :gun.open(String.to_charlist(host), port, %{protocols: [:http]})
    {:ok, :http} = :gun.await_up(conn, @timeout)

    ref =
      :gun.ws_upgrade(conn, ~c"/logs/websocket?vsn=2.0.0&access_token=#{Config.public_token()}")

    topic = "logs:#{source_token}"

    result =
      with :ok <- await_upgrade(conn, ref),
           :ok <- push(conn, ref, ["1", "1", topic, "phx_join", %{}]),
           :ok <- await_join(conn, ref),
           :ok <-
             push(conn, ref, [
               "1",
               "2",
               topic,
               "batch",
               %{batch: Enum.map(messages, &%{message: &1})}
             ]) do
        Process.sleep(1_000)
      end

    :gun.close(conn)
    result
  end

  @doc "Exports the messages as OTLP log records over gRPC."
  @spec grpc(String.t(), [String.t()]) :: :ok | {:error, term()}
  def grpc(source_token, messages) do
    {:ok, _} = Application.ensure_all_started(:grpc)
    {:ok, sup} = GRPC.Client.Supervisor.start_link([])
    headers = [{"x-api-key", Config.public_token()}, {"x-source", source_token}]
    {:ok, channel} = GRPC.Stub.connect("localhost:#{Config.grpc_port()}", headers: headers)
    now = System.os_time(:nanosecond)

    records =
      for message <- messages,
          do: %LogRecord{time_unix_nano: now, body: %AnyValue{value: {:string_value, message}}}

    request = %ExportLogsServiceRequest{
      resource_logs: [
        %ResourceLogs{
          resource: %Resource{},
          scope_logs: [
            %ScopeLogs{scope: %InstrumentationScope{name: "ingest-qa"}, log_records: records}
          ]
        }
      ]
    }

    result =
      case Stub.export(channel, request) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end

    Supervisor.stop(sup)
    result
  end

  defp push(conn, ref, message), do: :gun.ws_send(conn, ref, {:text, Jason.encode!(message)})

  defp await_upgrade(conn, ref) do
    receive do
      {:gun_upgrade, ^conn, ^ref, ["websocket"], _} -> :ok
      {:gun_response, ^conn, ^ref, _, status, _} -> {:error, {:upgrade_status, status}}
      {:gun_error, ^conn, ^ref, reason} -> {:error, reason}
    after
      @timeout -> {:error, :upgrade_timeout}
    end
  end

  defp await_join(conn, ref) do
    receive do
      {:gun_ws, ^conn, ^ref, {:text, data}} ->
        case Jason.decode!(data) do
          [_, "1", _, "phx_reply", %{"status" => "ok"}] -> :ok
          [_, "1", _, "phx_reply", payload] -> {:error, {:join, payload}}
          _ -> await_join(conn, ref)
        end
    after
      @timeout -> {:error, :join_timeout}
    end
  end
end
