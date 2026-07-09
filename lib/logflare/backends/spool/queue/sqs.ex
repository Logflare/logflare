defmodule Logflare.Backends.Spool.Queue.SQS do
  @moduledoc false

  @behaviour Logflare.Backends.Spool.Queue

  @impl Logflare.Backends.Spool.Queue
  def resolve(queue_name) do
    case request(ExAws.SQS.get_queue_url(queue_name)) do
      {:ok, %{body: %{queue_url: url}}} -> {:ok, url}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl Logflare.Backends.Spool.Queue
  def receive(queue_url, opts) do
    max = Keyword.get(opts, :max_number_of_messages, 1)

    case request(ExAws.SQS.receive_message(queue_url, max_number_of_messages: max)) do
      {:ok, %{body: %{messages: messages}}} ->
        normalized =
          Enum.map(messages, fn %{body: body, receipt_handle: handle} ->
            %{id: handle, body: body}
          end)

        {:ok, normalized}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl Logflare.Backends.Spool.Queue
  def ack(queue_url, handle) do
    case request(ExAws.SQS.delete_message(queue_url, handle)) do
      {:ok, _} ->
        :ok

      {:error, {:fatal, {:expected_element_start_tag, _, _, _}}} ->
        # ElasticMQ returns 200 with an empty body for DeleteMessage. Our XML
        # parser can't parse an empty document and exits with exactly this
        # reason even though the delete itself landed (200 status) — only
        # this specific shape is treated as success; any other parse
        # failure or {:error, reason} from the request itself is real.
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl Logflare.Backends.Spool.Queue
  def nack(queue_url, handle) do
    case request(ExAws.SQS.change_message_visibility(queue_url, handle, 0)) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl Logflare.Backends.Spool.Queue
  def publish(queue_url, body) do
    case request(ExAws.SQS.send_message(queue_url, body)) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp request(operation) do
    ExAws.request(operation)
  rescue
    e -> {:error, Exception.message(e)}
  catch
    # A response body our XML parser can't handle (e.g. ElasticMQ's empty
    # 200 body for DeleteMessage) exits the process rather than raising —
    # xmerl reports malformed/empty XML via :exit, not an exception. Catch
    # it here so callers get a normal {:error, reason} to pattern-match on
    # instead of crashing.
    :exit, reason -> {:error, reason}
  end
end
