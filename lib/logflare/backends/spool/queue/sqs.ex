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
    # ElasticMQ returns 200 with an empty body for DeleteMessage, which the XML
    # parser raises on. A 200 means the delete landed — ignore parse errors.
    request(ExAws.SQS.delete_message(queue_url, handle))
    :ok
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
    try do
      ExAws.request(operation)
    rescue
      e -> {:error, Exception.message(e)}
    end
  end
end
