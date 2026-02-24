defmodule LogflareWeb.LogChannel do
  @moduledoc false
  use LogflareWeb, :channel

  alias Logflare.Backends
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias LogflareWeb.Endpoint
  alias LogflareWeb.Router.Helpers, as: Routes

  def join("logs:" <> source_uuid, _payload, socket),
    do: join(source_uuid, socket)

  def join("logs:elixir:" <> source_uuid, _payload, socket),
    do: join(source_uuid, socket)

  def join("logs:elixir:logger:" <> source_uuid, _payload, socket),
    do: join(source_uuid, socket)

  def join("logs:erlang:" <> source_uuid, _payload, socket),
    do: join(source_uuid, socket)

  def join("logs:erlang:logger:" <> source_uuid, _payload, socket),
    do: join(source_uuid, socket)

  def join("logs:erlang:lager:" <> source_uuid, _payload, socket),
    do: join(source_uuid, socket)

  def join("logs:javascript:node:" <> source_uuid, _payload, socket),
    do: join(source_uuid, socket)

  def join(source_uuid, socket) do
    case Sources.Cache.get_by_and_preload_rules(token: source_uuid) do
      %Source{} = source ->
        url = Routes.source_url(Endpoint, :show, source.id)
        socket = socket |> assign(:source, source)

        send(
          self(),
          {:notify,
           %{
             message: "💥 Connected to Logflare! Can we haz all your datas? 👀 ➡️ #{url}",
             source: %{
               name: source.name,
               token: source.token,
               url: url
             }
           }}
        )

        {:ok, socket}

      nil ->
        {:error, socket}
    end
  end

  def handle_in("batch", %{"batch" => batch}, socket) when is_list(batch) do
    source = socket.assigns.source |> Sources.refresh_source_metrics_for_ingest()

    case Backends.ingest_logs(batch, source) do
      {:ok, _count} ->
        push(socket, "batch", %{message: "Handled batch"})
        {:noreply, socket}

      {:error, errors} ->
        push(socket, "batch", %{message: "Batch error", errors: errors})
        {:noreply, socket}
    end
  end

  def handle_in("ping", _payload, socket) do
    push(socket, "pong", %{message: "Pong"})
    {:noreply, socket}
  end

  def handle_in(_event, payload, socket) do
    send(
      self(),
      {:notify,
       %{
         message: "Unhandled event type. Please verify.",
         echo_payload: inspect(payload)
       }}
    )

    {:noreply, socket}
  end

  def handle_info({:notify, payload}, socket) do
    push(socket, "notify", payload)
    {:noreply, socket}
  end
end
