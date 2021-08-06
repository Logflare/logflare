defmodule LogflareWeb.LogChannel do
  use LogflareWeb, :channel

  alias Logflare.{Logs, Sources, Source}
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint

  def join("logs:" <> source_uuid, payload, socket),
    do: join(source_uuid, payload, socket)

  def join("logs:elixir:" <> source_uuid, payload, socket),
    do: join(source_uuid, payload, socket)

  def join("logs:elixir:logger:" <> source_uuid, payload, socket),
    do: join(source_uuid, payload, socket)

  def join("logs:erlang:" <> source_uuid, payload, socket),
    do: join(source_uuid, payload, socket)

  def join("logs:erlang:logger:" <> source_uuid, payload, socket),
    do: join(source_uuid, payload, socket)

  def join("logs:erlang:lager:" <> source_uuid, payload, socket),
    do: join(source_uuid, payload, socket)

  def join(source_uuid, _payload, socket) do
    case Sources.Cache.get_by_and_preload_rules(token: source_uuid) do
      %Source{} = source ->
        url = Routes.source_url(Endpoint, :show, source.id)
        socket = socket |> assign(:source, source)

        send(
          self,
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
    source = socket.assigns.source

    case Logs.ingest_logs(batch, source) do
      :ok ->
        push(socket, "batch", %{message: "Handled batch"})
        {:noreply, socket}

      {:error, errors} ->
        push(socket, "batch", %{message: "Batch error", errors: errors})
        {:noreply, socket}
    end
  end

  def handle_in("ping", payload, socket) do
    push(socket, "pong", %{message: "Pong"})
    {:noreply, socket}
  end

  def handle_in(event, payload, socket) do
    send(
      self,
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
