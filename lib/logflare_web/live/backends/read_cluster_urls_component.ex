defmodule LogflareWeb.Backends.ReadClusterUrlsComponent do
  @moduledoc """
  Backend-form editor for a ClickHouse backend's per-caller read-cluster URLs: a dynamic
  list of free-form `label -> URL` rows plus a default cluster, folded into `read_only_urls`.

  Field names and element ids are keyed on a per-row `ref` that is never reused, so removing
  a row does not renumber the rows after it.
  """
  use LogflareWeb, :live_component

  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1, is_non_empty_map: 1]

  require Logger

  alias Logflare.Backends.Backend

  @type row :: {non_neg_integer(), String.t(), String.t()}

  @impl true
  def update(assigns, socket) do
    socket = assign(socket, assigns)
    socket = assign_new(socket, :rows, fn -> initial_rows(socket.assigns.backend) end)
    socket = assign_new(socket, :next_ref, fn -> length(socket.assigns.rows) end)

    {:ok,
     assign_new(socket, :default_read_cluster, fn ->
       input_value(socket.assigns.form, :default_read_cluster) || ""
     end)}
  end

  @impl true
  def handle_event("sync", %{"backend" => %{"config" => config}}, socket) when is_map(config) do
    rows =
      Enum.map(socket.assigns.rows, fn {ref, label, url} ->
        {
          ref,
          Map.get(config, "read_cluster_label_#{ref}", label),
          Map.get(config, "read_cluster_url_#{ref}", url)
        }
      end)

    {:noreply,
     assign(socket,
       rows: rows,
       default_read_cluster:
         Map.get(config, "default_read_cluster", socket.assigns.default_read_cluster)
     )}
  end

  def handle_event("sync", _params, socket) do
    Logger.warning("Unexpected sync payload shape in ReadClusterUrlsComponent",
      backend_id: socket.assigns.backend && socket.assigns.backend.id
    )

    {:noreply, socket}
  end

  def handle_event("add_row", _params, socket) do
    %{next_ref: ref, rows: rows} = socket.assigns

    {:noreply, assign(socket, rows: rows ++ [{ref, "", ""}], next_ref: ref + 1)}
  end

  def handle_event("remove_row", %{"ref" => ref}, socket) do
    %{next_ref: next_ref, rows: rows} = socket.assigns
    ref = String.to_integer(ref)

    case Enum.reject(rows, fn {row_ref, _label, _url} -> row_ref == ref end) do
      [] -> {:noreply, assign(socket, rows: [{next_ref, "", ""}], next_ref: next_ref + 1)}
      remaining -> {:noreply, assign(socket, :rows, remaining)}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <div class="form-group">
        {label(@form, :labeled_read_pool_size, "Read Cluster Pool Size")}
        {text_input(@form, :labeled_read_pool_size,
          class: "form-control",
          value: input_value(@form, "labeled_read_pool_size") || 32
        )}
        <small class="form-text text-muted">
          Connections per node for each read cluster below. The default read cluster uses the primary read pool size instead, since it also absorbs unrecognized callers and failover traffic.
        </small>
      </div>

      <div class="form-group">
        <label>Read-Only Cluster URLs (Optional)</label>
        <small class="form-text text-muted">
          Optionally add multiple read-only cluster URLs for the ability to route endpoint queries to a specific cluster.
        </small>

        <%= for {row_ref, row_label, row_url} <- @rows do %>
          <div class="form-row tw-flex tw-gap-2 tw-mb-2" id={"read-cluster-row-#{row_ref}"}>
            {text_input(@form, "read_cluster_label_#{row_ref}",
              value: row_label,
              placeholder: "caller label",
              class: "form-control",
              phx_change: "sync",
              phx_target: @myself,
              phx_debounce: "blur"
            )}
            {text_input(@form, "read_cluster_url_#{row_ref}",
              value: row_url,
              placeholder: "https://read-cluster:8443",
              class: "form-control",
              phx_change: "sync",
              phx_target: @myself,
              phx_debounce: "blur"
            )}
            <button type="button" class="btn btn-outline-danger" phx-click="remove_row" phx-value-ref={row_ref} phx-target={@myself}>
              <i class="fas fa-minus"></i>
            </button>
          </div>
        <% end %>

        <button type="button" class="btn btn-outline-secondary" phx-click="add_row" phx-target={@myself}>
          <i class="fas fa-plus"></i> Add read cluster
        </button>
      </div>

      <div class="form-group">
        {label(@form, :default_read_cluster, default_read_cluster_label(cluster_configured?(@rows)))}
        {text_input(@form, :default_read_cluster,
          value: @default_read_cluster,
          class: "form-control",
          placeholder: "caller label",
          required: cluster_configured?(@rows),
          phx_change: "sync",
          phx_target: @myself,
          phx_debounce: "blur"
        )}
        <small class="form-text text-muted">
          The caller label whose cluster absorbs unrecognized or absent callers. Must match a label above.
        </small>
        <small :if={default_read_cluster_missing?(@rows, @default_read_cluster)} class="form-text tw-text-red-500">
          Required once a read cluster is configured. Without it, callers that send no label read from the ingest cluster instead.
        </small>
      </div>
    </div>
    """
  end

  @doc """
  Folds the flat `read_cluster_label_<ref>` / `read_cluster_url_<ref>` form fields into a
  `read_only_urls` map, dropping blank rows and erroring on duplicate labels (checked
  here since the map collapse hides them). URLs may repeat across labels. Refs are only
  used to pair a label with its URL, so gaps left by removed rows are irrelevant.
  """
  @spec assemble_read_only_urls(map()) :: {:ok, map()} | {:error, String.t()}
  def assemble_read_only_urls(config) when is_map(config) do
    labeled =
      for {"read_cluster_label_" <> i, label} <- config,
          is_non_empty_binary(label),
          do: {i, label}

    labels = Enum.map(labeled, fn {_i, label} -> label end)

    duplicate_labels =
      for {label, count} <- Enum.frequencies(labels), count > 1, do: label

    case duplicate_labels do
      [] ->
        read_only_urls =
          for {i, label} <- labeled,
              url = Map.get(config, "read_cluster_url_#{i}"),
              is_non_empty_binary(url),
              into: %{},
              do: {label, url}

        config =
          config
          |> Map.reject(fn {key, _value} -> read_cluster_form_key?(key) end)
          |> Map.put("read_only_urls", read_only_urls)

        {:ok, config}

      dupes ->
        {:error, "Duplicate read cluster labels are not allowed: #{Enum.join(dupes, ", ")}"}
    end
  end

  @spec initial_rows(Backend.t() | nil) :: [row()]
  defp initial_rows(%Backend{config: %{read_only_urls: urls}}) when is_non_empty_map(urls) do
    urls
    |> Enum.sort_by(fn {label, _url} -> label end)
    |> Enum.with_index(fn {label, url}, ref -> {ref, label, url} end)
  end

  defp initial_rows(_backend), do: [{0, "", ""}]

  @spec cluster_configured?([row()]) :: boolean()
  defp cluster_configured?(rows) do
    Enum.any?(rows, fn {_ref, label, url} ->
      is_non_empty_binary(label) and is_non_empty_binary(url)
    end)
  end

  @spec default_read_cluster_missing?([row()], String.t() | nil) :: boolean()
  defp default_read_cluster_missing?(_rows, default) when is_non_empty_binary(default), do: false

  defp default_read_cluster_missing?(rows, _default), do: cluster_configured?(rows)

  @spec default_read_cluster_label(boolean()) :: String.t()
  defp default_read_cluster_label(true), do: "Default Read Cluster"
  defp default_read_cluster_label(false), do: "Default Read Cluster (Optional)"

  @spec read_cluster_form_key?(String.t()) :: boolean()
  defp read_cluster_form_key?(key) do
    String.starts_with?(key, "read_cluster_label_") or
      String.starts_with?(key, "read_cluster_url_")
  end
end
