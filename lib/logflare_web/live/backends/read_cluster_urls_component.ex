defmodule LogflareWeb.Backends.ReadClusterUrlsComponent do
  @moduledoc """
  Backend-form editor for a ClickHouse backend's per-caller read-cluster URLs: a dynamic
  list of free-form `label -> URL` rows plus a default cluster, folded into `read_only_urls`.
  """
  use LogflareWeb, :live_component

  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1, is_non_empty_map: 1]

  @impl true
  def update(assigns, socket) do
    socket = assign(socket, assigns)

    socket =
      if Map.has_key?(socket.assigns, :rows) do
        socket
      else
        assign(socket, :rows, initial_rows(socket.assigns.form))
      end

    {:ok, socket}
  end

  @impl true
  def handle_event("add_row", _params, socket) do
    {:noreply, assign(socket, :rows, socket.assigns.rows ++ [{"", ""}])}
  end

  def handle_event("remove_row", %{"index" => index}, socket) do
    rows =
      socket.assigns.rows
      |> List.delete_at(String.to_integer(index))
      |> case do
        [] -> [{"", ""}]
        rows -> rows
      end

    {:noreply, assign(socket, :rows, rows)}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <div class="form-group">
        <label>Per-Caller Read Cluster URLs (Optional)</label>
        <small class="form-text text-muted">
          Route ClickHouse reads to a dedicated cluster per caller. Each row maps a free-form
          caller label to a read cluster URL. The query source sends the label in the <code>LF-ENDPOINT-READ-CLUSTER</code> header and Logflare routes the read to the matching cluster. Leave empty to use the
          Read-Only URL above.
        </small>

        <%= for {{row_label, row_url}, i} <- Enum.with_index(@rows) do %>
          <div class="form-row tw-flex tw-gap-2 tw-mb-2" id={"read-cluster-row-#{i}"}>
            {text_input(@form, "read_cluster_label_#{i}",
              value: row_label,
              placeholder: "caller label",
              class: "form-control"
            )}
            {text_input(@form, "read_cluster_url_#{i}",
              value: row_url,
              placeholder: "https://read-cluster:8443",
              class: "form-control"
            )}
            <button type="button" class="btn btn-outline-danger" phx-click="remove_row" phx-value-index={i} phx-target={@myself}>
              <i class="fas fa-minus"></i>
            </button>
          </div>
        <% end %>

        <button type="button" class="btn btn-outline-secondary" phx-click="add_row" phx-target={@myself}>
          <i class="fas fa-plus"></i> Add read cluster
        </button>
      </div>

      <div class="form-group">
        {label(@form, :default_read_cluster, "Default Read Cluster (Optional)")}
        {text_input(@form, :default_read_cluster, class: "form-control", placeholder: "caller label")}
        <small class="form-text text-muted">
          The caller label whose cluster absorbs unrecognized or absent callers. Must match a label above.
        </small>
      </div>
    </div>
    """
  end

  @doc """
  Folds the flat `read_cluster_label_<i>` / `read_cluster_url_<i>` form fields into a
  `read_only_urls` map, dropping blank rows and erroring on duplicate labels (checked
  here since the map collapse hides them). URLs may repeat across labels.
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

  @spec initial_rows(Phoenix.HTML.Form.t()) :: [{String.t(), String.t()}]
  defp initial_rows(form) do
    case input_value(form, :read_only_urls) do
      urls when is_non_empty_map(urls) -> Map.to_list(urls)
      _ -> [{"", ""}]
    end
  end

  @spec read_cluster_form_key?(String.t()) :: boolean()
  defp read_cluster_form_key?(key) do
    String.starts_with?(key, "read_cluster_label_") or
      String.starts_with?(key, "read_cluster_url_")
  end
end
