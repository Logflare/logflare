defmodule LogflareWeb.Backends.QueryClassSettingsComponent do
  use LogflareWeb, :live_component

  import Logflare.Endpoints.ClickHouseSettings, only: [is_valid_timeout: 1]

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryClassSettings

  @fields [
    {"priority", "Priority"},
    {"max_threads", "Max threads"},
    {"max_memory_usage", "Max memory usage (bytes)"},
    {"max_execution_time", "Max execution time (seconds)"},
    {"max_rows_to_read", "Max rows to read"},
    {"max_bytes_to_read", "Max bytes to read"}
  ]
  @numeric_keys Enum.map(@fields, &elem(&1, 0))
  @override_classes QueryClassSettings.classes()
  @classes ["default" | @override_classes]
  @invalid_form "Query-class settings must contain named classes and numeric fields."
  @cache_key "read_from_filesystem_cache_if_exists_otherwise_bypass_cache"
  @hidden_keys [@cache_key, "read_overflow_mode", "timeout_overflow_mode"]

  @impl true
  def update(assigns, socket) do
    {:ok,
     socket
     |> assign(assigns)
     |> assign_new(:original_policy, fn ->
       assigns.backend.config[:query_class_settings] || %{}
     end)
     |> assign_new(:params, fn %{original_policy: policy} -> policy end)
     |> assign_new(:hidden_overrides, fn %{original_policy: policy} ->
       hidden_overrides(policy)
     end)
     |> assign_new(:error, fn -> nil end)
     |> assign_new(:message, fn -> nil end)
     |> assign_new(:input_revisions, &fresh_input_revisions/0)
     |> assign(:classes, @classes)
     |> assign(:fields, @fields)}
  end

  @impl true
  def handle_event("save", %{"query_class_settings" => params} = payload, socket)
      when is_map(params) do
    case form_params(params) do
      {:ok, params} -> submit(Map.fetch(payload, "reset_class"), params, socket)
      :error -> invalid_form(socket)
    end
  end

  def handle_event("save", _params, socket), do: invalid_form(socket)
  def handle_event("clear", _params, socket), do: save_settings(%{}, socket)

  @spec submit(:error | {:ok, term()}, map(), Phoenix.LiveView.Socket.t()) ::
          {:noreply, Phoenix.LiveView.Socket.t()}
  defp submit(:error, params, socket) do
    settings =
      Map.merge(socket.assigns.hidden_overrides, parse_settings(params), fn _class,
                                                                            hidden,
                                                                            limits ->
        Map.merge(hidden, limits)
      end)

    save_settings(settings, assign(socket, :params, params))
  end

  defp submit({:ok, class}, params, socket) when class in @override_classes do
    {:noreply,
     assign(socket,
       params: Map.delete(params, class),
       hidden_overrides: Map.delete(socket.assigns.hidden_overrides, class),
       input_revisions: Map.put(socket.assigns.input_revisions, class, Ecto.UUID.generate()),
       error: nil,
       message: "Class reset in this draft. Save query-class settings to apply it."
     )}
  end

  defp submit({:ok, _class}, _params, socket), do: invalid_form(socket)

  @spec invalid_form(Phoenix.LiveView.Socket.t()) :: {:noreply, Phoenix.LiveView.Socket.t()}
  defp invalid_form(socket), do: {:noreply, assign(socket, error: @invalid_form, message: nil)}

  @spec save_settings(map(), Phoenix.LiveView.Socket.t()) ::
          {:noreply, Phoenix.LiveView.Socket.t()}
  defp save_settings(settings, socket) do
    socket = assign(socket, :message, nil)

    case Backends.configure_query_class_settings(
           socket.assigns.actor,
           socket.assigns.backend,
           settings,
           expected_policy: socket.assigns.original_policy
         ) do
      {:ok, backend} ->
        policy = backend.config.query_class_settings

        {message, input_revisions} =
          if policy == %{} do
            {"Query-class policy disabled", fresh_input_revisions()}
          else
            {"Query-class settings saved", socket.assigns.input_revisions}
          end

        {:noreply,
         assign(socket,
           backend: backend,
           original_policy: policy,
           params: policy,
           hidden_overrides: hidden_overrides(policy),
           input_revisions: input_revisions,
           error: nil,
           message: message
         )}

      {:error, %Ecto.Changeset{} = changeset} ->
        {:noreply,
         assign(socket, :error, LogflareWeb.Utils.stringify_changeset_errors(changeset))}

      {:error, :stale_policy} ->
        {:noreply,
         assign(
           socket,
           :error,
           "The query-class policy changed. Reload and reapply your changes."
         )}

      {:error, :forbidden} ->
        {:noreply, assign(socket, :error, "You no longer have permission to edit this backend.")}

      {:error, _reason} ->
        {:noreply,
         assign(socket, :error, "Unable to save query-class settings. Reload and try again.")}
    end
  end

  @spec fresh_input_revisions() :: map()
  defp fresh_input_revisions do
    revision = Ecto.UUID.generate()
    Map.new(@classes, &{&1, revision})
  end

  @spec form_params(map()) :: {:ok, map()} | :error
  defp form_params(params) do
    Enum.reduce_while(params, {:ok, %{}}, fn
      {class, values}, {:ok, form} when class in @classes and is_map(values) ->
        fields = Map.take(values, @numeric_keys)

        if Enum.all?(Map.values(fields), &is_binary/1),
          do: {:cont, {:ok, Map.put(form, class, fields)}},
          else: {:halt, :error}

      _entry, _acc ->
        {:halt, :error}
    end)
  end

  @spec parse_settings(map()) :: map()
  defp parse_settings(params) do
    for {class, fields} <- params,
        limits = parse_fields(fields),
        limits != %{},
        into: %{},
        do: {class, limits}
  end

  @spec parse_fields(map()) :: map()
  defp parse_fields(fields) do
    for {key, value} <- fields, value != "", into: %{}, do: {key, parse_value(key, value)}
  end

  @spec parse_value(String.t(), String.t()) :: number() | String.t()
  defp parse_value("max_execution_time", value) do
    parsed =
      value
      |> String.replace_prefix(".", "0.")
      |> String.replace(~r/\.(?=[eE]|$)/, ".0")
      |> Float.parse()

    case parsed do
      {number, ""} -> number
      _other -> value
    end
  end

  defp parse_value(_key, value) do
    case Decimal.parse(value, max_digits: 64, max_exponent: 64) do
      {number, ""} ->
        if Decimal.integer?(number), do: Decimal.to_integer(number), else: value

      _other ->
        value
    end
  end

  @spec class_form(map(), String.t(), String.t()) :: Phoenix.HTML.Form.t()
  defp class_form(params, class, revision) do
    to_form(Map.get(params, class, %{}),
      as: "query_class_settings[#{class}]",
      id: "query-class-#{class}-#{revision}"
    )
  end

  @spec hidden_overrides(map()) :: map()
  defp hidden_overrides(policy) do
    for {class, limits} <- policy,
        overrides = Map.take(limits, @hidden_keys),
        overrides != %{},
        into: %{},
        do: {class, overrides}
  end

  @spec ignored_timeout?(term()) :: boolean()
  defp ignored_timeout?(%{"max_execution_time" => seconds}), do: not is_valid_timeout(seconds)
  defp ignored_timeout?(_policy), do: false

  @spec cache_override?(map(), String.t()) :: boolean()
  defp cache_override?(overrides, class), do: not is_nil(get_in(overrides, [class, @cache_key]))
end
