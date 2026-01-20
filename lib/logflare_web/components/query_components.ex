defmodule LogflareWeb.QueryComponents do
  use Phoenix.Component

  import Phoenix.HTML.Form
  import LogflareWeb.ErrorHelpers

  alias Logflare.Backends.Backend
  alias LogflareWeb.Utils
  alias Phoenix.LiveView.JS

  attr :backends, :list, required: true
  attr :form, :any, required: true
  attr :field, :atom, default: :backend_id
  attr :show_language, :boolean, default: true
  attr :label, :string, default: nil

  slot :help

  def backend_select(assigns) do
    assigns =
      assign_new(assigns, :options, fn ->
        [{"Default (BigQuery)", nil}] ++
          Enum.map(assigns.backends, fn backend ->
            {"#{backend.name} (#{backend.type})", backend.id}
          end)
      end)

    ~H"""
    <div class="form-group">
      <label :if={@label}>{@label}</label>
      <small :if={render_slot(@help)} class="form-text text-muted">{render_slot(@help)}</small>
      <select id={@form[@field].id} name={@form[@field].name} class="form-control">
        {options_for_select(@options, @form[@field].value)}
      </select>
      {error_tag(@form, @field)}
      <div :if={@show_language} class="tw-mt-2">
        <strong>Query Language: <span id="query-language">{format_query_language(@form[@field].value, @backends)}</span></strong>
      </div>
    </div>
    """
  end

  defp format_query_language(nil, _backends), do: "BigQuery SQL"

  defp format_query_language(backend_id, backends) do
    backend = Enum.find(backends, &(to_string(&1.id) == backend_id))
    format_backend_language(backend)
  end

  defp format_backend_language(%Backend{type: :clickhouse}), do: "ClickHouse SQL"
  defp format_backend_language(%Backend{type: :postgres}), do: "Postgres SQL"
  defp format_backend_language(_), do: "BigQuery SQL"

  attr :bytes, :integer, default: nil

  def query_cost(assigns) do
    {size, unit} = Utils.humanize_bytes(assigns.bytes)

    assigns =
      assigns
      |> assign(unit: unit)
      |> assign(size: Decimal.from_float(size) |> Decimal.normalize())

    ~H"""
    <div :if={is_number(@bytes)} class="tw-text-sm">
      {@size} {@unit} processed
    </div>
    """
  end

  attr :sql_string, :string
  attr :params, :list, default: [], doc: "List of %GoogleApi.BigQuery.V2.Model.QueryParameter{}"

  def formatted_sql(assigns) do
    assigns =
      assigns
      |> assign_new(:formatted_sql, fn ->
        {:ok, formatted} =
          Utils.sql_params_to_sql(assigns.sql_string, assigns.params)
          |> prepare_table_name()
          |> SqlFmt.format_query()

        formatted
      end)

    ~H"""
    <div class="tw-group tw-relative">
      <.link
        class="tw-text-[0.65rem] tw-py-1 tw-px-2 tw-rounded
        tw-text-sm
        group-hover:tw-visible tw-invisible tw-absolute tw-capitalize tw-top-0 tw-right-0 tw-bg-[#6c757d] tw-hover:tw-bg-[#5a6268] tw-text-white tw-no-underline"
        phx-click={
          JS.dispatch("logflare:copy-to-clipboard",
            detail: %{
              text: @formatted_sql
            }
          )
        }
        data-toggle="tooltip"
        data-placement="top"
        title="Copy to clipboard"
      >
        <i class="fa fa-clone" aria-hidden="true"></i> copy
      </.link>
      <pre class="tw-flex-grow"><code class="sql"><%= @formatted_sql %></code></pre>
    </div>
    """
  end

  defp prepare_table_name(sql) do
    Regex.replace(
      ~r/`([^`]+)`\.([^\s]+)/,
      sql,
      fn _, project, rest ->
        "`#{project}.#{rest}`"
      end
    )
  end
end
