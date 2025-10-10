defmodule LogflareWeb.QueryComponents do
  use Phoenix.Component

  alias LogflareWeb.Utils
  alias Phoenix.LiveView.JS

  attr :bytes, :integer, default: nil

  def query_cost(assigns) do
    {size, unit} = Utils.humanize_bytes(assigns.bytes)

    assigns =
      assigns
      |> assign(unit: unit)
      |> assign(size: Decimal.from_float(size) |> Decimal.normalize())

    ~H"""
    <div :if={is_number(@bytes)} class="tw-text-sm">
      <%= @size %> <%= @unit %> processed
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
end
