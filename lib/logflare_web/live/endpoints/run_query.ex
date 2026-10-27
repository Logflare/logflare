defmodule LogflareWeb.Endpoints.RunQuery do
  use LogflareWeb, :html
  use Phoenix.Component

  alias LogflareWeb.QueryComponents
  alias LogflareWeb.QueryErrorHelpers
  alias Phoenix.LiveView.JS

  attr :result, :map, required: true

  defp query_output(assigns) do
    assigns =
      assigns
      |> assign(:plan, Map.get(assigns.result, :query_plan))
      |> assign(
        :results_title,
        if(assigns.result.kind == :sandboxed_endpoint,
          do: "Sandbox Query Results",
          else: "Results"
        )
      )
      |> assign(
        :formatted_rows,
        Jason.encode!(assigns.result.rows) |> Jason.Formatter.pretty_print()
      )

    ~H"""
    <section id="endpoint-test-output" phx-hook="LiveTooltips">
      <.tabs id="endpoint-test-output-tabs" aria-label="Test query output" class="tw-mb-3 tw-border-0 tw-border-b tw-border-solid tw-border-zinc-700" tab_class="tw-border-b-2 tw-border-solid tw-border-transparent tw-text-zinc-400 [&.active]:tw-border-blue-500 [&.active]:tw-text-white">
        <:tab id={if @plan, do: "endpoint-test-results", else: "endpoint-test-results-only"} label="Results" active on_select={copy_output_tooltip("Copy results to clipboard")}>
          <h5 class={["tw-text-white", @plan && "tw-sr-only"]}>
            {@results_title}
          </h5>
          <code id={"endpoint-test-rows-#{@result.run_id}"} phx-mounted={@plan && JS.exec("phx-click", to: "#endpoint-test-results-tab")} class="tw-block tw-overflow-x-auto tw-whitespace-pre-wrap tw-rounded tw-bg-zinc-800 tw-p-2 tw-text-xs tw-text-white">{@formatted_rows}</code>
          <div :if={is_integer(@result.total_bytes_processed)} class="tw-mt-2 tw-text-right">
            <QueryComponents.query_cost bytes={@result.total_bytes_processed} />
          </div>
        </:tab>

        <:tab :if={@plan} id="endpoint-test-plan" label="Query plan" on_select={copy_output_tooltip("Copy plan to clipboard") |> JS.push("load-query-plan")} busy={@plan.loading == true}>
          <div :if={@plan.loading} id="endpoint-test-plan-loading" role="status">
            {live_react_component("Components.Loader", %{}, id: "endpoint-test-plan-loader")}
            <span class="tw-sr-only">Loading query plan…</span>
          </div>
          <pre :if={@plan.ok?} class="tw-mb-2 tw-overflow-x-auto tw-whitespace-pre tw-rounded tw-bg-zinc-800 tw-p-2 tw-text-xs tw-text-white"><code>{@plan.result}</code></pre>
          <.alert :if={@plan.failed} variant="warning">
            <strong>Could not generate query plan.</strong>
            <br />
            {QueryErrorHelpers.query_error_message(@plan.failed)}
          </.alert>
        </:tab>

        <:actions>
          <.clipboard_button id="endpoint-test-output-copy" to="#endpoint-test-output-tabs-panels > .tab-pane.active code" label="" class="btn-sm logflare-tooltip" title="Copy results to clipboard" aria-label="Copy results to clipboard" />
        </:actions>
      </.tabs>
    </section>
    """
  end

  @spec copy_output_tooltip(String.t()) :: JS.t()
  defp copy_output_tooltip(text) do
    JS.remove_attribute("title", to: "#endpoint-test-output-copy")
    |> JS.set_attribute({"data-original-title", text}, to: "#endpoint-test-output-copy")
    |> JS.set_attribute({"aria-label", text}, to: "#endpoint-test-output-copy")
  end

  attr :endpoint, :map, required: true
  attr :form, :map, required: true
  attr :declared_params, :list, default: []
  attr :result, :map, default: nil
  attr :editing, :boolean, default: false

  def endpoint_test(assigns) do
    ~H"""
    <aside id="endpoint-test" class="tw-flex tw-min-w-0 tw-flex-col tw-gap-8">
      <div>
        <h3 class="tw-mb-2 tw-text-xl tw-font-semibold tw-text-white">Test your endpoint</h3>
        <p class="tw-mb-5 tw-text-sm tw-text-zinc-400">
          {if @editing,
            do: "Test your current changes before saving.",
            else: "Enter parameter values and optionally add a sandbox query before running the endpoint."}
        </p>

        <section class="tw-rounded tw-border tw-bg-dashboard-grey tw-p-5">
          <.endpoint_test_form endpoint={@endpoint} form={@form} declared_params={@declared_params} />
          <.test_result result={@result} />
        </section>
      </div>

      <.endpoint_call_examples :if={not @editing} endpoint={@endpoint} form={@form} declared_params={@declared_params} />
    </aside>
    """
  end

  attr :endpoint, :map, required: true
  attr :form, :map, required: true
  attr :declared_params, :list, default: []

  def endpoint_test_form(assigns) do
    ~H"""
    <.form :let={f} for={@form} id="endpoint-test-form" phx-change="validate" phx-submit="run-query" aria-label="Test endpoint">
      <.input field={f[:query]} type="hidden" class="" />

      <.parameter_fields :if={@declared_params != []} form={f} declared_params={@declared_params} />
      <.dynamic_reservation_field :if={@endpoint.enable_dynamic_reservation} form={f} class="form-group tw-mb-5" />
      <.sandbox_query_fields :if={@endpoint.sandboxable} form={f} />

      <.button type="submit" variant="secondary">Test endpoint</.button>
    </.form>
    """
  end

  attr :form, :map, required: true
  attr :declared_params, :list, default: []

  defp parameter_fields(assigns) do
    ~H"""
    <fieldset class="tw-mb-5">
      <legend class="tw-mb-3 tw-text-base tw-font-semibold tw-text-white">Parameters</legend>
      <.inputs_for :let={params_f} field={@form[:params]}>
        <div :for={key <- @declared_params} class="tw-mb-3">
          {label(params_f, key, key, class: "tw-mb-1 tw-block tw-text-sm tw-text-white")}
          <.input field={params_f[key]} class="form-control tw-w-full" />
        </div>
      </.inputs_for>
    </fieldset>
    """
  end

  attr :form, :map, required: true

  defp sandbox_query_fields(assigns) do
    ~H"""
    <fieldset class="tw-mb-5 tw-border-0 tw-border-t tw-border-solid tw-border-zinc-700 tw-p-0 tw-pt-5">
      <legend class="tw-mb-2 tw-text-base tw-font-semibold tw-text-white">
        Sandbox query <span class="tw-font-normal tw-text-zinc-500">(optional)</span>
      </legend>
      <p class="tw-mb-4 tw-text-sm tw-text-zinc-400">
        Test how consumers can query your endpoint using the <code class="tw-whitespace-nowrap tw-rounded tw-bg-zinc-700 tw-px-1 tw-text-xs">?sql=</code> or <code class="tw-whitespace-nowrap tw-rounded tw-bg-zinc-700 tw-px-1 tw-text-xs">?lql=</code> parameter.
        Queries are restricted to the CTE tables defined in your endpoint query.
      </p>

      <div class="tw-mb-3 tw-flex tw-gap-4">
        <label class="tw-flex tw-items-center tw-gap-1 tw-text-sm tw-text-white">
          <.input field={@form[:query_mode]} type="radio" id={"#{@form[:query_mode].id}_sql"} value="sql" checked={@form[:query_mode].value == "sql"} class="" /> SQL
        </label>
        <label class="tw-flex tw-items-center tw-gap-1 tw-text-sm tw-text-white">
          <.input field={@form[:query_mode]} type="radio" id={"#{@form[:query_mode].id}_lql"} value="lql" checked={@form[:query_mode].value == "lql"} class="" /> LQL
        </label>
      </div>

      {textarea(@form, :sandbox_query,
        placeholder: "SELECT * FROM cte_name WHERE condition...",
        rows: 6,
        class: "form-control tw-w-full tw-rounded tw-border-zinc-700 tw-bg-zinc-900 tw-p-2 tw-font-mono tw-text-sm tw-text-white"
      )}

      <label class="tw-mt-3 tw-flex tw-items-center tw-text-sm tw-text-white">
        <.input field={@form[:show_transformed]} type="checkbox" class="tw-mr-2" /> Show transformed query
      </label>
    </fieldset>
    """
  end

  attr :form, :map, required: true
  attr :class, :string, default: nil

  defp dynamic_reservation_field(assigns) do
    ~H"""
    <div class={@class}>
      {label(@form, :reservation, "BigQuery Reservation")}
      <.input field={@form[:reservation]} phx-debounce="300" placeholder="projects/{project}/locations/{location}/reservations/{reservation}" />
    </div>
    """
  end

  attr :endpoint, :map, required: true
  attr :form, :map, required: true
  attr :declared_params, :list, default: []

  defp endpoint_call_examples(assigns) do
    reservation =
      case assigns.form[:reservation].value do
        value when is_binary(value) and value != "" -> String.replace(value, ["'", "\""], "")
        _ -> "projects/PROJECT/locations/LOCATION/reservations/RESERVATION"
      end

    headers =
      [
        "-H 'X-API-KEY: YOUR-ACCESS-TOKEN'",
        if(assigns.endpoint.redact_pii, do: "-H 'LF-ENDPOINT-REDACT-PII: true'"),
        if(assigns.endpoint.enable_dynamic_reservation,
          do: "-H 'LF-ENDPOINT-BIGQUERY-RESERVATION: #{reservation}'"
        ),
        "-H 'Content-Type: application/json; charset=utf-8'"
      ]
      |> Enum.reject(&is_nil/1)

    assigns = assign(assigns, :headers, headers)

    ~H"""
    <section id="endpoint-call-examples">
      <h3 class="tw-mb-3 tw-text-xl tw-font-semibold tw-text-white">Call your endpoint</h3>
      <div class="tw-flex tw-flex-col tw-gap-3">
        <.curl_example title="By UUID" identifier={@endpoint.token} headers={@headers} declared_params={@declared_params} />
        <.curl_example :if={@endpoint.enable_auth} title="By name" identifier={@endpoint.name} headers={@headers} declared_params={@declared_params} />
      </div>
    </section>
    """
  end

  attr :title, :string, required: true
  attr :identifier, :any, required: true

  attr :headers, :list,
    default: [
      "-H 'X-API-KEY: YOUR-ACCESS-TOKEN'",
      "-H 'Content-Type: application/json; charset=utf-8'"
    ]

  attr :declared_params, :list, default: []

  defp curl_example(assigns) do
    assigns =
      assign(
        assigns,
        :command,
        curl_command(assigns.identifier, assigns.headers, assigns.declared_params)
      )

    ~H"""
    <div class="tw-group tw-rounded tw-bg-zinc-800 tw-p-4">
      <div class="tw-mb-2 tw-flex tw-items-center tw-justify-between tw-gap-3">
        <h4 class="tw-mb-0 tw-text-sm tw-font-semibold tw-text-zinc-300">{@title}</h4>
        <.clipboard_button text={@command} label="" class="btn-sm tw-ml-auto tw-shrink-0 tw-invisible group-hover:tw-visible group-focus-within:tw-visible" title={"Copy #{@title} request"} aria-label={"Copy #{@title} request"} />
      </div>
      <pre class="tw-mb-0 tw-overflow-x-auto tw-whitespace-pre-wrap tw-break-words"><code class="tw-text-xs tw-text-zinc-200">{@command}</code></pre>
    </div>
    """
  end

  attr :result, :map, default: nil

  defp test_result(assigns) do
    ~H"""
    <div :if={@result}>
      <div :if={@result.kind == :endpoint and @result.status == :error} class="tw-mt-4">
        <.alert variant="danger">
          <strong>Query error!</strong>
          <br />
          <span>{@result.error}</span>
        </.alert>
      </div>

      <div :if={@result.kind == :sandboxed_endpoint and @result.status == :error} class="tw-mt-5">
        <.alert variant="danger">
          {@result.error}
        </.alert>
      </div>

      <div :if={@result.status == :ok} class="tw-mt-5">
        <.query_output result={@result} />
        <details :if={@result.transformed_query} class="tw-mt-4">
          <summary class="tw-cursor-pointer tw-text-sm tw-font-semibold tw-text-white">
            Show Transformed Query
          </summary>
          <code class="tw-mt-2 tw-block tw-whitespace-pre-wrap tw-rounded tw-bg-zinc-900 tw-p-2 tw-text-xs tw-text-white">
            {@result.transformed_query}
          </code>
        </details>
      </div>
    </div>
    """
  end

  defp curl_command(identifier, headers, declared_params) do
    identifier = URI.encode(to_string(identifier), &URI.char_unreserved?/1)
    url = "https://api.logflare.app/api/endpoints/query/#{identifier}"

    parameter_arguments =
      case declared_params do
        [] -> []
        params -> ["-G " <> Enum.map_join(params, " ", fn param -> ~s(-d "#{param}=VALUE") end)]
      end

    separator = " " <> "\\" <> "\n  "
    Enum.join([~s(curl "#{url}") | headers] ++ parameter_arguments, separator)
  end
end
