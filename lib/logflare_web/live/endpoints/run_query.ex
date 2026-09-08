defmodule LogflareWeb.Endpoints.RunQuery do
  use LogflareWeb, :html
  use Phoenix.Component

  alias LogflareWeb.QueryComponents

  attr :form, :map, required: true
  attr :endpoint_changeset, :map, required: true
  attr :declared_params, :list, default: []

  def query_form(assigns) do
    ~H"""
    <.form :let={f} for={@form} phx-submit="run-query" class="tw-min-h-[80px]">
      {hidden_input(f, :query)}

      <.inputs_for :let={params_f} field={f[:params]}>
        <div :for={key <- @declared_params}>
          {label(params_f, key, key)}
          {text_input(params_f, key)}
        </div>
      </.inputs_for>

      <.dynamic_reservation_field :if={Ecto.Changeset.get_field(@endpoint_changeset, :enable_dynamic_reservation)} form={f} class="form-group tw-mt-2" />

      {submit("Test query", class: "btn btn-secondary")}
    </.form>
    """
  end

  attr :result, :map, default: nil

  def query_result(%{result: %{kind: :endpoint, status: :ok}} = assigns) do
    ~H"""
    <div>
      <div class="tw-flex tw-justify-between">
        <h5 class="tw-text-white">Results</h5>

        <QueryComponents.query_cost :if={is_integer(@result.total_bytes_processed)} bytes={@result.total_bytes_processed} />
      </div>

      <code class="tw-whitespace-pre-wrap tw-overflow-x-auto tw-block tw-text-white tw-bg-zinc-800 tw-rounded tw-p-2 tw-text-xs">{Jason.encode!(@result.rows) |> Jason.Formatter.pretty_print()}</code>
    </div>
    """
  end

  def query_result(%{result: %{error: error}} = assigns) when not is_nil(error) do
    ~H"""
    <div class="tw-mt-2">
      <.alert variant="danger">
        <strong>Query error!</strong>
        <br />
        <span>{@result.error}</span>
      </.alert>
    </div>
    """
  end

  def query_result(assigns), do: ~H""

  attr :endpoint, :map, required: true
  attr :form, :map, required: true
  attr :declared_params, :list, default: []
  attr :result, :map, default: nil

  def endpoint_test(assigns) do
    ~H"""
    <aside id="endpoint-test" class="tw-flex tw-min-w-0 tw-flex-col tw-gap-8">
      <div>
        <h3 class="tw-mb-2 tw-text-xl tw-font-semibold tw-text-white">Test your endpoint</h3>
        <p class="tw-mb-5 tw-text-sm tw-text-zinc-400">
          Enter parameter values and optionally add a sandbox query before running the endpoint.
        </p>

        <section class="tw-rounded tw-border tw-bg-dashboard-grey tw-p-5">
          <.endpoint_test_form endpoint={@endpoint} form={@form} declared_params={@declared_params} />
          <.test_result result={@result} />
        </section>
      </div>

      <.endpoint_call_examples endpoint={@endpoint} declared_params={@declared_params} />
    </aside>
    """
  end

  attr :endpoint, :map, required: true
  attr :form, :map, required: true
  attr :declared_params, :list, default: []

  def endpoint_test_form(assigns) do
    ~H"""
    <.form :let={f} for={@form} phx-submit="run-query" aria-label="Test endpoint">
      {hidden_input(f, :query)}

      <.parameter_fields form={f} declared_params={@declared_params} dynamic_reservation?={@endpoint.enable_dynamic_reservation} />
      <.sandbox_query_fields :if={@endpoint.sandboxable} form={f} />

      {submit("Test endpoint", class: "btn btn-secondary")}
    </.form>
    """
  end

  attr :form, :map, required: true
  attr :declared_params, :list, default: []
  attr :dynamic_reservation?, :boolean, default: false

  defp parameter_fields(assigns) do
    ~H"""
    <fieldset class="tw-mb-5">
      <legend class="tw-mb-3 tw-text-base tw-font-semibold tw-text-white">Parameters</legend>
      <p :if={@declared_params == []} class="tw-mb-0 tw-text-sm tw-text-zinc-500">
        This endpoint has no parameters.
      </p>
      <.inputs_for :let={params_f} field={@form[:params]}>
        <div :for={key <- @declared_params} class="tw-mb-3">
          {label(params_f, key, key, class: "tw-mb-1 tw-block tw-text-sm tw-text-white")}
          {text_input(params_f, key, class: "form-control tw-w-full")}
        </div>
      </.inputs_for>

      <.dynamic_reservation_field :if={@dynamic_reservation?} form={@form} class="form-group tw-mb-0 tw-mt-3" />
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
          {radio_button(@form, :query_mode, "sql")} SQL
        </label>
        <label class="tw-flex tw-items-center tw-gap-1 tw-text-sm tw-text-white">
          {radio_button(@form, :query_mode, "lql")} LQL
        </label>
      </div>

      {textarea(@form, :sandbox_query,
        placeholder: "SELECT * FROM cte_name WHERE condition...",
        rows: 6,
        class: "form-control tw-w-full tw-rounded tw-border-zinc-700 tw-bg-zinc-900 tw-p-2 tw-font-mono tw-text-sm tw-text-white"
      )}

      <label class="tw-mt-3 tw-flex tw-items-center tw-text-sm tw-text-white">
        {checkbox(@form, :show_transformed, class: "tw-mr-2")} Show transformed query
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
      {text_input(@form, :reservation,
        class: "form-control",
        placeholder: "projects/{project}/locations/{location}/reservations/{reservation}"
      )}
    </div>
    """
  end

  attr :endpoint, :map, required: true
  attr :declared_params, :list, default: []

  defp endpoint_call_examples(assigns) do
    ~H"""
    <section id="endpoint-call-examples">
      <h3 class="tw-mb-3 tw-text-xl tw-font-semibold tw-text-white">Call your endpoint</h3>
      <div class="tw-flex tw-flex-col tw-gap-3">
        <.curl_example title="By UUID" identifier={@endpoint.token} declared_params={@declared_params} />
        <.curl_example :if={@endpoint.enable_auth} title="By name" identifier={@endpoint.name} declared_params={@declared_params} />
        <.curl_example
          title="With per-request PII redaction"
          identifier={@endpoint.token}
          headers={[
            "-H 'X-API-KEY: YOUR-ACCESS-TOKEN'",
            "-H 'LF-ENDPOINT-REDACT-PII: true'",
            "-H 'Content-Type: application/json; charset=utf-8'"
          ]}
          declared_params={@declared_params}
        />
        <.curl_example
          :if={@endpoint.enable_dynamic_reservation}
          title="With dynamic BigQuery reservation"
          identifier={@endpoint.token}
          headers={[
            "-H 'X-API-KEY: YOUR-ACCESS-TOKEN'",
            "-H 'LF-ENDPOINT-BIGQUERY-RESERVATION: projects/PROJECT/locations/LOCATION/reservations/RESERVATION'",
            "-H 'Content-Type: application/json; charset=utf-8'"
          ]}
          declared_params={@declared_params}
        />
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

      <div :if={@result.kind == :sandbox and @result.status == :error} class="tw-mt-5">
        <.alert variant="danger">
          {@result.error}
        </.alert>
      </div>

      <div :if={@result.status == :ok} class="tw-mt-5">
        <div class="tw-mb-2 tw-flex tw-justify-between">
          <h5 class="tw-text-white">
            {if @result.kind == :sandbox, do: "Sandbox Query Results", else: "Results"}
          </h5>
          <QueryComponents.query_cost :if={is_integer(@result.total_bytes_processed)} bytes={@result.total_bytes_processed} />
        </div>
        <code class="tw-block tw-overflow-x-auto tw-whitespace-pre-wrap tw-rounded tw-bg-zinc-800 tw-p-2 tw-text-xs tw-text-white">{Jason.encode!(@result.rows) |> Jason.Formatter.pretty_print()}</code>
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
