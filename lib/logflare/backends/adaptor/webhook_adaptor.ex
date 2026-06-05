defmodule Logflare.Backends.Adaptor.WebhookAdaptor do
  @moduledoc """
  Backend adaptor for webhooks / HTTP posts.

  A number of other adaptors (_DataDog, Elastic, Loki, etc_) leverage this to handle the final HTTP transaction.

  ### Finch Pool Selection

  By default the pool will be selected automatically based on the `:http` configuration option.

  If you want to manually select a specific Finch pool, you can use the `:pool_name` option and provide the module name.


  ### Dynamic URL handling with URL Override

  This adaptor performs a merge on config that will prevent you from leveraging a dynamically generated URL configuration at runtime.
  To bypass this behavior, you can use the optional `:url_override` attribute.
  """

  use GenServer

  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Backends.Adaptor.HttpBased.Headers
  alias Logflare.Utils
  alias Logflare.Utils.SSRF

  @behaviour Logflare.Backends.Adaptor

  # Sentinel value substituted for secret header values by redact_config/1.
  @redacted_value "REDACTED"

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend} = args) do
    GenServer.start_link(__MODULE__, args, name: Backends.via_source(source, __MODULE__, backend))
  end

  @impl GenServer
  def init({source, backend}) do
    args = %{
      config: backend.config,
      source: source,
      backend: backend
    }

    {:ok, _pipeline_pid} = __MODULE__.Pipeline.start_link(args)
    {:ok, %{}}
  end

  @impl Logflare.Backends.Adaptor
  def cast_config(params, existing_config \\ %{}) do
    {existing_config, %{url: :string, headers: :map, http: :string, gzip: :boolean}}
    |> Ecto.Changeset.cast(params, [:url, :headers, :http, :gzip])
    |> unredact_headers(existing_config)
    |> normalize_header_keys()
    |> Logflare.Utils.default_field_value(:http, "http2")
    |> Logflare.Utils.default_field_value(:gzip, true)
  end

  # Canonicalizes submitted header names to lower case so the stored config cannot
  # hold case-variant duplicates of the same header (e.g. "Content-Type" and
  # "content-type"). Runs after unredact_headers/2 so the REDACTED-sentinel restore
  # still matches the keys the client echoed back.
  defp normalize_header_keys(changeset) do
    case Ecto.Changeset.get_change(changeset, :headers) do
      nil -> changeset
      headers -> Ecto.Changeset.put_change(changeset, :headers, Headers.normalize_keys(headers))
    end
  end

  # Restores secret header values submitted back as the "REDACTED" sentinel.
  #
  # redact_config/1 masks secret headers (e.g. Authorization) when serializing a
  # backend, so clients never receive the real value. On update the client echoes
  # the sentinel back for unchanged headers; without this, casting would overwrite
  # the stored secret with the literal string "REDACTED". For each submitted header
  # still set to the sentinel we swap in the existing stored value (dropping it if
  # there is nothing to restore). Headers the user actually changed pass through.
  defp unredact_headers(changeset, existing_config) do
    with headers when not is_nil(headers) <- Ecto.Changeset.get_change(changeset, :headers),
         existing_headers <-
           Map.get(existing_config, :headers) || Map.get(existing_config, "headers") || %{} do
      restored =
        headers
        |> Enum.reduce(%{}, fn
          {key, @redacted_value}, acc ->
            replace_header_with_existing(acc, existing_headers, key)

          {key, value}, acc ->
            Map.put(acc, key, value)
        end)

      Ecto.Changeset.put_change(changeset, :headers, restored)
    else
      _ -> changeset
    end
  end

  defp replace_header_with_existing(headers, existing_headers, key) do
    case Map.get(existing_headers, key) do
      nil -> headers
      value -> Map.put(headers, key, value)
    end
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:url])
    |> Ecto.Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
    |> Ecto.Changeset.validate_inclusion(:http, ["http1", "http2"])
    |> validate_no_ssrf()
  end

  @spec validate_no_ssrf(Ecto.Changeset.t()) :: Ecto.Changeset.t()
  defp validate_no_ssrf(changeset) do
    case Ecto.Changeset.get_field(changeset, :url) do
      nil ->
        changeset

      url ->
        host = URI.parse(url).host

        case SSRF.safe_resolve(host) do
          {:ok, _} -> changeset
          {:error, reason} -> Ecto.Changeset.add_error(changeset, :url, reason, validation: :ssrf)
        end
    end
  end

  @impl Logflare.Backends.Adaptor
  def redact_config(config) do
    Map.update(config, :headers, %{}, &redact_headers/1)
  end

  defp redact_headers(headers) do
    for {key, value} <- headers, into: %{}, do: redact_header(key, value)
  end

  defp redact_header(key, value) do
    if String.downcase(key) == "authorization" do
      {key, @redacted_value}
    else
      {key, value}
    end
  end

  @impl Logflare.Backends.Adaptor
  @spec test_connection(Backend.t()) :: :ok | {:error, term()}
  def test_connection(%Backend{} = backend) do
    test_connection(backend, [])
  end

  @doc """
  Tests connectivity by sending a custom probe `body` to the configured URL.

  Used by adaptors that wrap `WebhookAdaptor` (e.g. `LokiAdaptor`,
  `IncidentioAdaptor`) to share HTTP plumbing while choosing a payload shape
  the receiver will accept.
  """
  @spec test_connection(Backend.t(), term()) :: :ok | {:error, term()}
  def test_connection(%Backend{config: config}, body) do
    response =
      __MODULE__.Client.send(
        url: config.url,
        body: body,
        headers: Map.get(config, :headers, %{}),
        http: Map.get(config, :http),
        gzip: Map.get(config, :gzip, true)
      )

    case response do
      {:ok, %Tesla.Env{status: status}} when status in 200..299 ->
        :ok

      {:ok, %Tesla.Env{status: status, body: resp_body}} ->
        {:error, "Unexpected response: #{status} #{inspect(resp_body)}"}

      {:error, reason} ->
        {:error, "Request error: #{inspect(reason)}"}
    end
  end

  # HTTP Client
  defmodule Client do
    @moduledoc false
    alias Logflare.Backends.Adaptor.HttpBased.EgressTracer
    alias Logflare.Backends.Adaptor.HttpBased.Headers
    alias Logflare.Backends.Adaptor.HttpBased.SSRFProtection
    use Tesla, docs: false

    defguardp is_possible_pool(value)
              when not is_nil(value) and not is_boolean(value) and is_atom(value)

    def send(opts) do
      http_opt = Keyword.get(opts, :http)
      pool_name = Keyword.get(opts, :pool_name)

      adaptor =
        cond do
          is_possible_pool(pool_name) ->
            {Tesla.Adapter.Finch, name: pool_name, receive_timeout: 5_000}

          http_opt == "http2" ->
            {Tesla.Adapter.Finch, name: Logflare.FinchDefault, receive_timeout: 5_000}

          true ->
            {Tesla.Adapter.Finch, name: Logflare.FinchDefaultHttp1, receive_timeout: 5_000}
        end

      reserved = reserved_header_names(opts)

      opts =
        opts
        |> Keyword.put_new(:method, :post)
        |> Keyword.update(:headers, [], &Headers.drop_reserved(&1, reserved))

      Tesla.client(
        [
          Tesla.Middleware.Telemetry,
          Tesla.Middleware.JSON,
          if(opts[:gzip], do: {Tesla.Middleware.CompressRequest, format: "gzip"}),
          SSRFProtection,
          EgressTracer
        ]
        |> Enum.filter(& &1),
        adaptor
      )
      |> request(opts)
    end

    # Header names the client's own middleware will set for this request, so they
    # must be dropped from user-supplied headers to avoid duplicates (see
    # `Headers.drop_reserved/2`). `content-type` is only owned when the JSON
    # middleware will actually encode the body — for binary payloads (e.g. NDJSON)
    # it is skipped, and a custom content-type must survive. `content-encoding` is
    # owned whenever gzip compression is enabled.
    @spec reserved_header_names(keyword()) :: [String.t()]
    defp reserved_header_names(opts) do
      content_type = if json_encodable?(opts[:body]), do: ["content-type"], else: []
      content_encoding = if opts[:gzip], do: ["content-encoding"], else: []
      content_type ++ content_encoding
    end

    # Mirrors Tesla.Middleware.JSON's encodability check: only non-binary,
    # non-multipart bodies get JSON-encoded (and thus a content-type header) set.
    @spec json_encodable?(term()) :: boolean()
    defp json_encodable?(nil), do: false
    defp json_encodable?(body) when is_binary(body), do: false
    defp json_encodable?(%Tesla.Multipart{}), do: false
    defp json_encodable?(_), do: true
  end

  # Broadway Pipeline
  defmodule Pipeline do
    @moduledoc false
    use Broadway
    alias Broadway.Message
    alias Logflare.Backends.BufferProducer
    alias Logflare.Backends.Adaptor.WebhookAdaptor.Client

    def start_link(args) do
      Broadway.start_link(__MODULE__,
        name: Backends.via_source(args.source, __MODULE__, args.backend),
        hibernate_after: 5_000,
        spawn_opt: [
          fullsweep_after: 10
        ],
        producer: [
          module:
            {BufferProducer,
             [
               backend_id: Map.get(args.backend || %{}, :id),
               source_id: args.source.id
             ]},
          transformer: {__MODULE__, :transform, []},
          concurrency: 1
        ],
        processors: [
          default: [concurrency: 3, min_demand: 1]
        ],
        batchers: [
          http: [concurrency: 6, batch_size: 250]
        ],
        context: %{
          startup_config: args.config,
          source_id: args.source.id,
          backend_id: Map.get(args.backend || %{}, :id),
          source_token: args.source.token,
          backend_token: Map.get(args.backend || %{}, :token),
          user_id: args.source.user_id
        }
      )
    end

    # see the implementation for Backends.via_source/2 for how tuples are used to identify child processes
    def process_name({:via, module, {registry, identifier}}, base_name) do
      new_identifier = Utils.append_to_tuple(identifier, base_name)
      {:via, module, {registry, new_identifier}}
    end

    def handle_message(_processor_name, message, _context) do
      message
      |> Message.put_batcher(:http)
    end

    def handle_batch(:http, messages, batch_info, context) do
      :telemetry.execute(
        [:logflare, :backends, :pipeline, :handle_batch],
        %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
        %{
          backend_type: :webhook
        }
      )

      %{metadata: backend_metadata} = backend = Backends.Cache.get_backend(context.backend_id)
      config = Backends.Adaptor.get_backend_config(backend)

      # convert this to a custom format if needed
      payload =
        if format_batch = Map.get(config, :format_batch) do
          events = for %{data: le} <- messages, do: le
          format_batch.(events)
        else
          for %{data: le} <- messages, do: le.body
        end

      process_data(payload, config, backend_metadata, context)
      messages
    end

    defp process_data(payload, config, backend_metadata, context) do
      backend_meta =
        for {k, v} <- backend_metadata || %{}, into: %{} do
          {"backend.#{k}", v}
        end

      Client.send(
        # if a `url_override` key is available in the merged config, use that before falling back to `url`
        url: Map.get(config, :url_override, config.url),
        pool_override: Map.get(config, :pool_override),
        body: payload,
        headers: config[:headers] || %{},
        gzip: Map.get(config, :gzip, true),
        opts: [
          # metadata map will get set as OTEL attributes in EgressTracer
          metadata:
            %{
              "source_id" => context[:source_id],
              "source_uuid" => context[:source_token],
              "backend_id" => context[:backend_id],
              "backend_uuid" => context[:backend_token],
              "user_id" => context[:user_id]
            }
            |> Map.merge(backend_meta)
        ],
        http: config[:http]
      )
    end

    # Broadway transformer for custom producer
    def transform(event, _opts) do
      %Message{
        data: event,
        acknowledger: {__MODULE__, :ack_id, :ack_data}
      }
    end

    @doc """
    Merges configs and handles headers.

    Keys in maps in the second argument always overwrite the
    keys in maps in the first argument, even when nested.

    ## Examples

      iex> Logflare.Backends.Adaptor.WebhookAdaptor.Pipeline.merge_configs(%{}, %{})
      %{}

      iex> Logflare.Backends.Adaptor.WebhookAdaptor.Pipeline.merge_configs(%{headers: %{"one" => "one-value"}}, %{headers: %{"one" => "two-value"}})
      %{headers: %{"one" => "two-value"}}

      iex> Logflare.Backends.Adaptor.WebhookAdaptor.Pipeline.merge_configs(%{"username" => "me", "password" => "god"}, %{headers: %{"one" => "two-value"}})
      %{"username" => "me", "password" => "god", headers: %{"one" => "two-value"}}

      iex> Logflare.Backends.Adaptor.WebhookAdaptor.Pipeline.merge_configs(%{username: "me", password: "god"}, %{headers: %{"one" => "two-value"}})
      %{username: "me", password: "god", headers: %{"one" => "two-value"}}

      iex> Logflare.Backends.Adaptor.WebhookAdaptor.Pipeline.merge_configs(%{username: "me", password: "god", headers: %{"one" => "one-value"}}, %{headers: %{"one" => "two-value"}})
      %{username: "me", password: "god", headers: %{"one" => "two-value"}}

      iex> Logflare.Backends.Adaptor.WebhookAdaptor.Pipeline.merge_configs(%{username: "me", password: "god", headers: %{"one" => "one-value"}}, %{headers: %{}})
      %{username: "me", password: "god", headers: %{"one" => "one-value"}}

      iex> Logflare.Backends.Adaptor.WebhookAdaptor.Pipeline.merge_configs(%{"username" => "me", "password" => "god"}, %{"username" => "me", "password" => "another-god"})
      %{"username" => "me", "password" => "another-god"}

      iex> Logflare.Backends.Adaptor.WebhookAdaptor.Pipeline.merge_configs(%{"username" => "me", "password" => "god"}, %{"username" => "me", "password" => "another-god"})
      %{"username" => "me", "password" => "another-god"}

    """
    def merge_configs(config_1, config_2) when is_map(config_1) and is_map(config_2) do
      Map.merge(config_1, config_2, fn
        :headers, v1, v2 ->
          Map.merge(v1, v2)

        _key, _v1, v2 ->
          v2
      end)
    end

    def ack(_ack_ref, _successful, _failed) do
      # TODO: re-queue failed
    end
  end
end
