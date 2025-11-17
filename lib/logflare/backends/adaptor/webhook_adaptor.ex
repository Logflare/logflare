defmodule Logflare.Backends.Adaptor.WebhookAdaptor do
  @moduledoc """
  Backend adaptor for webhooks / HTTP posts.

  A number of other adaptors (_ClickHouse, DataDog, Elastic, Loki, etc_) leverage this to handle the final HTTP transaction.

  ### Finch Pool Selection

  By default the pool will be selected automatically based on the `:http` configuration option.

  If you want to manually select a specific Finch pool, you can use the `:pool_name` option and provide the module name.


  ### Dynamic URL handling with URL Override

  This adaptor performs a merge on config that will prevent you from leveraging a dynamically generated URL configuration at runtime.
  To bypass this behavior, you can use the optional `:url_override` attribute.

  See the `Logflare.Backends.Adaptor.ClickhouseWebhookAdaptor` for an example that utilizes this.
  """

  use GenServer

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.WebhookAdaptor.EgressMiddleware
  alias Logflare.Utils

  @behaviour Logflare.Backends.Adaptor

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
  def cast_config(params) do
    {%{}, %{url: :string, headers: :map, http: :string, gzip: :boolean}}
    |> Ecto.Changeset.cast(params, [:url, :headers, :http, :gzip])
    |> Logflare.Utils.default_field_value(:http, "http2")
    |> Logflare.Utils.default_field_value(:gzip, true)
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:url])
    |> Ecto.Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
    |> Ecto.Changeset.validate_inclusion(:http, ["http1", "http2"])
  end

  @impl Logflare.Backends.Adaptor
  def redact_config(config) do
    config
    |> Map.update(:headers, %{}, fn headers ->
      for {key, value} <- headers, into: %{} do
        if String.downcase(key) == "authorization" do
          {key, "REDACTED"}
        else
          {key, value}
        end
      end
    end)
  end

  # HTTP Client
  defmodule Client do
    @moduledoc false
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

      opts =
        opts
        |> Keyword.put_new(:method, :post)
        |> Keyword.update(:headers, [], &Map.to_list/1)

      Tesla.client(
        [
          Tesla.Middleware.Telemetry,
          Tesla.Middleware.JSON,
          if(opts[:gzip], do: {Tesla.Middleware.CompressRequest, format: "gzip"}),
          {EgressMiddleware, metadata: opts[:metadata]}
        ]
        |> Enum.filter(& &1),
        adaptor
      )
      |> request(opts)
    end
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
          backend_token: Map.get(args.backend || %{}, :token)
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
        # metadata map will get set as OTEL attributes in EgressMiddleware
        metadata:
          %{
            "source_id" => context[:source_id],
            "source_uuid" => context[:source_token],
            "backend_id" => context[:backend_id],
            "backend_uuid" => context[:backend_token]
          }
          |> Map.merge(backend_meta),
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
