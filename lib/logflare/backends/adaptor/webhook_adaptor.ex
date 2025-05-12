defmodule Logflare.Backends.Adaptor.WebhookAdaptor do
  @moduledoc false
  use GenServer
  use TypedStruct

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.WebhookAdaptor.EgressMiddleware

  @behaviour Logflare.Backends.Adaptor

  typedstruct do
    field(:config, %{
      url: String.t(),
      headers: map(),
      http: String.t(),
      gzip: boolean()
    })
  end

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
  def execute_query(_ident, _query),
    do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:url])
    |> Ecto.Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
    |> Ecto.Changeset.validate_inclusion(:http, ["http1", "http2"])
  end

  # HTTP Client
  defmodule Client do
    @moduledoc false
    use Tesla, docs: false

    def send(opts) do
      adaptor =
        if Keyword.get(opts, :http) == "http1" do
          {Tesla.Adapter.Finch, name: Logflare.FinchDefaultHttp1, receive_timeout: 5_000}
        else
          {Tesla.Adapter.Finch, name: Logflare.FinchDefault, receive_timeout: 5_000}
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
          fullsweep_after: 100
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
      new_identifier = Tuple.append(identifier, base_name)
      {:via, module, {registry, new_identifier}}
    end

    def handle_message(_processor_name, message, _context) do
      message
      |> Message.put_batcher(:http)
    end

    def handle_batch(:http, messages, _batch_info, %{startup_config: startup_config} = context) do
      # convert this to a custom format if needed
      payload =
        if format_batch = Map.get(startup_config, :format_batch) do
          events = for %{data: le} <- messages, do: le
          format_batch.(events)
        else
          for %{data: le} <- messages, do: le.body
        end

      process_data(payload, context)
      messages
    end

    defp process_data(payload, %{startup_config: startup_config} = context) do
      %{config: stored_config, metadata: backend_metadata} =
        Backends.Cache.get_backend(context.backend_id)

      config =
        merge_configs(startup_config, stored_config)

      backend_meta =
        for {k, v} <- backend_metadata || %{testing: 123}, into: %{} do
          {"backend.#{k}", v}
        end

      Client.send(
        # if a `url_override` key is available in the merged config, use that before falling back to `url`
        url: Map.get(config, :url_override, config.url),
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
