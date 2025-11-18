defmodule Logflare.Backends.Adaptor.AxiomAdaptor do
  @moduledoc """
  An **ingest-only** adaptor sending logs to Axiom

  This adaptor wraps the WebhookAdaptor to provide specific functionality
  for sending logs to Axiom dataset using their REST API

  ## Configuration

  - `:domain` - The domain specific to the region your account is using,
    Defaults to the US domain (`api.axiom.co`)
  - `:api_token` - Bearer token used for authorization
  - `:dataset_name` - Name of the dataset used for log ingest
  """

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Backend

  @behaviour Adaptor

  def child_spec(init_arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [init_arg]}
    }
  end

  @impl Adaptor
  def start_link({source, backend}) do
    HttpBased.Pipeline.start_link(source, backend, __MODULE__.Client)
  end

  @impl Adaptor
  def cast_config(params) do
    defaults = %{
      domain: "api.axiom.co"
    }

    types = %{
      domain: :string,
      api_token: :string,
      dataset_name: :string
    }

    {%{}, types}
    |> Ecto.Changeset.change(defaults)
    |> Ecto.Changeset.cast(params, Map.keys(types))
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:domain, :api_token, :dataset_name])
  end

  @impl Adaptor
  def redact_config(config) do
    if Map.get(config, :api_token) do
      Map.put(config, :api_token, "REDACTED")
    else
      config
    end
  end

  @impl Adaptor
  def test_connection({_source, backend}) do
    test_connection(backend)
  end

  def test_connection(%Backend{config: config}) do
    __MODULE__.Client.test_connection(config)
  end

  @doc false
  @spec transform_timestamp(map()) :: map()
  def transform_timestamp(log_event_body) do
    Map.update!(log_event_body, "timestamp", fn timestamp ->
      timestamp
      |> DateTime.from_unix!(:microsecond)
      |> DateTime.to_iso8601()
    end)
  end

  defmodule Client do
    @moduledoc false
    alias Logflare.Backends.Adaptor.AxiomAdaptor
    alias Logflare.Backends.Adaptor.HttpBased

    @behaviour HttpBased.Client

    @impl HttpBased.Client
    def send_logs(config, log_events, metadata) do
      # TODO: Log failures
      config
      |> new()
      |> Tesla.post("", log_events, opts: [metadata: metadata])

      :ok
    end

    @impl HttpBased.Client
    def test_connection(config) do
      case config |> new() |> Tesla.post("", []) do
        {:ok, %Tesla.Env{status: 200}} -> :ok
        {:ok, %Tesla.Env{body: %{"message" => message}}} -> {:error, message}
        {:ok, env} -> {:error, "Unexpected response: #{env.status} #{inspect(env.body)}"}
        {:error, reason} -> {:error, "Request error: #{reason}"}
      end
    end

    defp new(config) do
      url =
        %URI{
          scheme: "https",
          host: config.domain,
          path: "/v1/datasets/#{config.dataset_name}/ingest"
        }
        |> URI.to_string()

      query = [
        {"timestamp-field", "timestamp"},
        {"timestamp-format", "2006-01-02T15:04:05.999999Z07:00"}
      ]

      HttpBased.Client.new(
        formatter: {HttpBased.LogEventTransformer, map: &AxiomAdaptor.transform_timestamp/1},
        gzip: true,
        url: url,
        query: query,
        token: config.api_token
      )
    end
  end
end
