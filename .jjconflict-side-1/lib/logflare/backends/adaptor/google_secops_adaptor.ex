defmodule Logflare.Backends.Adaptor.GoogleSecOpsAdaptor do
  @moduledoc """
  Adaptor sending logs to a [Google SecOps](https://cloud.google.com/chronicle/docs) SIEM webhook feed.
  This adaptor is **ingest-only**.

  Events are POSTed newline-delimited to the feed's `:importPushLogs` endpoint, so the
  feed must be created with the split delimiter set to `\\n`.

  ## Configuration
  - `:region` - Region of the SecOps instance, determining the endpoint address
  - `:project_number`, `:instance_id`, `:feed_id` - Feed identifiers, readable from the
    endpoint URL in the feed's Endpoint Information
  - `:api_key` - Google Cloud API key restricted to the Chronicle API
  - `:secret` - Secret key generated for the feed
  """

  alias Ecto.Changeset
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Backend

  @behaviour Adaptor
  @behaviour HttpBased.Client

  # A single DNS label; anything that could break out of the
  # `https://<region>-chronicle.googleapis.com` host (`/`, `?`, `#`, `@`, `.`, `:`)
  # is rejected, keeping the request pinned under googleapis.com (SSRF guard).
  # A non-existent region surfaces as a DNS error via test_connection/1.
  @region_format ~r/^[a-z0-9-]+$/

  @uuid_format ~r/^[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$/

  def child_spec(init_arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [init_arg]}
    }
  end

  @impl Adaptor
  def start_link({source, backend}) do
    HttpBased.Pipeline.start_link(source, backend, __MODULE__)
  end

  @impl Adaptor
  def cast_config(params, existing_config \\ %{}) do
    types = %{
      region: :string,
      project_number: :string,
      instance_id: :string,
      feed_id: :string,
      api_key: :string,
      secret: :string
    }

    {existing_config, types}
    |> Changeset.cast(params, Map.keys(types))
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Changeset.validate_required([
      :region,
      :project_number,
      :instance_id,
      :feed_id,
      :api_key,
      :secret
    ])
    |> Changeset.validate_format(:region, @region_format)
    |> Changeset.validate_format(:project_number, ~r/^\d+$/)
    |> Changeset.validate_format(:instance_id, @uuid_format)
    |> Changeset.validate_format(:feed_id, @uuid_format)
  end

  @impl Adaptor
  def redact_config(config) do
    config
    |> Map.put(:api_key, "REDACTED")
    |> Map.put(:secret, "REDACTED")
  end

  @impl Adaptor
  def test_connection(%Backend{} = backend) do
    # TODO: Verify if empty payload is accepted and returns 200
    case HttpBased.Client.send_events(__MODULE__, [], backend) do
      {:ok, %Tesla.Env{status: 200}} -> :ok
      {:ok, env} -> {:error, "Unexpected response: #{env.status} #{inspect(env.body)}"}
      {:error, reason} -> {:error, "Request error: #{inspect(reason)}"}
    end
  end

  @impl HttpBased.Client
  def client_opts(%Backend{config: config} = backend) do
    [
      url: endpoint_url(config),
      formatter: HttpBased.NdjsonFormatter,
      formatter_opts: [metadata: [backend_id: backend.id]],
      json: false,
      headers: %{
        "x-goog-api-key" => config.api_key,
        "x-webhook-access-key" => config.secret
      }
    ]
  end

  defp endpoint_url(config) do
    Enum.join(
      [
        "https://#{config.region}-chronicle.googleapis.com/v1",
        "projects",
        config.project_number,
        "locations",
        config.region,
        "instances",
        config.instance_id,
        "feeds",
        "#{config.feed_id}:importPushLogs"
      ],
      "/"
    )
  end
end
