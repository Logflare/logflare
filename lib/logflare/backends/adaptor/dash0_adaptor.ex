defmodule Logflare.Backends.Adaptor.Dash0Adaptor do
  @moduledoc """
  Adaptor sending logs to [Dash0](https://www.dash0.com) via OTLP/HTTP
  This adaptor is **ingest-only**

  ## Configuration
  - `:endpoint` - OTLP/HTTP endpoint of the Dash0 organization, shown under **Settings > Endpoints**
    in Dash0, for example `https://ingress.eu-west-1.aws.dash0.com`. The `/v1/logs` path is
    appended automatically.
  - `:auth_token` - Dash0 auth token with telemetry ingestion permission, created under
    **Settings > Auth Tokens**. Sent as a bearer token.
  - `:dataset` - Optional Dash0 dataset to write to. When unset, Dash0 uses the auth token's
    default ingestion dataset.
  """

  @behaviour Logflare.Backends.Adaptor
  @behaviour Logflare.Backends.Adaptor.HttpBased.Client

  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1]

  alias Ecto.Changeset
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.OtlpAdaptor
  alias Logflare.Backends.Backend

  @dataset_header "dash0-dataset"
  @logs_path "/v1/logs"

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
      endpoint: :string,
      auth_token: :string,
      dataset: :string
    }

    {existing_config, types}
    |> Changeset.cast(params, Map.keys(types))
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Changeset.validate_required([:endpoint, :auth_token])
    |> Changeset.validate_format(:endpoint, ~r/https\:\/\/.+/)
    |> Changeset.validate_change(:endpoint, &logs_path_validator/2)
  end

  @spec logs_path_validator(atom(), String.t()) :: [{atom(), String.t()}]
  defp logs_path_validator(field, endpoint) do
    if String.ends_with?(String.trim_trailing(endpoint, "/"), @logs_path) do
      [{field, "must not include the #{@logs_path} path, which is appended automatically"}]
    else
      []
    end
  end

  @impl Adaptor
  def redact_config(config) do
    Map.put(config, :auth_token, "REDACTED")
  end

  @impl Adaptor
  def sanitize_config_for_display(config) do
    Adaptor.mask_config_values(config, except: [:endpoint, :dataset])
  end

  @impl Adaptor
  def test_connection(args) do
    OtlpAdaptor.Common.test_connection(__MODULE__, args)
  end

  @impl HttpBased.Client
  def client_opts(%Backend{config: config}) do
    OtlpAdaptor.Common.flattened_logs_client_opts(config.endpoint, headers(config))
  end

  @spec headers(map()) :: %{String.t() => String.t()}
  defp headers(%{auth_token: token} = config) do
    %{"authorization" => "Bearer " <> token}
    |> maybe_put_dataset(config)
  end

  @spec maybe_put_dataset(map(), map()) :: map()
  defp maybe_put_dataset(headers, %{dataset: dataset}) when is_non_empty_binary(dataset),
    do: Map.put(headers, @dataset_header, dataset)

  defp maybe_put_dataset(headers, _config), do: headers
end
