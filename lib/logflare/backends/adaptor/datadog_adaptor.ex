defmodule Logflare.Backends.Adaptor.DatadogAdaptor do
  @moduledoc """
  Wrapper module for `Logflare.Backends.Adaptor.WebhookAdaptor` to provide API
  for DataDog logs ingestion endpoint.
  """

  use TypedStruct

  alias Logflare.Backends.Adaptor.WebhookAdaptor

  typedstruct enforce: true do
    field(:api_key, String.t())
  end

  @behaviour Logflare.Backends.Adaptor

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    backend = %{
      backend
      | config: %{
          url: "https://http-intake.logs.datadoghq.com/api/v2/logs",
          headers: %{
            "dd-api-key" => backend.config.api_key
          }
        }
    }

    WebhookAdaptor.start_link({source, backend})
  end

  @impl Logflare.Backends.Adaptor
  def ingest(pid, log_events, opts) do
    new_events =
      Enum.map(log_events, &translate_event/1)

    WebhookAdaptor.ingest(pid, new_events, opts)
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_ident, _query), do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{api_key: :string}}
    |> Ecto.Changeset.cast(params, [:api_key])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    import Ecto.Changeset

    changeset
    |> validate_required([:api_key])
  end

  defp translate_event(%Logflare.LogEvent{} = le) do
    %Logflare.LogEvent{
      le
      | body: %{
          message: Jason.encode!(le.body),
          ddsource: "logflare",
          service: le.source.name
        }
    }
  end
end
