defmodule Logflare.Backends.Adaptor.OtlpAdaptor.Common do
  alias Logflare.Backends.Backend
  alias Logflare.Backends.Adaptor.HttpBased

  @doc """
  A code for testing connection meant to be shared by all OTLP based adaptors.

  Sends an empty list of events, expecting success response.
  """
  @spec test_connection(module(), Backend.t()) ::
          :ok | {:error, term()}
  def test_connection(client_module, %Backend{} = backend) do
    case HttpBased.Client.send_events(client_module, [], backend) do
      {:ok, %Tesla.Env{status: 200, body: %{partial_success: nil}}} -> :ok
      {:ok, %Tesla.Env{status: 200, body: %{partial_success: %{error_message: ""}}}} -> :ok
      {:ok, env} -> {:error, env}
      {:error, _reason} = err -> err
    end
  end
end
