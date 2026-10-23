defmodule Logflare.Backends.Adaptor.BigQueryV2Adaptor.Provisioner do
  @moduledoc """
  One-shot task that runs when a `BigQueryV2Adaptor` starts. Verifies access to the
  configured dataset, then creates any missing tables.
  """

  use Task, restart: :transient

  require Logger

  alias Logflare.Backends.Adaptor.BigQueryV2Adaptor
  alias Logflare.Backends.Backend

  @doc false
  @spec start_link(Backend.t()) :: {:ok, pid()}
  def start_link(%Backend{} = backend) do
    Task.start_link(__MODULE__, :run, [backend])
  end

  @doc false
  @spec run(Backend.t()) :: :ok
  def run(%Backend{} = backend) do
    with {:test_connection, :ok} <- {:test_connection, BigQueryV2Adaptor.test_connection(backend)},
         {:provision_tables, :ok} <-
           {:provision_tables, BigQueryV2Adaptor.provision_tables(backend)} do
      :ok
    else
      {step, {:error, reason} = error} ->
        Logger.warning("BigQuery v2 provisioning failed",
          backend_id: backend.id,
          step: step,
          error_string: inspect(reason)
        )

        exit({:shutdown, error})
    end
  end
end
