defmodule Logflare.Backends.SupervisorTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends
  alias Logflare.Test.RegistryShutdownClient

  @isolated_names %{
    Backends.SourceRegistry => __MODULE__.SourceRegistry,
    Backends.BackendRegistry => __MODULE__.BackendRegistry,
    Backends.BufferProducer.InFlightRegistry => __MODULE__.InFlightRegistry,
    Backends.SourcesSup => __MODULE__.SourcesSup
  }

  test "starts the registries before the supervisors of their clients" do
    {:ok, {_flags, specs}} = Backends.Supervisor.init([])
    ids = Enum.map(specs, & &1.id)

    position = fn id ->
      Enum.find_index(ids, &(&1 == id)) || flunk("#{inspect(id)} not started")
    end

    for registry <- [
          Backends.SourceRegistry,
          Backends.BackendRegistry,
          Backends.BufferProducer.InFlightRegistry
        ],
        supervisor <- [
          Backends.ConsolidatedSup,
          Backends.SourcesSup,
          Backends.Adaptor.PostgresAdaptor.Supervisor,
          Backends.Adaptor.ClickHouseAdaptor.QueryConnectionSup
        ] do
      assert position.(registry) < position.(supervisor),
             "#{inspect(registry)} must start before #{inspect(supervisor)}"
    end
  end

  test "registries remain available while clients in both pipeline trees shut down" do
    {:ok, {_flags, specs}} = Backends.Supervisor.init([])

    children =
      specs
      |> Enum.filter(&(Map.has_key?(@isolated_names, &1.id) or &1.id == Backends.ConsolidatedSup))
      |> Enum.map(&isolate_child/1)

    start_supervised!(%{
      id: __MODULE__.Tree,
      start:
        {Supervisor, :start_link, [children, [strategy: :one_for_one, name: __MODULE__.Tree]]},
      type: :supervisor
    })

    clients =
      for {supervisor, registry} <- [
            {__MODULE__.ConsolidatedSup, __MODULE__.BackendRegistry},
            {{:via, PartitionSupervisor, {__MODULE__.SourcesSup, self()}},
             __MODULE__.SourceRegistry}
          ] do
        {:ok, pid} =
          DynamicSupervisor.start_child(
            supervisor,
            {RegistryShutdownClient,
             owner: self(),
             registry: registry,
             key: :client,
             in_flight_registry: __MODULE__.InFlightRegistry}
          )

        {pid, Process.monitor(pid)}
      end

    stop_supervised!(__MODULE__.Tree)

    for {pid, ref} <- clients do
      assert_receive {:terminated, ^pid, :shutdown, [{^pid, nil}], [{^pid, :in_flight}]}
      assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
    end

    refute_receive {:registry_exit, _, _, _}, 0
  end

  defp isolate_child(%{id: Backends.ConsolidatedSup} = spec) do
    children = [
      {DynamicSupervisor, name: __MODULE__.ConsolidatedSup, strategy: :one_for_one}
    ]

    %{
      spec
      | start:
          {Supervisor, :start_link,
           [children, [strategy: :one_for_one, name: __MODULE__.ConsolidatedTree]]}
    }
  end

  defp isolate_child(%{start: {module, :start_link, [opts]}} = spec) do
    opts = Keyword.put(opts, :name, Map.fetch!(@isolated_names, spec.id))
    %{spec | start: {module, :start_link, [opts]}}
  end
end
