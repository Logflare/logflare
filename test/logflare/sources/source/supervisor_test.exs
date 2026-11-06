defmodule Logflare.Sources.Source.SupervisorTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.SingleTenant
  alias Logflare.Sources.Cache
  alias Logflare.Sources.Source.Supervisor

  test "runs the system backend source hook when source startup times out" do
    source = build(:source)

    stub(SingleTenant, :single_tenant?, fn -> false end)

    expect(Cache, :get_by, fn [token: token] ->
      assert token == source.token
      source
    end)

    expect(Backends, :start_source_sup, fn ^source -> {:error, :start_timeout} end)
    expect(BigQueryAdaptor, :on_source_start, fn ^source -> :ok end)

    assert {:noreply, nil} = Supervisor.handle_cast({:create, source.token}, nil)
  end
end
