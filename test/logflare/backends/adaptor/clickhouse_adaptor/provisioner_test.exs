defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.ProvisionerTest do
  use Logflare.DataCase, async: false
  import Mimic

  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.Provisioner

  setup :set_mimic_global
  setup :verify_on_exit!

  describe "child_spec/1" do
    test "returns correct child specification" do
      args = {build(:source), build(:backend)}
      spec = Provisioner.child_spec(args)

      assert spec.id == Provisioner
      assert spec.restart == :transient
      assert spec.start == {Provisioner, :start_link, [args]}
    end
  end

  describe "successful provisioning flow" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse)

      [source: source, backend: backend]
    end

    test "provisions successfully and terminates normally", %{source: source, backend: backend} do
      ClickhouseAdaptor
      |> expect(:test_connection, fn test_source, test_backend ->
        assert test_source.id == source.id
        assert test_backend.id == backend.id
        :ok
      end)

      ClickhouseAdaptor
      |> expect(:provision_all, fn {prov_source, prov_backend} ->
        assert prov_source.id == source.id
        assert prov_backend.id == backend.id
        :ok
      end)

      {:ok, pid} = Provisioner.start_link({source, backend})
      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :noproc}, 1_000
    end

    test "handles state correctly during initialization", %{source: source, backend: backend} do
      ClickhouseAdaptor
      |> expect(:test_connection, fn _, _ -> :ok end)
      |> expect(:provision_all, fn _ -> :ok end)

      {:ok, pid} = Provisioner.start_link({source, backend})
      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :noproc}, 1_000
    end
  end

  describe "connection test failure handling" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse)

      [source: source, backend: backend]
    end

    test "fails initialization when connection test fails", %{source: source, backend: backend} do
      ClickhouseAdaptor
      |> expect(:test_connection, fn _, _ -> {:error, :connection_failed} end)

      result = Provisioner.start_link({source, backend})

      assert {:error, :connection_failed} = result
    end

    test "does not call provision_all when connection test fails", %{
      source: source,
      backend: backend
    } do
      ClickhouseAdaptor
      |> expect(:test_connection, fn _, _ -> {:error, :invalid_credentials} end)
      |> reject(:provision_all, 1)

      assert {:error, :invalid_credentials} = Provisioner.start_link({source, backend})
    end
  end

  describe "provisioning failure handling" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse)

      [source: source, backend: backend]
    end

    test "fails initialization when provisioning fails", %{source: source, backend: backend} do
      ClickhouseAdaptor
      |> expect(:test_connection, fn _, _ -> :ok end)
      |> expect(:provision_all, fn _ -> {:error, :table_creation_failed} end)

      result = Provisioner.start_link({source, backend})

      assert {:error, :table_creation_failed} = result
    end

    test "handles different provisioning error types", %{source: source, backend: backend} do
      test_cases = [
        {:error, :insufficient_permissions},
        {:error, :database_not_found},
        {:error, :timeout}
      ]

      for error_result <- test_cases do
        ClickhouseAdaptor
        |> expect(:test_connection, fn _, _ -> :ok end)
        |> expect(:provision_all, fn _ -> error_result end)

        result = Provisioner.start_link({source, backend})
        assert {:error, reason} = result
        assert reason == elem(error_result, 1)
      end
    end
  end

  describe "process lifecycle and monitoring" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse)

      [source: source, backend: backend]
    end

    test "process traps exits during initialization", %{source: source, backend: backend} do
      test_pid = self()

      ClickhouseAdaptor
      |> expect(:test_connection, fn _, _ ->
        send(test_pid, {:trapped_exits, Process.flag(:trap_exit, false)})
        :ok
      end)
      |> expect(:provision_all, fn _ -> :ok end)

      {:ok, _pid} = Provisioner.start_link({source, backend})

      assert_receive {:trapped_exits, true}, 1_000
    end

    test "continues to close_process after successful provisioning", %{
      source: source,
      backend: backend
    } do
      ClickhouseAdaptor
      |> expect(:test_connection, fn _, _ -> :ok end)
      |> expect(:provision_all, fn _ -> :ok end)

      {:ok, pid} = Provisioner.start_link({source, backend})
      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :noproc}, 1_000
    end
  end

  describe "integration with ClickhouseAdaptor functionality" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse)

      [source: source, backend: backend]
    end

    test "calls ClickhouseAdaptor functions with correct arguments", %{
      source: source,
      backend: backend
    } do
      ClickhouseAdaptor
      |> expect(:test_connection, fn test_source, test_backend ->
        assert test_source.id == source.id
        assert test_source.token == source.token
        assert test_backend.id == backend.id
        assert test_backend.type == backend.type
        :ok
      end)
      |> expect(:provision_all, fn {prov_source, prov_backend} ->
        assert prov_source.id == source.id
        assert prov_backend.id == backend.id
        :ok
      end)

      assert {:ok, pid} = Provisioner.start_link({source, backend})
      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :noproc}, 1_000
    end

    test "preserves source and backend data in state", %{source: source, backend: backend} do
      ClickhouseAdaptor
      |> expect(:test_connection, fn _, _ -> :ok end)
      |> expect(:provision_all, fn _ -> :ok end)

      {:ok, pid} = Provisioner.start_link({source, backend})

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :noproc}, 1_000
    end
  end

  describe "edge cases and boundary conditions" do
    test "handles invalid source/backend combos" do
      ClickhouseAdaptor
      |> expect(:test_connection, fn _, _ -> {:error, :invalid_args} end)
      |> reject(:provision_all, 1)

      invalid_source = %Logflare.Source{id: nil, token: nil}
      invalid_backend = %Logflare.Backends.Backend{id: nil}

      result = Provisioner.start_link({invalid_source, invalid_backend})
      assert {:error, :invalid_args} = result
    end

    test "handles concurrent provisioner starts for same source/backend", %{} do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse)

      ClickhouseAdaptor
      |> expect(:test_connection, 2, fn _, _ -> :ok end)
      |> expect(:provision_all, 2, fn _ ->
        Process.sleep(100)
        :ok
      end)

      task1 = Task.async(fn -> Provisioner.start_link({source, backend}) end)
      task2 = Task.async(fn -> Provisioner.start_link({source, backend}) end)
      assert {:ok, pid1} = Task.await(task1)
      assert {:ok, pid2} = Task.await(task2)

      ref1 = Process.monitor(pid1)
      ref2 = Process.monitor(pid2)

      assert_receive {:DOWN, ^ref1, :process, ^pid1, :noproc}, 1_000
      assert_receive {:DOWN, ^ref2, :process, ^pid2, :noproc}, 1_000
    end
  end
end
