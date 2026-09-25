defmodule Logflare.RepoSupervisionTest do
  use ExUnit.Case, async: false

  @repo_sup Logflare.Repo.Supervisor
  @pool_name :fake_repo_pool

  # `:sys.get_state/1` on a supervisor returns OTP's private `#state{}` record;
  # positions 5 and 6 are `intensity` and `period`. There is no public API.
  defp restart_intensity(sup) do
    state = :sys.get_state(sup)
    {elem(state, 5), elem(state, 6)}
  end

  defp child_ids(sup) do
    sup
    |> Supervisor.which_children()
    |> Enum.map(fn {id, _pid, _type, _mods} -> id end)
  end

  describe "supervision topology" do
    test "Logflare.Repo is not a direct child of the application supervisor" do
      refute Logflare.Repo in child_ids(Logflare.Supervisor),
             "a Repo crash escalates straight to Logflare.Supervisor"
    end

    test "Logflare.Repo is supervised by a dedicated supervisor" do
      assert @repo_sup in child_ids(Logflare.Supervisor)
      assert Logflare.Repo in child_ids(@repo_sup)
    end

    test "the repo supervisor tolerates far more restarts than the app supervisor" do
      {app_intensity, _period} = restart_intensity(Logflare.Supervisor)
      {repo_intensity, _period} = restart_intensity(Process.whereis(@repo_sup))

      assert repo_intensity > app_intensity
      assert repo_intensity >= 10
    end

    test "the repo is still started and reachable" do
      assert is_pid(Process.whereis(Logflare.Repo))
    end
  end

  describe "restart escalation mechanism" do
    setup do
      Process.flag(:trap_exit, true)
      :ok
    end

    # Mimics `Ecto.Repo.Supervisor.init/1`, which supervises the single
    # DBConnection pool child with `max_restarts: 0` - any pool exit takes the
    # whole repo supervisor down and escalates to whoever supervises the repo.
    defp repo_like_spec do
      pool_spec = %{
        id: :pool,
        start: {Agent, :start_link, [fn -> :ok end, [name: @pool_name]]}
      }

      %{
        id: :repo_like,
        start:
          {Supervisor, :start_link, [[pool_spec], [strategy: :one_for_one, max_restarts: 0]]},
        type: :supervisor
      }
    end

    defp kill_pool! do
      pid = await_pool!()
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 1_000
    end

    defp await_pool!(remaining \\ 200)

    defp await_pool!(0), do: flunk("pool #{@pool_name} never came back up")

    defp await_pool!(remaining) do
      case Process.whereis(@pool_name) do
        nil ->
          Process.sleep(5)
          await_pool!(remaining - 1)

        pid ->
          pid
      end
    end

    test "the 4th repo crash within 5s exceeds default intensity and kills the parent" do
      {:ok, app_sup} = Supervisor.start_link([repo_like_spec()], strategy: :one_for_one)

      # default intensity is max_restarts: 3 within max_seconds: 5
      for _ <- 1..3, do: kill_pool!()
      assert Process.alive?(app_sup)

      kill_pool!()

      assert_receive {:EXIT, ^app_sup, :shutdown}, 1_000
      refute Process.alive?(app_sup)
    end

    test "an intermediate supervisor absorbs the same crashes" do
      isolation_sup = %{
        id: :repo_isolation,
        start:
          {Supervisor, :start_link,
           [[repo_like_spec()], [strategy: :one_for_one, max_restarts: 100, max_seconds: 1]]},
        type: :supervisor
      }

      {:ok, app_sup} = Supervisor.start_link([isolation_sup], strategy: :one_for_one)

      for _ <- 1..10, do: kill_pool!()

      refute_receive {:EXIT, ^app_sup, _}, 200
      assert Process.alive?(app_sup)
      assert is_pid(await_pool!())

      Supervisor.stop(app_sup)
    end
  end
end
