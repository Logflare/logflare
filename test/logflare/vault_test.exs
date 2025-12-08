defmodule Logflare.VaultTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Repo

  setup do

    on_exit(fn ->
    Ecto.Adapters.SQL.Sandbox.unboxed_run(Logflare.Repo, fn ->
      for b <- Logflare.Repo.all(Logflare.Backends.Backend) do
          Logflare.Repo.delete(b)
        end
      end)
    end)
  end
  describe "migrator with retired" do
    setup do
      insert(:backend, config_encrypted: %{some_value: "testing"})
      {:ok, prev_config} = Logflare.Vault.get_config()

      new_config =
        Keyword.put(prev_config, :ciphers,
          default: Logflare.Vault.get_cipher("S757rfGBA90+qpmcJ/WaDt4cBEyZVYVnYKyG4tTH5PQ="),
          retired: prev_config[:ciphers][:default],
          fallback: prev_config[:ciphers][:fallback]
        )

      assert new_config[:ciphers][:default] != new_config[:ciphers][:retired]

      Logflare.Vault.save_config(new_config)

      on_exit(fn ->
        Logflare.Vault.save_config(prev_config)
      end)

      {:ok, ciphers: new_config[:ciphers]}
    end

    test "do_migrate will migrate data using new cipher", %{ciphers: ciphers} do
      assert ciphers[:default] != ciphers[:retired]
      initial = get_config_encrypted()
      assert :ok = Logflare.Vault.maybe_migrate(ciphers)
      migrated = get_config_encrypted()
      assert initial !== migrated

      decoded_initial = Logflare.Vault.decrypt!(initial) |> Jason.decode!()
      decoded_migrated = Logflare.Vault.decrypt!(migrated) |> Jason.decode!()
      assert decoded_initial == decoded_migrated
      assert is_binary(migrated)
    end
  end

  describe "migrator with new default key and a fallback, and no retired" do
    setup do
      # insert using fallback
      insert(:backend, config_encrypted: %{some_value: "testing"})
      {:ok, prev_config} = Logflare.Vault.get_config()

      new_config =
        Keyword.put(prev_config, :ciphers,
          # use a different cipher
          default: Logflare.Vault.get_cipher("S757rfGBA90+qpmcJ/WaDt4cBEyZVYVnYKyG4tTH5PQ="),
          retired: prev_config[:ciphers][:default],
          fallback: prev_config[:ciphers][:fallback]

        )

      Logflare.Vault.save_config(new_config)

      on_exit(fn ->
        Logflare.Vault.save_config(prev_config)
      end)

      {:ok, ciphers: new_config[:ciphers]}
    end

    test "do_migrate will migrate data encrypted with fallback to use new default cipher", %{ciphers: ciphers} do
      initial = get_config_encrypted()
      assert :ok = Logflare.Vault.maybe_migrate(ciphers)
      migrated = get_config_encrypted()
      assert initial !== migrated

      decoded_initial = Logflare.Vault.decrypt!(initial) |> Jason.decode!()
      decoded_migrated = Logflare.Vault.decrypt!(migrated) |> Jason.decode!()
      assert decoded_initial === decoded_migrated
      assert is_binary(migrated)
    end
  end


  describe "don't perform migration if already migrated" do
    setup do
      # insert using fallback
      {:ok, prev_config} = Logflare.Vault.get_config()

      new_config =
        Keyword.put(prev_config, :ciphers,
          # use a different cipher
          default: Logflare.Vault.get_cipher("TFTkxD8gkdR6JutRJAxH348RjNpXztsH90A1aCf06tc="),
          fallback: prev_config[:ciphers][:fallback],
          retired: prev_config[:ciphers][:default]
        )

      Logflare.Vault.save_config(new_config)

      on_exit(fn ->
        Logflare.Vault.save_config(prev_config)

      end)

      {:ok, ciphers: new_config[:ciphers]}
    end

    test "will not perform any migration", %{ciphers: ciphers} do

      Ecto.Adapters.SQL.Sandbox.unboxed_run(Logflare.Repo, fn ->
        insert(:backend, config_encrypted: %{some_value: "testing"})
      end)

      initial = get_config_encrypted() |> dbg()

      Ecto.Adapters.SQL.Sandbox.unboxed_run(Logflare.Repo, fn ->
        assert :ok = Logflare.Vault.maybe_migrate(ciphers)
      end)

      :timer.sleep(1000)
      post_migrate = get_config_encrypted() |> dbg
      assert post_migrate !== initial
      assert :noop = Logflare.Vault.maybe_migrate(ciphers)

      decoded_initial = Logflare.Vault.decrypt!(initial) |> Jason.decode!()
      decoded_post_migrate = Logflare.Vault.decrypt!(post_migrate) |> Jason.decode!()
      assert decoded_initial == decoded_post_migrate
    end
  end

  defp get_config_encrypted do
    [
      %{
        config: nil,
        config_encrypted: encrypted_str
      }
    ] = Repo.all(from b in "backends", select: [:config, :config_encrypted])

    encrypted_str
  end
end
