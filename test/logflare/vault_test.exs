defmodule Logflare.BackendsTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Repo

  describe "migrator" do
    setup do
      insert(:backend, config_encrypted: %{some_value: "testing"})
      {:ok, prev_config} = Logflare.Vault.get_config()

      new_config =
        Keyword.put(prev_config, :ciphers,
          default: Logflare.Vault.get_cipher("S757rfGBA90+qpmcJ/WaDt4cBEyZVYVnYKyG4tTH5PQ="),
          retired: prev_config[:ciphers][:default]
        )

      Logflare.Vault.save_config(new_config)

      on_exit(fn ->
        Logflare.Vault.save_config(prev_config)
      end)

      :ok
    end

    test "do_migrate will migrate data using new cipher" do
      initial = get_config_encrypted()
      Logflare.Vault.do_migrate()
      migrated = get_config_encrypted()
      assert initial != migrated

      decoded_initial = Logflare.Vault.decrypt!(initial) |> Jason.decode!()
      decoded_migrated = Logflare.Vault.decrypt!(migrated) |> Jason.decode!()
      assert decoded_initial == decoded_migrated
      assert is_binary(migrated)
    end
  end

  defp get_config_encrypted() do
    [
      %{
        config: nil,
        config_encrypted: encrypted_str
      }
    ] = Repo.all(from b in "backends", select: [:config, :config_encrypted])

    encrypted_str
  end
end
