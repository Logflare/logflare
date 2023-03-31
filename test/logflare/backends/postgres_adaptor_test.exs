defmodule Logflare.Backends.PostgresAdaptorTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.{LogEvent, Backends.Adaptor.PostgresAdaptor}
  setup :set_mimic_global

  @valid_config %{
    db_host: "localhost",
    db_port: 5432,
    db_database: "logflare_test",
    db_password: "postgres",
    db_username: "postgres"
  }
  setup do
    source_backend = insert(:source_backend, type: :postgres, config: @valid_config)

    pid = start_supervised!({PostgresAdaptor, source_backend})
    {:ok, pid: pid}
  end

  test "ingest/2", %{pid: pid} do
    PostgresAdaptor.Repo
    |> expect(:insert_all, fn events, _opts -> {:ok, length(events)} end)

    assert :ok = PostgresAdaptor.ingest(pid, [%LogEvent{body: %{"event_message"=> "something"}}])
    :timer.sleep(1_500)
  end

  test "cast_and_validate_config/1" do
    for valid <- [
          @valid_config
        ] do
      assert %Ecto.Changeset{valid?: true} = PostgresAdaptor.cast_and_validate_config(valid),
             "valid: #{inspect(valid)}"
    end

    for invalid <- [
          %{},
          %{@valid_config | db_host: nil},
          %{@valid_config | db_port: nil},
          %{@valid_config | db_database: nil},
          %{@valid_config | db_password: nil},
          %{@valid_config | db_username: nil},
        ] do
      assert %Ecto.Changeset{valid?: false} = PostgresAdaptor.cast_and_validate_config(invalid),
             "invalid: #{inspect(invalid)}"
    end
  end
end
