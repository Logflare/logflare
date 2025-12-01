defmodule Logflare.Backends.Adaptor.TCPAdaptorTest do
  use Logflare.DataCase, async: false
  @moduletag :telegraf

  setup do
    start_supervised!(Logflare.SystemMetrics.AllLogsLogged)
    :ok
  end

  setup do
    insert(:plan)
    :ok
  end

  setup do
    telegraf_output_path = "db/telegraf_output/metrics.out"

    if File.exists?(telegraf_output_path) do
      File.write!(telegraf_output_path, _empty = "")
    end

    _pid = spawn_link_telegraf_watcher(telegraf_output_path)
    :ok
  end

  describe "telegraf + tcp_adapter with basic config" do
    setup do
      config = %{host: "localhost", port: 6514}
      source = insert(:source, user: build(:user))
      backend = insert(:backend, type: :tcp, sources: [source], config: config)
      start_supervised!({Logflare.Backends.AdaptorSupervisor, {source, backend}})
      {:ok, source: source}
    end

    test "sends RFC5424 message with octet counting", %{source: source} do
      body = %{"message" => "hello world", "level" => "info"}
      %{id: log_event_id} = log_event = build(:log_event, source: source, body: body)
      assert {:ok, 1} = Logflare.Backends.ingest_logs([log_event], source)

      assert_receive {:telegraf, telegraf_event}, to_timeout(second: 5)

      assert %{
               "fields" => %{
                 "facility_code" => 16,
                 "message" => telegraf_event_message,
                 "msgid" => msgid,
                 "severity_code" => 6,
                 "timestamp" => _timestamp,
                 "version" => 1
               },
               "name" => "syslog",
               "tags" => %{
                 "appname" => "logflare",
                 "facility" => "local0",
                 "severity" => "info"
               }
             } = telegraf_event

      assert Base.decode32!(msgid, padding: false) == Ecto.UUID.dump!(log_event_id)

      assert %{
               "body" => %{"level" => "info", "message" => "hello world"},
               "event_message" => "test-msg",
               "id" => _id,
               "timestamp" => _timestamp
             } = Jason.decode!(telegraf_event_message)

      refute_received _anything_else
    end
  end

  describe "mTLS" do
    # setup %{source: source} do
    #   _backend =
    #     insert(:backend,
    #       type: :tcp,
    #       sources: [source],
    #       config: %{
    #         host: "localhost",
    #         port: 6515,
    #         ca_cert: read_cert("ca.crt"),
    #         client_cert: read_cert("client.crt"),
    #         client_key: read_cert("client.key")
    #       }
    #     )
    # end
  end

  defp spawn_link_telegraf_watcher(path) do
    test = self()
    spawn_link(fn -> watch_telegraf(path, _offset = 0, test) end)
  end

  defp watch_telegraf(path, offset, test) do
    new_lines = path |> File.stream!() |> Enum.drop(offset)
    new_offset = offset + length(new_lines)

    Enum.each(new_lines, fn line ->
      send(test, {:telegraf, Jason.decode!(line)})
    end)

    Process.sleep(100)
    watch_telegraf(path, new_offset, test)
  end
end
