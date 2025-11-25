defmodule Logflare.Backends.Adaptor.TCPAdaptorTest do
  use Logflare.DataCase

  alias Logflare.Backends.Adaptor.TCPAdaptor
  @output "db/telegraf_output/metrics.out"
  @moduletag :telegraf

  setup_all do
    if File.exists?(@output), do: File.write!(@output, "")
    :ok
  end

  describe "integration with telegraf" do
    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id)

      backend =
        insert(:backend,
          type: :tcp,
          sources: [source],
          config: %{host: "localhost", port: 6514}
        )

      {:ok, source: source, backend: backend}
    end

    test "sends RFC5424 message with octet counting", %{source: source, backend: backend} do
      log_event =
        build(:log_event, source: source, body: %{"message" => "hello world", "level" => "info"})

      {:ok, pid} = TCPAdaptor.start_link({source, backend})
      TCPAdaptor.ingest(pid, [log_event])

      assert_eventually(
        fn -> refute @output |> File.stream!() |> Enum.empty?() end,
        5000,
        200
      )

      assert [telegraf_event] =
               @output |> File.stream!() |> Stream.map(&Jason.decode!/1) |> Enum.into([])

      assert %{
               "fields" => %{
                 "facility_code" => 16,
                 "message" => telegraf_event_message,
                 "msgid" => "msgid",
                 "procid" => "procid",
                 "severity_code" => 6,
                 "timestamp" => _timestamp,
                 "version" => 1
               },
               "name" => "syslog",
               "tags" => %{
                 "appname" => "app_name",
                 "facility" => "local0",
                 "hostname" => "hostname",
                 "severity" => "info"
               }
             } = telegraf_event

      assert %{
               "body" => %{"level" => "info", "message" => "hello world"},
               "event_message" => "test-msg",
               "id" => _id,
               "timestamp" => _timestamp
             } = Jason.decode!(telegraf_event_message)
    end
  end

  defp assert_eventually(fun, timeout, interval) do
    try do
      fun.()
    rescue
      ex ->
        if timeout > 0 do
          Process.sleep(interval)
          assert_eventually(fun, timeout - interval, interval)
        else
          reraise ex, __STACKTRACE__
        end
    end
  end
end
