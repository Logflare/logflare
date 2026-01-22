defmodule Logflare.Backends.Adaptor.SyslogAdaptorTest do
  use Logflare.DataCase, async: false
  @moduletag :telegraf

  @telegraf_output_path "db/telegraf_output/metrics.out"

  setup_all do
    on_exit(fn ->
      if File.exists?(@telegraf_output_path) do
        File.write!(@telegraf_output_path, _empty = "")
      end
    end)
  end

  test "basic fields check" do
    {source, _backend} = start_syslog(%{host: "localhost", port: 6514})

    assert [
             %{
               "fields" => %{
                 "message" => telegraf_message
               },
               "name" => "syslog",
               "tags" => %{
                 "appname" => "logflare",
                 "facility" => "local0",
                 "severity" => "info"
               }
             }
           ] =
             ingest_syslog(
               [build(:log_event, message: "basic unicode message ✍️")],
               source
             )

    assert %{"event_message" => "basic unicode message ✍️"} =
             Jason.decode!(telegraf_message)
  end

  test "handles opentelemetry metadata" do
    {source, _backend} = start_syslog(%{host: "localhost", port: 6514})

    assert [
             %{
               "tags" => %{
                 "appname" => "Logflare_(from_resource.name)",
                 "hostname" => ":\"logflare-versioned@10.0.0.123\""
               }
             }
           ] =
             ingest_syslog(
               [
                 build(:log_event,
                   message: "hello from opentelemetry",
                   resource: %{
                     "cluster" => "versioned",
                     "name" => "Logflare (from resource.name)",
                     "node" => ":\"logflare-versioned@10.0.0.123\"",
                     "service" => %{"name" => "Logflare", "version" => "1.26.25"}
                   }
                 )
               ],
               source
             )
  end

  test "extracts level from input" do
    {source, _backend} = start_syslog(%{host: "localhost", port: 6514})

    assert [
             %{"tags" => %{"severity" => "debug"}},
             %{"tags" => %{"severity" => "err"}}
           ] =
             ingest_syslog(
               [
                 build(:log_event, level: "debug", message: "eh"),
                 build(:log_event, metadata: %{"level" => "error"}, message: "eh")
               ],
               source
             )
  end

  test "replaces invalid or empty log level with `info` severity code" do
    {source, _backend} = start_syslog(%{host: "localhost", port: 6514})

    assert [
             %{"tags" => %{"severity" => "info"}},
             %{"tags" => %{"severity" => "info"}}
           ] =
             ingest_syslog(
               [
                 build(:log_event, message: "no level"),
                 build(:log_event, message: "bad level", level: "bad")
               ],
               source
             )
  end

  test "sends message over mTLS" do
    {source, _backend} =
      start_syslog(%{
        host: "localhost",
        port: 6515,
        tls: true,
        ca_cert: File.read!("priv/telegraf/ca.crt"),
        client_cert: File.read!("priv/telegraf/client.crt"),
        client_key: File.read!("priv/telegraf/client.key")
      })

    assert [%{"fields" => %{"message" => telegraf_event_message}}] =
             ingest_syslog(
               [build(:log_event, message: "hello world over tls")],
               source
             )

    assert %{"event_message" => "hello world over tls"} = Jason.decode!(telegraf_event_message)
  end

  test "can send encrypted message" do
    key = :crypto.strong_rand_bytes(32)

    {source, _backend} =
      start_syslog(%{host: "localhost", port: 6514, cipher_key: Base.encode64(key)})

    assert [%{"fields" => %{"message" => encrypted_message}}] =
             ingest_syslog([build(:log_event, message: "hello cipher")], source)

    assert <<iv::12-bytes, tag::16-bytes, ciphertext::bytes>> = Base.decode64!(encrypted_message)

    plaintext =
      :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, ciphertext, "syslog", tag, false)

    assert %{"event_message" => "hello cipher"} = Jason.decode!(plaintext)
  end

  test "formats message with structured data" do
    {source, _backend} =
      start_syslog(%{
        host: "localhost",
        port: 6514,
        structured_data: "[logtail@11993 source_token=\"123\"]"
      })

    assert [%{"fields" => %{"logtail@11993_source_token" => "123"}}] =
             ingest_syslog(
               [build(:log_event, message: "hello world over tls")],
               source
             )
  end

  test "handles backend config change" do
    initial_backend_config = %{host: "localhost", port: 6514}
    {source, backend} = start_syslog(initial_backend_config)

    assert [_, _] =
             ingest_syslog(
               [build(:log_event, message: "one"), build(:log_event, message: "two")],
               source
             )

    key = :crypto.strong_rand_bytes(32)

    assert {:ok, _updated_backend} =
             Logflare.Backends.update_backend(backend, %{
               "config" => %{
                 "host" => "localhost",
                 "port" => 6515,
                 "cipher_key" => Base.encode64(key),
                 "tls" => true,
                 "ca_cert" => File.read!("priv/telegraf/ca.crt"),
                 "client_cert" => File.read!("priv/telegraf/client.crt"),
                 "client_key" => File.read!("priv/telegraf/client.key")
               }
             })

    # simulate cache bust
    Logflare.ContextCache.bust_keys([{Logflare.Backends, backend.id}])

    assert [%{"fields" => %{"message" => encrypted_message}}] =
             ingest_syslog([build(:log_event, message: "three")], source)

    assert <<iv::12-bytes, tag::16-bytes, ciphertext::bytes>> = Base.decode64!(encrypted_message)

    plaintext =
      :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, ciphertext, "syslog", tag, false)

    assert %{"event_message" => "three"} = Jason.decode!(plaintext)
  end

  defp ingest_syslog(log_events, source, timeout \\ to_timeout(second: 5)) do
    deadline = System.monotonic_time(:millisecond) + timeout
    {:ok, _count} = Logflare.Backends.ingest_logs(log_events, source)
    collect_telegraf_logs(log_events, deadline)
  end

  defp start_syslog(backend_config) do
    start_supervised!(Logflare.SystemMetrics.AllLogsLogged)
    insert(:plan)

    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend, type: :syslog, sources: [source], config: backend_config, user: user)

    start_supervised!({Logflare.Backends.AdaptorSupervisor, {source, backend}})

    {source, backend}
  end

  defp collect_telegraf_logs(log_events, deadline) do
    # extract msgids to match them with telegraf output
    syslog_msgids =
      Enum.map(log_events, fn log_event ->
        # uuid -> base32 is what we do in syslog formatter to fit in MSGID size limits
        log_event.id |> Ecto.UUID.dump!() |> Base.encode32(padding: false)
      end)

    telegraf_logs =
      @telegraf_output_path
      |> File.stream!()
      |> Stream.map(fn line ->
        case Jason.decode(line) do
          {:ok, json} ->
            json

          {:error, reason} ->
            raise """
            Failed to parse telegraf line (from #{@telegraf_output_path}) as JSON.
            Line: #{inspect(line)}
            Error: #{Exception.format(:error, reason)}
            """
        end
      end)
      |> Enum.filter(fn %{"fields" => %{"msgid" => msgid}} ->
        msgid in syslog_msgids
      end)

    cond do
      length(log_events) == length(telegraf_logs) ->
        telegraf_logs_lookup =
          Map.new(telegraf_logs, fn %{"fields" => %{"msgid" => syslog_msg_id}} = log ->
            log_event_id = syslog_msg_id |> Base.decode32!(padding: false) |> Ecto.UUID.load!()
            {log_event_id, log}
          end)

        # now we match "output" telegraf logs with "input" log events
        Enum.map(log_events, fn log_event ->
          Map.fetch!(telegraf_logs_lookup, log_event.id)
        end)

      System.monotonic_time(:millisecond) < deadline ->
        Process.sleep(100)
        collect_telegraf_logs(log_events, deadline)

      true ->
        raise """
        Failed to collect all #{length(log_events)} telegraf logs (from #{@telegraf_output_path}) before deadline.
        Collected #{length(telegraf_logs)} logs: #{inspect(telegraf_logs)}
        """
    end
  end

  describe "config validation" do
    test "rejects invalid structured data" do
      bad_examples = [
        "invalid",
        # Example 3 from https://datatracker.ietf.org/doc/html/rfc5424#section-6.3.5
        "[exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] [examplePriority@32473 class=\"high\"]",
        # Example 4 from https://datatracker.ietf.org/doc/html/rfc5424#section-6.3.5
        "[ exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"][examplePriority@32473 class=\"high\"]"
      ]

      for structured_data <- bad_examples do
        changeset =
          syslog_changeset(%{
            host: "localhost",
            port: 6514,
            structured_data: structured_data
          })

        refute changeset.valid?
        assert %{structured_data: ["invalid format"]} = errors_on(changeset)
      end
    end

    test "allows valid structured data" do
      rfc_examples = [
        # Example 1 https://datatracker.ietf.org/doc/html/rfc5424#section-6.3.5
        "[exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]",
        # Example 2 https://datatracker.ietf.org/doc/html/rfc5424#section-6.3.5
        "[exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"][examplePriority@32473 class=\"high\"]"
      ]

      for structured_data <- rfc_examples do
        changeset =
          syslog_changeset(%{
            host: "localhost",
            port: 6514,
            structured_data: structured_data
          })

        assert changeset.valid?
      end
    end

    test "rejects invalid PEM configuration" do
      changeset =
        syslog_changeset(%{
          tls: true,
          ca_cert: "invalid-pem",
          client_cert: "invalid-pem",
          client_key: "invalid-pem"
        })

      refute changeset.valid?

      assert %{
               ca_cert: ["must be a valid PEM encoded string"],
               client_cert: ["must be a valid PEM encoded string"],
               client_key: ["must be a valid PEM encoded string"]
             } = errors_on(changeset)
    end
  end

  defp syslog_changeset(backend_config) do
    backend_config
    |> Logflare.Backends.Adaptor.SyslogAdaptor.cast_config()
    |> Logflare.Backends.Adaptor.SyslogAdaptor.validate_config()
  end
end
