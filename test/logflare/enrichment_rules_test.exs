defmodule Logflare.EnrichmentRulesTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.EnrichmentRules
  alias Logflare.EnrichmentRules.EnrichmentRule
  alias Logflare.LogEvent
  alias Logflare.SystemMetrics.AllLogsLogged

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    [user: user, source: source]
  end

  describe "create/2" do
    test "parses the LQL and stores the fields", %{source: source} do
      assert {:ok, %EnrichmentRule{} = rule} =
               EnrichmentRules.create(source, %{
                 "lql_string" => "m.level:error",
                 "set_fields" => %{"m.alert" => true}
               })

      assert rule.source_id == source.id
      assert [%{path: "metadata.level", value: "error"}] = rule.lql_filters
      assert rule.set_fields == %{"m.alert" => true}
      assert [%EnrichmentRule{id: id}] = EnrichmentRules.list_by_source_id(source.id)
      assert id == rule.id
    end

    test "accepts a filter on a field the source schema does not have yet", %{source: source} do
      assert {:ok, rule} =
               EnrichmentRules.create(source, %{
                 "lql_string" => "debug_mode:errors_only",
                 "set_fields" => %{"excluded" => true}
               })

      assert [%{path: "debug_mode", value: "errors_only"}] = rule.lql_filters
    end

    test "rejects invalid LQL", %{source: source} do
      assert {:error, changeset} =
               EnrichmentRules.create(source, %{
                 "lql_string" => "m.level:(",
                 "set_fields" => %{"alert" => true}
               })

      assert %{lql_string: [_]} = errors_on(changeset)
    end

    test "rejects an empty filter", %{source: source} do
      assert {:error, changeset} =
               EnrichmentRules.create(source, %{"lql_string" => "", "set_fields" => %{"a" => 1}})

      assert %{lql_string: _} = errors_on(changeset)
    end

    test "rejects empty, reserved and non-scalar set fields", %{source: source} do
      for {set_fields, message} <- [
            {%{}, "must set at least one field"},
            {%{"timestamp" => 1}, "cannot set reserved field: timestamp"},
            {%{"m.tags" => ["a"]}, "value for m.tags must be a string, number or boolean"},
            {%{"metadata..x" => 1}, "has an invalid field path"}
          ] do
        assert {:error, changeset} =
                 EnrichmentRules.create(source, %{
                   "lql_string" => "testing",
                   "set_fields" => set_fields
                 })

        assert %{set_fields: [error]} = errors_on(changeset)
        assert error =~ message
      end
    end
  end

  describe "apply_rules/2" do
    test "leaves events unchanged when the source has no rules", %{source: source} do
      events = [build(:log_event, source: source, message: "testing")]

      assert EnrichmentRules.apply_rules(events, source) == events
    end

    test "sets fields on matching events only", %{source: source} do
      insert(:enrichment_rule,
        source: source,
        lql_string: "m.level:error",
        set_fields: %{"alert" => true, "m.team" => "core"}
      )

      matching = build(:log_event, source: source, metadata: %{"level" => "error"})
      other = build(:log_event, source: source, metadata: %{"level" => "info"})

      assert [enriched, unchanged] = EnrichmentRules.apply_rules([matching, other], source)
      assert enriched.body["alert"] == true
      assert enriched.body["metadata"]["team"] == "core"
      assert enriched.body["metadata"]["level"] == "error"
      assert unchanged == other
    end

    test "matches on fields set by key-value enrichment and supports negation and comparisons",
         %{source: source} do
      insert(:enrichment_rule,
        source: source,
        lql_string: ~s{debug_mode:errors_only -m.level:~"^(error|fatal)$"},
        set_fields: %{"excluded" => true}
      )

      insert(:enrichment_rule,
        source: source,
        lql_string: "debug_mode:errors_only m.status:<400",
        set_fields: %{"excluded_status" => true}
      )

      info =
        build(:log_event,
          source: source,
          debug_mode: "errors_only",
          metadata: %{"level" => "info", "status" => 200}
        )

      error =
        build(:log_event,
          source: source,
          debug_mode: "errors_only",
          metadata: %{"level" => "error", "status" => 500}
        )

      other_project =
        build(:log_event, source: source, debug_mode: "max", metadata: %{"level" => "info"})

      assert [info, error, other_project] =
               EnrichmentRules.apply_rules([info, error, other_project], source)

      assert info.body["excluded"] == true
      assert info.body["excluded_status"] == true
      refute Map.has_key?(error.body, "excluded")
      refute Map.has_key?(error.body, "excluded_status")
      refute Map.has_key?(other_project.body, "excluded")
    end

    test "applies matching rules in creation order", %{source: source} do
      insert(:enrichment_rule,
        source: source,
        lql_string: "testing",
        set_fields: %{"tier" => "first"}
      )

      insert(:enrichment_rule,
        source: source,
        lql_string: "testing",
        set_fields: %{"tier" => "second"}
      )

      [event] =
        EnrichmentRules.apply_rules(
          [build(:log_event, source: source, message: "testing")],
          source
        )

      assert event.body["tier"] == "second"
    end

    test "skips a path that crosses a non-map value", %{source: source} do
      insert(:enrichment_rule,
        source: source,
        lql_string: "testing",
        set_fields: %{"m.level.nested" => 1, "ok" => 1}
      )

      [event] =
        EnrichmentRules.apply_rules(
          [build(:log_event, source: source, message: "testing", metadata: %{"level" => "info"})],
          source
        )

      assert event.body["metadata"]["level"] == "info"
      assert event.body["ok"] == 1
    end

    test "leaves events routed in by another rule unchanged", %{source: source} do
      insert(:enrichment_rule, source: source, lql_string: "testing")
      routed = %{build(:log_event, source: source, message: "testing") | via_rule_id: 1}

      assert EnrichmentRules.apply_rules([routed], source) == [routed]
    end

    test "skips the rules lookup when every event was routed in", %{source: source} do
      insert(:enrichment_rule, source: source, lql_string: "testing")
      cache_key = {:compiled_by_source_id, [source.id]}
      routed = %{build(:log_event, source: source, message: "testing") | via_rule_id: 1}

      assert EnrichmentRules.apply_rules([routed, routed], source) == [routed, routed]
      assert Cachex.get!(EnrichmentRules.Cache, cache_key) == nil

      fresh = build(:log_event, source: source, message: "testing")
      assert [^routed, enriched] = EnrichmentRules.apply_rules([routed, fresh], source)
      assert enriched.body["enriched"] == true
      assert {:cached, %{}} = Cachex.get!(EnrichmentRules.Cache, cache_key)
    end

    test "resets cached byte sizes so metering measures the enriched body", %{source: source} do
      insert(:enrichment_rule, source: source, lql_string: "testing")

      event = LogEvent.cache_sizes(build(:log_event, source: source, message: "testing"))
      [enriched] = EnrichmentRules.apply_rules([event], source)

      assert enriched.accounted_bytes == nil
      assert LogEvent.accounted_byte_size(enriched) > LogEvent.accounted_byte_size(event)
    end
  end

  describe "dispatch" do
    setup %{source: source} do
      start_supervised!(AllLogsLogged)
      start_supervised!({SourceSup, source})

      insert(:enrichment_rule,
        source: source,
        lql_string: "m.level:error",
        set_fields: %{"alert" => true}
      )

      :ok
    end

    test "direct ingest enriches events before they are queued", %{source: source} do
      le = build(:log_event, source: source, metadata: %{"level" => "error"})

      assert {:ok, 1} = Backends.ingest_logs([le], source)
      assert [queued] = Backends.list_recent_logs_local(source)
      assert queued.body["alert"] == true
    end

    test "the spool consumer enriches events before they are queued", %{source: source} do
      record = %{
        "id" => Ecto.UUID.generate(),
        "body" => %{
          "message" => "spooled",
          "timestamp" => System.system_time(:microsecond),
          "metadata" => %{"level" => "error"}
        },
        "event_type" => "log",
        "ingested_at" => DateTime.to_iso8601(DateTime.utc_now())
      }

      assert {:ok, 1} = Backends.dispatch_from_spool([record], source)
      assert [queued] = Backends.list_recent_logs_local(source)
      assert queued.body["alert"] == true
    end
  end
end
